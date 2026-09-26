"""Tests for hub.api (Phase 5 — REST API wrapping CLI)."""

import time
from pathlib import Path

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from hub.api import make_app
from hub.db import DB
from hub.log_store import LogStore
from hub import network as net_mod


pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def client(tmp_path: Path):
    db_path = tmp_path / "h.db"
    db = DB(db_path)

    # Seed: one device, one gateway, an active OT network, a few logs.
    db.register_device("11112222aaaabbbb", "sensor-A", "end_device", "")
    db.set_provision_info(
        device_id="11112222aaaabbbb",
        device_type="sample_c6",
        enc_pubkey_b64="",     # no real key — field endpoints will fail at 400
        mdns_addr="sensor-A.local",
        ml_eid="",
        gateway_id="ggggffffeeeedddd",
        capabilities='[{"n":"temperature","t":0}]',
    )
    db.register_gateway("ggggffffeeeedddd", "gw-test", "", "")
    db.update_gateway_thread_state("ggggffffeeeedddd", 3, 0x0c00,
                                    "fdfc2c266d000001abcd1234deadbeef")
    net_mod.get_or_create_network(db)

    log_store = LogStore(tmp_path / "logs", rotate_interval_sec=3600)
    log_store.append_text(
        device_id="11112222aaaabbbb",
        text="[00:00:01.000,000] <inf> ncp_host: hello",
    )
    log_store.append_text(
        device_id="11112222aaaabbbb",
        text="[00:00:02.000,000] <wrn> wifi: lost ap",
    )

    # Stub enc_priv (not used in tests below).
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    enc_priv = X25519PrivateKey.generate()

    # Stub proto_router — present so field endpoints don't 503; the
    # tests below all hit upstream-of-relay validation paths so the
    # router is never actually invoked.
    class _StubRouter:
        async def request_field(self, *a, **kw):
            raise RuntimeError("stub router should not be reached in this test")

    app = make_app(db, log_store, enc_priv, auth_token=None,
                   proto_router=_StubRouter())
    server = TestServer(app)
    async with TestClient(server) as cli:
        yield cli


async def test_healthz(client):
    r = await client.get("/api/v1/healthz")
    assert r.status == 200
    body = await r.json()
    assert body["ok"] is True


async def test_list_devices(client):
    r = await client.get("/api/v1/devices")
    assert r.status == 200
    body = await r.json()
    assert isinstance(body, list)
    assert len(body) == 1
    d = body[0]
    assert d["name"] == "sensor-A"
    assert d["mdns_addr"] == "sensor-A.local"
    assert d["capabilities"] == [{"n": "temperature", "t": 0}]


async def test_get_device_by_name_or_id(client):
    r = await client.get("/api/v1/devices/sensor-A")
    assert r.status == 200
    assert (await r.json())["id"] == "11112222aaaabbbb"

    r = await client.get("/api/v1/devices/11112222aaaabbbb")
    assert r.status == 200

    r = await client.get("/api/v1/devices/nonexistent")
    assert r.status == 404


async def test_list_gateways_and_thread_state(client):
    r = await client.get("/api/v1/gateways")
    body = await r.json()
    assert len(body) == 1
    assert body[0]["role_name"] == "leader"
    assert body[0]["rloc16"] == 0x0c00


async def test_get_gateway_includes_serviced_devices(client):
    r = await client.get("/api/v1/gateways/ggggffffeeeedddd")
    assert r.status == 200
    body = await r.json()
    serviced = [d["name"] for d in body["devices_serviced"]]
    assert serviced == ["sensor-A"]


async def test_get_network(client):
    r = await client.get("/api/v1/network")
    body = await r.json()
    assert body["configured"] is True
    assert body["channel"] in (15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26)
    assert body["dataset_tlvs_bytes"] > 0


async def test_logs_filters(client):
    r = await client.get("/api/v1/logs?limit=10")
    rows = await r.json()
    assert len(rows) == 2

    r = await client.get("/api/v1/logs?level=wrn")
    rows = await r.json()
    assert len(rows) == 1 and rows[0]["tag"] == "wifi"

    r = await client.get("/api/v1/logs?device=sensor-A")
    rows = await r.json()
    assert len(rows) == 2

    r = await client.get("/api/v1/logs?tag=ncp_host")
    rows = await r.json()
    assert len(rows) == 1 and rows[0]["content"] == "hello"


async def test_log_files(client):
    r = await client.get("/api/v1/log_files")
    body = await r.json()
    assert len(body) == 1
    assert body[0]["name"].startswith("logs_")


async def test_field_get_400_when_no_pubkey(client):
    r = await client.get("/api/v1/devices/sensor-A/field/temperature")
    assert r.status == 400  # no enc_pubkey_b64 → can't drive ECIES


async def test_field_put_400_on_missing_value(client):
    r = await client.put("/api/v1/devices/sensor-A/field/led", json={})
    assert r.status == 400


async def test_field_put_400_on_nonnumeric_value(client):
    r = await client.put("/api/v1/devices/sensor-A/field/led",
                         json={"value": "abc"})
    assert r.status == 400


async def test_bearer_auth_enforced(tmp_path: Path):
    """When auth_token is set, requests without bearer must 401."""
    db = DB(tmp_path / "h.db")
    log_store = LogStore(tmp_path / "logs", rotate_interval_sec=3600)
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    enc_priv = X25519PrivateKey.generate()
    app = make_app(db, log_store, enc_priv, auth_token="s3cret")
    server = TestServer(app)
    async with TestClient(server) as cli:
        r = await cli.get("/api/v1/healthz")
        assert r.status == 401

        r = await cli.get("/api/v1/healthz",
                          headers={"Authorization": "Bearer s3cret"})
        assert r.status == 200

        r = await cli.get("/api/v1/healthz",
                          headers={"Authorization": "Bearer wrong"})
        assert r.status == 401


# ── camera bundle whitelist ──────────────────────────────────────────────────
# put_camera_bundle WHITELISTS fields; rst/wdt_rc (watchdog telemetry)
# were once silently dropped here — device sent, service forwarded, hub
# discarded.  Pin both directions: known extras pass, unknown ones don't.

async def test_camera_bundle_whitelist_roundtrip(client):
    r = await client.put("/api/v1/cameras/camX/bundle", json={
        "version": "abc1234", "device_type": "nn-app-camera-x",
        "platform": "esp32p4", "rst": "WDT_RESET", "wdt_rc": 3,
        "not_whitelisted": "must vanish",
    })
    assert r.status == 200
    r = await client.get("/api/v1/cameras/camX/bundle")
    b = await r.json()
    assert b["version"] == "abc1234"
    assert b["device_type"] == "nn-app-camera-x"
    assert b["platform"] == "esp32p4"
    assert b["rst"] == "WDT_RESET"
    assert b["wdt_rc"] == 3
    assert b["reported_at"] > 0
    assert "not_whitelisted" not in b


async def test_camera_bundle_reports_system_layer(client):
    """nn-sysupd (A/B system slot agent) reports the running system version,
    the layout it runs and the active slot alongside the bundle version."""
    r = await client.put("/api/v1/cameras/camY/bundle", json={
        "version": "0.1.9", "device_type": "byai_camera",
        "platform": "1.0.0", "system": "20260915-1", "layout": "ab-v1", "slot": "b",
        "sysupd": "confirmed",
    })
    assert r.status == 200
    b = await (await client.get("/api/v1/cameras/camY/bundle")).json()
    assert (b["system"], b["layout"], b["slot"]) == ("20260915-1", "ab-v1", "b")
    assert b["sysupd"] == "confirmed"           # the agent's state word (idle/armed/confirmed/rolled-back)
    assert b["platform"] == "1.0.0"


async def test_camera_bundle_by_addr_resolves_slot(client):
    """nn-sysupd knows the board's BLE address, not its slot; the hub maps
    it through the binding recorded at provisioning (camera_addr#<slot>)."""
    db = client.server.app["db"]
    db.upsert_camera("cam3", "BeagleY", "http://127.0.0.1:8903", "{}")
    db.set_setting("camera_addr#cam3", "10:CA:BF:DA:35:B7")
    r = await client.put("/api/v1/cameras/by-addr/10:ca:bf:da:35:b7/bundle", json={
        "version": "-", "device_type": "byai_system", "system": "20260915-1",
        "layout": "ab-v1", "slot": "a"})
    assert r.status == 200 and (await r.json())["slot"] == "cam3"
    b = await (await client.get("/api/v1/cameras/cam3/bundle")).json()
    assert b["system"] == "20260915-1" and b["slot"] == "a"
    r = await client.put("/api/v1/cameras/by-addr/00:11:22:33:44:55/bundle",
                         json={"version": "-", "system": "x"})
    assert r.status == 404
    assert "provision it first" in (await r.json())["err"]


async def test_camera_bundle_rejects_bad_body(client):
    r = await client.put("/api/v1/cameras/camX/bundle", json={"nope": 1})
    assert r.status == 400
