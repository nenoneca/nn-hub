"""Fleet cache + live updates: _note_running, GET /fleet, /fleet/ws,
and the device-log-cap setting.

These pin the behavior added 2026-08-29: the hub persists every
device's self-reported firmware version (settings KV), answers the
Fleet view from that cache instantly, and streams changes to WS
subscribers — including the INFO_REPLY fix (version must persist even
when the device has no in-RAM OTA state row).
"""

import asyncio
import json
from pathlib import Path

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

import hub.api as api_mod
from hub.api import make_app
from hub.db import DB
from hub.log_store import LogStore
from hub.proto_router import HubProtoRouter

pytestmark = pytest.mark.asyncio


class _StubServer:
    def __init__(self):
        self.online_gw = set()
        self.sent = []

    def gateway_online(self, gw_id):          # pragma: no cover - trivial
        return gw_id in self.online_gw

    async def send_to_gateway(self, gw_id, frame):
        self.sent.append((gw_id, frame))
        return True


def _router(tmp_path, db):
    return HubProtoRouter(db, _StubServer(),
                          hub_privkey_path=tmp_path / "priv.bin")


# ── _note_running: persist + notify-on-change ────────────────────────────────

async def test_note_running_persists_to_kv(tmp_path: Path):
    db = DB(tmp_path / "h.db")
    r = _router(tmp_path, db)
    r._note_running("aabbccdd00112233", "1.2.3+0")
    assert db.get_setting("device_running:aabbccdd00112233") == "1.2.3+0"
    assert int(db.get_setting("device_running_at:aabbccdd00112233")) > 0


async def test_note_running_notifies_only_on_change(tmp_path: Path):
    db = DB(tmp_path / "h.db")
    r = _router(tmp_path, db)
    q = r.subscribe_fleet()

    r._note_running("aabbccdd00112233", "1.0.0")
    assert q.get_nowait() == {"device_id": "aabbccdd00112233",
                              "running": "1.0.0"}
    # same version again: freshness stamp updates, but no event
    r._note_running("aabbccdd00112233", "1.0.0")
    assert q.empty()
    # new version: event
    r._note_running("aabbccdd00112233", "1.0.1")
    assert q.get_nowait()["running"] == "1.0.1"
    # empty version: ignored entirely
    r._note_running("aabbccdd00112233", "")
    assert q.empty()
    assert db.get_setting("device_running:aabbccdd00112233") == "1.0.1"

    r.unsubscribe_fleet(q)
    r._note_running("aabbccdd00112233", "2.0.0")
    assert q.empty()


async def test_note_running_needs_no_ota_state_row(tmp_path: Path):
    """The historical bug: INFO_REPLY's firmware field was dropped when
    the device had no _ota_state entry (fresh hub boot).  _note_running
    must work standalone."""
    db = DB(tmp_path / "h.db")
    r = _router(tmp_path, db)
    assert "eeff0011eeff0011" not in r._ota_state
    r._note_running("eeff0011eeff0011", "0.0.32+0")
    assert db.get_setting("device_running:eeff0011eeff0011") == "0.0.32+0"


# ── REST: GET /fleet + device-log-cap ────────────────────────────────────────

class _StubFleetRouter:
    """Just enough router for the fleet endpoints."""
    def __init__(self):
        self.queues = []

    def subscribe_fleet(self):
        q = asyncio.Queue()
        self.queues.append(q)
        return q

    def unsubscribe_fleet(self, q):
        self.queues.remove(q)

    async def request_h2d(self, *a, **kw):
        raise RuntimeError("no live devices in this test")


@pytest_asyncio.fixture
async def client(tmp_path: Path):
    db = DB(tmp_path / "h.db")
    db.register_device("11112222aaaabbbb", "sensor-A", "sample_c6", "")
    # cached fleet state, as the router would have persisted it
    db.set_setting("device_image:11112222aaaabbbb", "nn-app-mdns-ot-esp32c6")
    db.set_setting("device_running:11112222aaaabbbb", "0.0.32+0")
    db.set_setting("device_running_at:11112222aaaabbbb", "1700000000")
    # a camera bundle report (cameras list itself is env/registration
    # driven and empty here — bundle rows are keyed independently)
    db.set_setting("camera_bundle:cam9", json.dumps(
        {"version": "0.1.9", "device_type": "byai_camera",
         "reported_at": 1700000000}))
    db.set_firmware_target("nn-app-mdns-ot-esp32c6", "0.0.32", "/x", 1, "s")

    log_store = LogStore(tmp_path / "logs", rotate_interval_sec=3600)
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    app = make_app(db, log_store, X25519PrivateKey.generate(),
                   proto_router=_StubFleetRouter())
    c = TestClient(TestServer(app))
    await c.start_server()
    api_mod._WS_TICKETS.clear()
    yield c
    await c.close()


async def test_get_fleet_answers_from_cache(client):
    r = await client.get("/api/v1/fleet")
    assert r.status == 200
    j = await r.json()
    assert j["targets"]["nn-app-mdns-ot-esp32c6"] == "0.0.32"
    rows = {x["name"]: x for x in j["rows"]}
    s = rows["sensor-A"]
    assert s["kind"] == "sensor"
    assert s["key"] == "nn-app-mdns-ot-esp32c6"
    assert s["running"] == "0.0.32+0"
    assert s["seen_at"] == 1700000000


async def test_fleet_ws_pushes_changes(client):
    r = await client.get("/api/v1/ws-ticket")
    ticket = (await r.json())["ticket"]
    router = client.app["proto_router"]
    ws = await client.ws_connect("/api/v1/fleet/ws?ticket=" + ticket)
    # subscription is registered synchronously in the handler prologue
    for _ in range(50):
        if router.queues:
            break
        await asyncio.sleep(0.01)
    assert router.queues, "handler never subscribed"
    router.queues[0].put_nowait(
        {"device_id": "11112222aaaabbbb", "running": "0.0.33+0"})
    msg = await asyncio.wait_for(ws.receive_json(), timeout=5)
    assert msg["name"] == "sensor-A"          # id resolved to display name
    assert msg["running"] == "0.0.33+0"
    assert msg["at"] > 0
    await ws.close()
    for _ in range(50):
        if not router.queues:
            break
        await asyncio.sleep(0.01)
    assert not router.queues, "unsubscribe missing on close"


async def test_device_log_cap_get_put(client):
    r = await client.get("/api/v1/settings/device-log-cap")
    j = await r.json()
    assert j["cap_mb"] == 256                 # default
    assert "used_mb" in j and "files" in j

    r = await client.put("/api/v1/settings/device-log-cap",
                         json={"cap_mb": 64})
    assert r.status == 200
    assert (await r.json())["cap_mb"] == 64
    assert client.app["log_store"].cap_mb == 64   # applied, not just stored
    assert client.app["db"].get_setting("device_log_cap_mb") == "64"

    for bad in ({"cap_mb": 1}, {"cap_mb": "x"}, {}):
        r = await client.put("/api/v1/settings/device-log-cap", json=bad)
        assert r.status == 400
