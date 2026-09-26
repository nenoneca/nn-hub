"""Phase 4 of nn-video pipelines: the hub as the media host's only writer.

A fake media host (aiohttp TestServer) stands in for nn-video-pipelines;
the hub app is the real one.  Covers: media host discovery, the pipeline
routes the webapp uses, the registration switch on unregister/adopt, the
"new camera" slot, and the per-camera log source filters.
"""
import asyncio
import base64
import json
import sys
from pathlib import Path

import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hub.db import DB  # noqa: E402
from hub import media_host as mh  # noqa: E402
from hub import api as hub_api  # noqa: E402

pytestmark = pytest.mark.asyncio


class FakeMediaHost:
    def __init__(self):
        self.cameras = {"cam0": {"id": "cam0", "name": "Camera 1"}, "cam3": {"id": "cam3", "name": "cam3"}}
        self.settings = {("cam3", "stream"): {"host_key_id": "cam3"}, ("cam0", "hub"): {"register": True}}
        self.keys = {}
        self.resets = []
        self.version = 1
        app = web.Application()
        app.router.add_get("/health", self.health)
        app.router.add_get("/provinfo", self.provinfo)
        app.router.add_get("/cameras/{id}", self.camera)
        app.router.add_post("/cameras", self.add_camera)
        app.router.add_put("/cameras/{id}/key", self.set_key)
        app.router.add_get("/cameras/{id}/settings/{p}", self.get_settings)
        app.router.add_put("/cameras/{id}/settings/{p}", self.put_settings)
        app.router.add_post("/cameras/{id}/reset/{p}", self.reset)
        self.server = TestServer(app)

    async def health(self, r):
        return web.json_response({"uptime_s": 3600, "rss_kb": 250000, "connections": {"cam3": {"peer": "x", "age_s": 5}},
                                 "pipelines": {"stream": {"mode": "async", "workers": 1, "budget_s": 30,
                                                          "queue": {"depth": 0, "dropped": 0, "avg_wait_ms": 0.5, "max_depth": 3},
                                                          "cameras": {"cam3": {"runs": 10, "failures_recent": 0, "stalls": 0, "resets": 1,
                                                                               "last_latency_ms": 0.4, "last_error": None}}},
                                               "detect": {"mode": "thread", "workers": 1, "budget_s": 2,
                                                          "queue": {"depth": 0, "dropped": 0, "avg_wait_ms": 0.8, "max_depth": 1},
                                                          "cameras": {}}}})

    async def provinfo(self, r):
        return web.json_response({"ingest_port": 8886, "ingest_aliases": [8888], "api_url": str(self.server.make_url("")),
                                  "host_key_id": "default", "host_pub_hex": "ab" * 32, "cameras": sorted(self.cameras)})

    async def camera(self, r):
        cid = r.match_info["id"]
        if cid not in self.cameras:
            return web.json_response({"err": "no such camera"}, status=404)
        return web.json_response({**self.cameras[cid], "connection": {"peer": "x", "age_s": 5} if cid == "cam3" else None,
                                  "settings": {p: {**self.settings.get((cid, p), {})} for p in ("stream", "detect", "hub")}})

    async def add_camera(self, r):
        b = await r.json()
        if b["id"] in self.cameras:
            return web.json_response({"err": "exists"}, status=409)
        self.cameras[b["id"]] = {"id": b["id"], "name": b.get("name")}
        return web.json_response(self.cameras[b["id"]], status=201)

    async def set_key(self, r):
        b = await r.json(); self.keys[r.match_info["id"]] = (b["device_pub_b64"], b.get("host_key_id"))
        return web.json_response({"ok": True})

    async def get_settings(self, r):
        k = (r.match_info["id"], r.match_info["p"])
        return web.json_response({"camera": k[0], "pipeline": k[1], "effective": self.settings.get(k, {}), "version": self.version})

    async def put_settings(self, r):
        k = (r.match_info["id"], r.match_info["p"]); self.settings.setdefault(k, {}).update(await r.json()); self.version += 1
        return web.json_response({"camera": k[0], "pipeline": k[1], "version": self.version, "effective": self.settings[k]})

    async def reset(self, r):
        self.resets.append((r.match_info["id"], r.match_info["p"]))
        return web.json_response({"ok": True, "camera": r.match_info["id"], "pipeline": r.match_info["p"], "resets": len(self.resets)})


@pytest_asyncio.fixture
async def stack(tmp_path, monkeypatch):
    fake = FakeMediaHost()
    await fake.server.start_server()
    monkeypatch.setenv("NN_MEDIA_HOST_URL", str(fake.server.make_url("")).rstrip("/"))
    db = DB(tmp_path / "h.db")
    from hub.log_store import LogStore
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    app = hub_api.make_app(db, LogStore(tmp_path / "logs", rotate_interval_sec=3600), X25519PrivateKey.generate(),
                           auth_token=None, proto_router=None)
    async with TestClient(TestServer(app)) as cli:
        yield cli, db, fake
    await fake.server.close()


def test_media_host_url_discovery(tmp_path, monkeypatch):
    monkeypatch.delenv("NN_MEDIA_HOST_URL", raising=False)
    db = DB(tmp_path / "d.db")
    assert mh.media_host_url(db) == "http://127.0.0.1:8880"
    db.upsert_camera("cam3", "cam3", "http://192.0.2.10:8880/cam/cam3", "{}")
    assert mh.media_host_url(db) == "http://192.0.2.10:8880", "derived from a registered camera URL"
    monkeypatch.setenv("NN_MEDIA_HOST_URL", "http://mh:1/")
    assert mh.media_host_url(db) == "http://mh:1"
    assert mh.next_camera_id({"cam0", "cam1", "cam3"}) == "cam2" and mh.next_camera_id(set()) == "cam0"


async def test_pipeline_routes_proxy_to_the_media_host(stack):
    cli, db, fake = stack
    d = await (await cli.get("/api/v1/cameras/cam3/pipelines")).json()
    assert d["camera"] == "cam3" and d["pipelines"]["stream"]["state"]["resets"] == 1
    assert d["pipelines"]["stream"]["settings"] == {"host_key_id": "cam3"} and d["pipelines"]["detect"]["state"] is None
    assert d["connection"]["peer"] == "x"
    r = await cli.put("/api/v1/cameras/cam3/pipeline-settings/detect", json={"interval_ms": 100})
    assert r.status == 200 and (await r.json())["effective"] == {"interval_ms": 100}
    assert fake.settings[("cam3", "detect")] == {"interval_ms": 100}
    assert (await cli.put("/api/v1/cameras/cam3/pipeline-settings/nope", json={"a": 1})).status == 404
    assert (await cli.put("/api/v1/cameras/cam3/pipeline-settings/detect", json=[])).status == 400
    r = await cli.post("/api/v1/cameras/cam3/pipeline-reset/stream")
    assert r.status == 200 and fake.resets == [("cam3", "stream")]
    g = await (await cli.get("/api/v1/cameras/cam3/pipeline-settings/detect")).json()
    assert g["effective"] == {"interval_ms": 100}
    m = await (await cli.get("/api/v1/media-host")).json()
    assert m["ok"] and m["connections"] == ["cam3"] and m["provinfo"]["ingest_port"] == 8886


async def test_unregister_and_adopt_switch_the_media_host_registration(stack):
    cli, db, fake = stack
    db.upsert_camera("cam3", "cam3", "http://127.0.0.1:8880/cam/cam3", "{}")
    r = await cli.post("/api/v1/cameras/cam3/free")
    assert r.status == 200 and fake.settings[("cam3", "hub")] == {"register": False}
    r = await cli.post("/api/v1/cameras/cam3/adopt")
    assert r.status == 200 and fake.settings[("cam3", "hub")] == {"register": True}


async def test_camera_slots_always_offer_a_new_camera(stack):
    cli, db, fake = stack
    db.upsert_camera("cam0", "Camera 1", "http://127.0.0.1:8880/cam/cam0", "{}")
    db.upsert_camera("cam3", "cam3", "http://127.0.0.1:8880/cam/cam3", "{}")
    slots = await (await cli.get("/api/v1/camera-slots")).json()
    new = [s for s in slots if s["state"] == "new"]
    assert len(new) == 1 and new[0]["id"] == "cam1" and new[0]["free"] is True
    assert hub_api._new_camera_target(db) == "cam1"
    db.set_setting("cameras_blocked", json.dumps(["cam3"]))
    assert hub_api._new_camera_target(db) == "", "a re-usable unregistered slot comes first"


async def test_install_device_key_and_ensure_camera(stack):
    cli, db, fake = stack
    row = await mh.ensure_camera(db, "cam7", "Garden")
    assert row["id"] == "cam7" and "cam7" in fake.cameras
    row = await mh.ensure_camera(db, "cam7", "Garden")          # exists: fine
    assert row["id"] == "cam7"
    await mh.install_device_key(db, "cam7", b"\x05" * 32, "default")
    assert fake.keys["cam7"] == (base64.b64encode(b"\x05" * 32).decode(), "default")
    assert await mh.camera_host_key_id(db, "cam3") == "cam3" and await mh.camera_host_key_id(db, "cam7") == "default"
    pi = await mh.provinfo(db)
    assert pi["ingest_port"] == 8886 and pi["host_pub_hex"] == "ab" * 32


def test_log_line_filter_matches_camera_and_pipeline_tags():
    f = hub_api._log_line_filter("cam3")
    assert f("2026-09-22T10:00:00+0800 h python3[1]: [cam3/stream] graph up")
    assert f("... [cam3/svc] >> HLS: ...") and f("... [cam3] worker line") and f("... [cam3/handler] session up")
    assert not f("... [cam30/stream] x") and not f("... [cam0/stream] x")
    g = hub_api._log_line_filter("cam3", "detect")
    assert g("... [cam3/detect] motion detector up") and not g("... [cam3/stream] graph up")


async def test_log_sources_include_a_view_per_camera(stack, monkeypatch):
    cli, db, fake = stack
    db.upsert_camera("cam3", "BeagleY", "http://127.0.0.1:8880/cam/cam3", "{}")
    hub_api._LOG_SOURCES, hub_api._LOG_SOURCES_AT = {}, 0.0
    import subprocess

    class R:
        stdout = "LoadState=loaded\nEnvironment=X=1\n"; returncode = 0; stderr = ""
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: R())
    d = await (await cli.get("/api/v1/logs/sources")).json()
    ids = {s["id"]: s for s in d["sources"]}
    assert "nn-video" in ids and ids["nn-video:cam3"]["camera"] == "cam3" and "BeagleY" in ids["nn-video:cam3"]["label"]
    assert "detect" in d["pipelines"]
    assert (await cli.get("/api/v1/logs/tail?source=nn-video:cam3&pipeline=nope")).status == 400
    hub_api._LOG_SOURCES, hub_api._LOG_SOURCES_AT = {}, 0.0
