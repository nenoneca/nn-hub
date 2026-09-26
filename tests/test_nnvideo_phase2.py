"""Phase 2 of nn-video Pipelines: detect, event, control and the hub's policy
fetch as their own pipelines.

No GStreamer graph here: a stub stands in for the camera's graph object (the
state container the compatibility routes read), so the tests cover what
phase 2 actually moved — ownership, queues, settings read at run start and
applied live, the session back-channel from the registry — while the graph
itself stays covered by the hardware gate.
"""
import asyncio
import json
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "media-host"))
from nnvideo import Engine, Kind, Pipeline, Registry, Request, Store  # noqa: E402
from nnvideo.pipes import (ControlPipeline, DetectPipeline, EventPipeline, HubPipeline,  # noqa: E402
                           IngestPipeline, NN_REC_VIDEO, StreamPipeline)

pytestmark = pytest.mark.asyncio


class Sink(Pipeline):
    def __init__(self, name):
        super().__init__(); self.name = name; self.got = []

    async def run(self, req, cfg):
        self.got.append(req); return None


class FakeSvc:
    """What the pipelines attach to: the legacy graph object's surface."""
    def __init__(self):
        self.event_engine = None; self.service_caps = None; self._det = None
        self.sess = None; self.ctrl = None; self._adapt_ctrl = None
        self.policy_doc = None; self.policy_version = 0; self.device_policy_version = 0
        self.edge_caps = None; self.applied = []; self.pushed = 0

    def apply_infer_mode(self): self.applied.append("infer_mode")
    def apply_policy(self): self.applied.append(("policy", self.policy_version))
    def push_device_config(self): self.pushed += 1


class FakeStream(StreamPipeline):
    """A stream pipeline whose 'graph' is a FakeSvc (no GStreamer)."""
    def __init__(self, registry):
        super().__init__(registry); self.fakes = {}

    def svc(self, cam):
        return self.fakes.get(cam)

    async def ensure(self, cam, cfg):
        self.fakes.setdefault(cam, FakeSvc())
        return SimpleNamespace(svc=self.fakes[cam])

    async def run(self, req, cfg):
        await self.ensure(req.camera_id, cfg); return None


async def _engine(tmp_path, *pipes):
    store = Store(tmp_path / "s.db")
    eng = Engine(store, exit_fn=lambda c: None)
    for p in pipes:
        eng.add(p)
    await eng.start()
    return store, eng


async def _drain(n=25):
    for _ in range(n):
        await asyncio.sleep(0.02)


def _frame(w=64, h=48, shift=0):
    """I420 frame with a bright block; `shift` moves it (motion)."""
    y = np.zeros((h, w), np.uint8); y[10:30, 10 + shift:30 + shift] = 200
    return np.concatenate([y.reshape(-1), np.full(w * h // 2, 128, np.uint8)]).reshape(h * 3 // 2, w)


# ── detect ───────────────────────────────────────────────────────────────────

async def test_detect_paces_frames_and_emits_motion_with_boxes(tmp_path):
    reg = Registry(); stream = FakeStream(reg); detect = DetectPipeline(stream); event = Sink("event")
    store, eng = await _engine(tmp_path, stream, detect, event)
    store.set_settings("cam", "detect", {"interval_ms": 100})
    await stream.ensure("cam", {})
    assert detect.wants_frame("cam") is True
    assert detect.wants_frame("cam") is False, "second frame inside the interval is not wanted"
    await asyncio.sleep(0.12)
    assert detect.wants_frame("cam") is True
    from nnvideo.frame import Frame
    frames = [Frame.from_i420_array(_frame(shift=shift), 64, 48, ts_ms=i) for i, shift in enumerate((0, 0, 20))]
    for fr in frames:
        await eng.submit(Request("cam", "detect", Kind.FRAME, fr, meta={"w": 64, "h": 48, "ts_ms": fr.ts_ms}))
        await _drain(5)
    await _drain()
    motions = [r for r in event.got if r.kind == Kind.MOTION]
    assert len(motions) == 2, "first frame primes, every later frame is a tick"
    assert motions[0].data["ratio"] == 0.0 and motions[1].data["ratio"] > 0.0
    assert motions[1].data["boxes"], "a moved block gives a motion box"
    assert motions[1].data["w"] == 64 and motions[1].data["frame"] is frames[2]
    assert frames[0].released and not frames[2].released, "the priming frame was released; a tick's frame is handed on"
    assert stream.svc("cam")._det is detect.dets["cam"], "the graph object sees the detector for /motion"
    store.set_settings("cam", "detect", {"enabled": False})
    await asyncio.sleep(0.12)
    assert detect.wants_frame("cam") is False, "disabled per camera through settings, live"
    await eng.stop()


# ── event ────────────────────────────────────────────────────────────────────

async def test_event_owns_engine_ring_and_applies_settings_live(tmp_path):
    reg = Registry(); stream = FakeStream(reg); event = EventPipeline(stream)
    store, eng = await _engine(tmp_path, stream, event)
    store.set_settings("cam", "event", {"detector": "none", "motion_thresh": 0.05, "keep_s": 5})
    await stream.ensure("cam", {})
    await eng.submit(Request("cam", "event", Kind.AU, b"\x00\x00\x00\x01\x65" + b"x" * 100,
                             meta={"ts_ms": 1000, "flags": 0x06}))
    await eng.submit(Request("cam", "event", Kind.AUDIO, b"aac", meta={"ts_ms": 1001, "flags": 0}))
    await _drain()
    e = event.engines["cam"]
    assert e.cam_id == "cam" and e.detector is None and e.motion_thresh == 0.05
    assert e.ring.bytes == 105 + 3, "AU and audio records land in the ring with their flags"
    svc = stream.svc("cam")
    assert svc.event_engine is e and "infer_mode" in svc.applied
    # a motion tick below threshold: no event, no crash without a detector
    from nnvideo.frame import Frame
    fr = Frame.from_i420_array(_frame(), 64, 48)
    await eng.submit(Request("cam", "event", Kind.MOTION, {"ratio": 0.01, "boxes": [], "frame": fr, "w": 64, "h": 48},
                             meta={"ts_ms": 1002}))
    await _drain()
    assert e.stats["events"] == 0 and fr.released, "a frame below the motion threshold is released at once"
    # settings edit through the store is live on the next run
    store.set_settings("cam", "event", {"motion_thresh": 0.2, "quiet_s": 9, "max_s": 7})
    await eng.submit(Request("cam", "event", Kind.AUDIO, b"a", meta={"ts_ms": 1003}))
    await _drain()
    assert (e.motion_thresh, e.quiet_s, e.max_ms) == (0.2, 9.0, 7000)
    assert eng.state("event", "cam").failures == []
    await eng.stop()


# ── control ──────────────────────────────────────────────────────────────────

class FakeSess:
    def __init__(self): self.sent = []; self.device_pub = b"\x01" * 32
    def send(self, b): self.sent.append(bytes(b))


async def test_control_starts_adapt_on_connect_from_registry_and_stops_on_disconnect(tmp_path):
    reg = Registry(); stream = FakeStream(reg); control = ControlPipeline(stream, reg)
    store, eng = await _engine(tmp_path, stream, control)
    store.set_settings("cam", "control", {"adapt_fps": 25, "start_bitrate": 1_000_000})
    sess = FakeSess()
    reg.connect("cam", peer="x", ingest_port=1, session=sess)
    await eng.submit(Request("cam", "control", Kind.CONNECT))
    await _drain()
    ctrl = control.ctrls["cam"]
    assert ctrl.cam_id == "cam" and ctrl.fps == 25 and ctrl.br == 1_000_000
    assert reg.raw("cam")["ctrl"] is ctrl and "ctrl" not in reg.get("cam"), "the controller is a raw fact, not API output"
    svc = stream.svc("cam")
    assert svc.sess is sess and svc.ctrl is ctrl and svc._adapt_ctrl is ctrl and svc.pushed == 1
    assert len(sess.sent) == 2 and sess.sent[0][0] == 0xC7, "GOP + bitrate commands went out on the session"
    await eng.submit(Request("cam", "control", Kind.COMMAND, {"name": "force_idr"}))
    await _drain()
    assert len(sess.sent) == 3 and sess.sent[2][1] == ctrl.CMD_FORCE_IDR
    # setting adapt off while connected stops the controller on the next tick
    store.set_settings("cam", "control", {"adapt": False})
    await eng.submit(Request("cam", "control", Kind.TICK))
    await _drain()
    assert "cam" not in control.ctrls and ctrl._stop.is_set() and svc.sess is sess
    store.set_settings("cam", "control", {"adapt": True})
    await eng.submit(Request("cam", "control", Kind.TICK))
    await _drain()
    assert "cam" in control.ctrls, "and back on"
    await eng.submit(Request("cam", "control", Kind.DISCONNECT))
    await _drain()
    assert "cam" not in control.ctrls and svc.sess is None and svc.ctrl is None
    # a file camera (no session) is simply skipped
    reg.connect("file", peer="file:x", ingest_port=0)
    await eng.submit(Request("file", "control", Kind.CONNECT))
    await _drain()
    assert "file" not in control.ctrls and eng.state("control", "file").failures == []
    await eng.stop()


# ── hub: policy fetch ─────────────────────────────────────────────────────────

async def test_hub_fetches_policy_and_applies_only_on_version_change(tmp_path):
    fetches = []
    app = web.Application()

    async def inference(r):
        fetches.append(r.match_info["cam"])
        return web.json_response({"policy": {"version": 7, "engines": {"service": {"classes": {"person": {}}}}}})

    async def cameras(r):
        return web.json_response({"ok": True})
    app.router.add_get("/api/v1/cameras/{cam}/inference", inference)
    app.router.add_post("/api/v1/cameras", cameras)
    srv = TestServer(app); await srv.start_server()
    reg = Registry(); stream = FakeStream(reg); hub = HubPipeline("http://127.0.0.1:8880", stream)
    store, eng = await _engine(tmp_path, stream, hub)
    store.set_settings("*", "hub", {"hub_url": str(srv.make_url("")), "policy_refresh_s": 0})
    store.add_camera("cam3", "cam3")
    await stream.ensure("cam3", {})
    await eng.submit(Request("cam3", "hub", Kind.TICK)); await _drain()
    await eng.submit(Request("cam3", "hub", Kind.TICK)); await _drain()
    svc = stream.svc("cam3")
    assert fetches == ["cam3", "cam3"]
    assert svc.policy_version == 7 and svc.policy_doc["engines"]["service"]["classes"] == {"person": {}}
    assert svc.applied.count(("policy", 7)) == 1, "same version twice: applied once"
    assert hub.last["cam3"]["policy_version"] == 7
    await eng.stop(); await srv.close()


# ── ingest fan-out ───────────────────────────────────────────────────────────

async def test_ingest_fans_au_to_stream_and_event_and_detect_records_to_event(tmp_path):
    stream, event, hub, control = Sink("stream"), Sink("event"), Sink("hub"), Sink("control")
    store, eng = await _engine(tmp_path, IngestPipeline(), stream, event, hub, control)
    store.set_settings("cam", "ingest", {"avsync_window_ms": 0})
    await eng.submit(Request("cam", "ingest", Kind.CONNECT, {"peer": "p"}))
    for ts in (100, 200):
        await eng.submit(Request("cam", "ingest", "record",
                                 {"typ": NN_REC_VIDEO, "flags": 0x06, "seq": 0, "payload": b"au"}, ts_device=ts / 1000.0))
    await _drain()
    # window 0: every record up to the newest timestamp is released at once
    assert [r.kind for r in stream.got] == [Kind.CONNECT, Kind.AU, Kind.AU]
    assert [r.kind for r in event.got] == [Kind.CONNECT, Kind.AU, Kind.AU] and event.got[1].meta["flags"] == 0x06
    assert [r.kind for r in hub.got] == [Kind.CONNECT] and [r.kind for r in control.got] == [Kind.CONNECT]
    await eng.stop()


# ── stream: one graph per camera even under concurrent first use ─────────────

async def test_stream_ensure_is_single_flight_per_camera(tmp_path, monkeypatch):
    import nnvideo.pipes as pipes
    built = []

    class SlowCameraStream:
        def __init__(self, cam, cfg, registry, engine, on_frame, frame_wanted=None):
            built.append(cam); self.cam = cam; self.port = 1; self.svc = SimpleNamespace(hls_dir="x")

        async def serve(self):
            await asyncio.sleep(0.05)          # the real one binds a port here

        async def close(self):
            pass
    monkeypatch.setattr(pipes, "CameraStream", SlowCameraStream)
    reg = Registry(); stream = StreamPipeline(reg)
    store, eng = await _engine(tmp_path, stream)
    a, b, c = await asyncio.gather(stream.ensure("cam", {}), stream.ensure("cam", {}), stream.ensure("cam", {}))
    assert a is b is c and built == ["cam"], "three concurrent askers, one graph"
    await stream.ensure("other", {})
    assert built == ["cam", "other"]
    await eng.stop()
