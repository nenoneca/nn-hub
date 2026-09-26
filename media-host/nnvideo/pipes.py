"""The pipelines: ingest → stream / detect → event, plus control and hub.

Phase 2 (2026-09-21): detection, events and control are their own pipelines
with their own settings, moved out of the legacy service class piece by
piece.  The legacy `VideoService` instance per camera is now only the
GStreamer graph (appsrc, HLS muxer, live branches, the decode branch that
hands frames to `detect`) plus the state container the compatibility routes
read; the objects the routes reach into (`_det`, `event_engine`, `ctrl`,
`sess`) are owned by the pipelines and attached to it.

    ingest   records → AU/AUDIO (A/V-sync order) → stream + event;
             status/heartbeat/cfgack → stream (+ hub); detect records → event
    stream   the graph: feed appsrc, live-view fan-out, audio sink; its decode
             branch calls `on_frame` → a FRAME request for detect
    detect   THREAD: motion detection on decoded frames → MOTION → event
    event    THREAD: the ring buffer + motion-gated recorder (EventEngine)
    control  the session back-channel: adapt controller, config push, commands
    hub      registration heartbeat, firmware ride-along, policy fetch
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Optional

from aiohttp import ClientSession, ClientTimeout, web

from .pipeline import Mode, Pipeline
from .queue import Policy
from .request import Kind, Request

HERE = Path(__file__).resolve().parent.parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

NN_REC_VIDEO, NN_REC_AUDIO, NN_REC_STATUS = 0x56, 0x41, 0x53
NN_REC_DETECT, NN_REC_CFGACK, NN_REC_HEART = 0x44, 0x4B, 0x48


def _log(cam: str, pipe: str, msg: str) -> None:
    print(f"[{cam}/{pipe}] {msg}", flush=True)


# ── ingest ───────────────────────────────────────────────────────────────────

class IngestPipeline(Pipeline):
    """Record → typed requests.  Video and audio are ordered by device
    timestamp inside an A/V-sync window per camera and go to `stream` (the
    graph) and `event` (the ring); status/heartbeat/cfgack go to `stream`
    (device state the routes show) and `hub`; detect records go to `event`."""
    name = "ingest"
    mode = Mode.ASYNC
    policy = Policy.DROP_OLDEST
    per_camera_cap = 512
    budget_s = 0.2
    workers = 1
    defaults = {"avsync_window_ms": 200, "audio": True}

    def __init__(self):
        super().__init__()
        self._avsync: dict[str, object] = {}
        self._pending: dict[str, list] = {}

    def _sync(self, cam: str, cfg: dict):
        av = self._avsync.get(cam)
        if av is None:
            from video_service import AVSync
            out: list = []
            # AVSync never looks inside the payload, so the record flags ride
            # along in a tuple and reach the ring buffer intact.
            av = AVSync(on_video=lambda pl, ts: out.append((Kind.AU, pl, ts)),
                        on_audio=lambda pl, ts: out.append((Kind.AUDIO, pl, ts)),
                        window_ms=int(cfg.get("avsync_window_ms", 200)))
            self._avsync[cam] = av
            self._pending[cam] = out
        return av, self._pending[cam]

    async def run(self, req: Request, cfg: dict):
        if req.kind in (Kind.CONNECT, Kind.DISCONNECT):
            if req.kind == Kind.DISCONNECT:
                self._avsync.pop(req.camera_id, None)
                self._pending.pop(req.camera_id, None)
            return [req.emit(p, req.kind, req.data) for p in ("hub", "stream", "control", "event")]
        if req.kind != "record":
            return None
        d = req.data
        typ, payload, ts_ms = d["typ"], d["payload"], int((req.ts_device or 0) * 1000)
        if typ == NN_REC_VIDEO or (typ == NN_REC_AUDIO and cfg.get("audio", True)):
            av, out = self._sync(req.camera_id, cfg)
            av.feed(typ, ts_ms, (payload, d.get("flags", 0)))
            emitted = []
            for kind, (pl, flags), ts in out:
                for p in ("stream", "event"):
                    e = req.emit(p, kind, pl, ts_ms=ts, flags=flags)
                    e.ts_device = ts / 1000.0
                    emitted.append(e)
            out.clear()
            return emitted
        if typ == NN_REC_STATUS:
            try:
                doc = json.loads(payload.decode())
            except Exception:
                return None
            return [req.emit("stream", Kind.STATUS, doc, dev_ts=ts_ms),
                    req.emit("hub", Kind.STATUS, doc, dev_ts=ts_ms)]
        if typ == NN_REC_HEART:
            try:
                doc = json.loads(payload.decode())
            except Exception:
                doc = {}
            return [req.emit("stream", Kind.HEARTBEAT, doc), req.emit("hub", Kind.HEARTBEAT, doc)]
        if typ == NN_REC_DETECT:
            from video_service import parse_detect_record
            dets = parse_detect_record(payload)
            return [req.emit("event", Kind.DETECT_RECORD, dets, dev_ts=ts_ms)] if dets is not None else None
        if typ == NN_REC_CFGACK:
            try:
                return [req.emit("stream", "cfgack", json.loads(payload.decode()))]
            except Exception:
                return None
        return None

    def reset(self, camera_id: str) -> None:
        self._avsync.pop(camera_id, None); self._pending.pop(camera_id, None)

    def describe(self) -> dict:
        return {"avsync": {c: av.status() for c, av in self._avsync.items()}}


# ── stream ───────────────────────────────────────────────────────────────────

class CameraStream:
    """One camera's graph: a legacy VideoService instance (appsrc, HLS,
    live-view, audio sink, the hardware decode branch) plus its control app
    on an ephemeral loopback port.  Everything per-camera comes from the
    store settings, nothing from the process environment."""

    def __init__(self, cam: str, cfg: dict, registry, engine, on_frame, frame_wanted=None):
        import video_service as vs
        vs.SINGLE_PROCESS = True
        self.cam, self.cfg, self.registry, self.engine = cam, cfg, registry, engine
        self.fed = 0
        svc = vs.VideoService()
        svc.cam_id = cam
        svc.run_glib_loop = False                     # the process runs ONE GLib loop
        svc.enc_qp = int(cfg.get("enc_qp", 0) or 0)
        svc.hls_dir = cfg.get("hls_dir") or f"/dev/shm/nn-hls-{cam}"
        svc.hls_port = registry.allocate_port(cam, "hls_feed")
        svc.avsync_window_ms = int(cfg.get("avsync_window_ms", 200))
        svc.adapt_enabled = False                     # the control pipeline owns adapt now
        svc.snapshot_interval = int(cfg.get("snapshot_interval_s", 30))
        if cfg.get("snapshot_dir"):
            svc.snapshot_dir = cfg["snapshot_dir"]
        state_dir = cfg.get("state_dir") or os.path.join(os.path.dirname(str(engine.store.path)), "state", cam)
        os.makedirs(state_dir, exist_ok=True)
        self.state_dir = state_dir
        svc.keydir = cfg.get("keydir") or state_dir      # /api/camera/provinfo reads the host key here
        if not cfg.get("keydir"):
            # a camera created through the API has no per-slot key directory:
            # its host key is the store's (its own id if imported, else the
            # default) — export it so the legacy route answers the truth
            # instead of generating an unrelated key on first read
            hk = engine.store.host_key(cfg.get("host_key_id") or cam) or engine.store.host_key("default")
            kp = os.path.join(state_dir, "hub_enc_key.raw")
            if hk is not None and not os.path.exists(kp):
                with open(kp, "wb") as f:
                    f.write(hk[1])
                os.chmod(kp, 0o600)
        svc.stream_port = int(cfg.get("ingest_port", 0) or 0)
        svc.hls_opts = {k: v for k, v in {
            "NN_HLS_REENCODE": cfg.get("hls_reencode"), "NN_HLS_ZEROCOPY": cfg.get("hls_zerocopy"),
            "NN_HLS_GOP": cfg.get("hls_gop"), "NN_HLS_SEGTIME": cfg.get("hls_segtime"),
            "NN_HLS_MODE": cfg.get("hls_mode"), "NN_HLS_WALLCLOCK": cfg.get("hls_wallclock"),
            "NN_HLS_PDT_MAX_SKEW": cfg.get("hls_pdt_max_skew"), "NN_TRANSCODED": cfg.get("transcoded"),
            "NN_HLS_WATCHDOG_S": cfg.get("hls_watchdog_s"),
        }.items() if v is not None}
        loop = engine.loop
        svc.on_wedged = lambda stalled: loop.call_soon_threadsafe(
            asyncio.ensure_future, engine.reset("stream", cam, f"HLS wedged {stalled:.0f}s"))
        svc.on_fatal = lambda reason: loop.call_soon_threadsafe(
            asyncio.ensure_future, engine.reset("stream", cam, f"graph fatal: {reason}"))
        svc.on_frame = on_frame                        # decode branch → detect pipeline
        svc.frame_wanted = frame_wanted                # asked before the copy; paces detection
        self.svc = svc
        svc.start()
        if svc.snapshot_interval > 0:
            threading.Thread(target=svc._snapshot_saver, daemon=True, name=f"snap:{cam}").start()
        if cfg.get("audio", True):
            try:
                svc.audio = vs.AudioSink(cfg.get("audio_file"))
            except Exception as e:                           # noqa: BLE001
                _log(cam, "stream", f"audio demux disabled ({e})")
        if cfg.get("detect_branch", True):
            svc.add_detection_branch(None, decoder=cfg.get("motion_decoder", "auto"),
                                     fmt=str(cfg.get("detect_format", "I420")))
        svc.load_infer_mode(state_dir)
        self.app = vs.make_app(svc)                    # also wires the HLS muxer threads
        self.port = registry.allocate_port(cam, "control")
        self.runner: Optional[web.AppRunner] = None

    async def serve(self) -> None:
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", self.port).start()
        self.registry.update(self.cam, control_url=f"http://127.0.0.1:{self.port}")

    async def close(self) -> None:
        self.svc.close()                       # helper loops + ffmpeg children end with the graph
        try:
            if self.runner:
                await self.runner.cleanup()
        except Exception:
            pass
        try:
            from gi.repository import Gst
            self.svc.pipeline.set_state(Gst.State.NULL)
        except Exception:
            pass
        for port in (self.port, self.svc.hls_port):
            self.registry.free_port(port)


class StreamPipeline(Pipeline):
    name = "stream"
    mode = Mode.ASYNC
    policy = Policy.DROP_OLDEST
    per_camera_cap = 256
    budget_s = 30.0             # a run is µs for an AU; the graph build on first request is not
    workers = 1
    defaults = {"hls_reencode": "1", "hls_gop": 8, "hls_segtime": "1", "enc_qp": 0,
                "snapshot_interval_s": 30, "avsync_window_ms": 200, "hls_watchdog_s": 120,
                "audio": True, "detect_branch": True, "motion_decoder": "auto",
                "detect_format": "I420"}      # GRAY8 = luma only, read from the decoder's DMABUF without a copy

    def __init__(self, registry):
        super().__init__()
        self.registry = registry
        self.streams: dict[str, CameraStream] = {}
        self._building: dict[str, asyncio.Lock] = {}
        self.frame_sink = None      # set by the service: fn(cam, frame)
        self.frame_gate = None      # set by the service: fn(cam) -> bool, asked before a frame is copied

    def svc(self, cam: str):
        cs = self.streams.get(cam)
        return cs.svc if cs else None

    async def ensure(self, cam: str, cfg: dict) -> CameraStream:
        """The camera's graph, built on first use.  Single-flight per camera:
        several pipelines (stream, control, the /cam proxy) may ask at the
        same moment and a second graph for the same camera would leak a
        decoder branch and an HLS muxer writing into the same directory."""
        cs = self.streams.get(cam)
        if cs is not None:
            return cs
        lock = self._building.setdefault(cam, asyncio.Lock())
        async with lock:
            cs = self.streams.get(cam)
            if cs is None:
                # shielded: a caller cancelled by its own budget (control's
                # CONNECT during a busy start) must not abandon a half-served
                # graph — the build finishes and is registered regardless
                cs = await asyncio.shield(self._build(cam, cfg))
        return cs

    async def _build(self, cam: str, cfg: dict) -> CameraStream:
        sink, gate = self.frame_sink, self.frame_gate
        cs = CameraStream(cam, cfg, self.registry, self.engine,
                          (lambda frame: sink(cam, frame)) if sink else None,
                          (lambda: gate(cam)) if gate else None)
        await cs.serve()
        self.streams[cam] = cs
        _log(cam, "stream", f"graph up, control on :{cs.port}, hls {cs.svc.hls_dir}")
        return cs

    async def run(self, req: Request, cfg: dict):
        cs = await self.ensure(req.camera_id, cfg)
        svc = cs.svc
        k = req.kind
        if k == Kind.AU:
            ts_ms = int(req.meta.get("ts_ms") or (req.ts_device or 0) * 1000)
            svc.feed(req.data)
            svc.ws_forward(req.data, ts_ms)
            cs.fed += 1
            return None
        if k == Kind.AUDIO:
            if svc.audio:
                svc.audio.feed(req.data, int(req.meta.get("ts_ms") or 0))
            return None
        if k == Kind.STATUS:
            doc = dict(req.data or {})
            ad = getattr(svc, "_adapt_ctrl", None)
            if ad is not None and getattr(ad, "net_kbps", 0):
                doc["net_kbps"] = ad.net_kbps
            svc.cam_settings = doc
            svc.cam_settings_dev_ts = req.meta.get("dev_ts", 0)
            svc.cam_settings_ts = time.time()
            if "infer" in doc and doc["infer"] != svc.edge_caps:
                # only on change: applying on every 10 s status record pushed
                # the device config each time and the device acked each push
                svc.edge_caps = doc["infer"]
                svc.apply_infer_mode()
            return None
        if k == Kind.HEARTBEAT:
            svc.device_hb = req.data
            svc.device_hb_ts = time.time()
            return None
        if k == "cfgack":
            a = req.data or {}
            try:
                svc.device_policy_version = int(a.get("v") or 0)
            except Exception:
                pass
            _log(req.camera_id, "stream", f"device applied config v{svc.device_policy_version}"
                 f"{'' if a.get('applied') else ' (REJECTED: %s)' % a.get('err')}")
            return None
        return None

    def reset(self, camera_id: str) -> None:
        cs = self.streams.pop(camera_id, None)
        if cs is not None:
            asyncio.ensure_future(cs.close())
            _log(camera_id, "stream", "graph torn down; rebuilt on the next request")

    def describe(self) -> dict:
        return {"graphs": {c: {"control_port": cs.port, "fed": cs.fed, "hls_dir": cs.svc.hls_dir}
                           for c, cs in self.streams.items()}}


# ── detect ───────────────────────────────────────────────────────────────────

class DetectPipeline(Pipeline):
    """Motion detection on decoded frames (THREAD: numpy releases the
    interpreter lock).  One MotionDetector per camera, attached to the
    camera's graph object for the /motion, /snapshot and /mjpeg routes.
    `wants_frame` is what the graph's decode branch asks before it copies a
    frame out, so only one frame per detection interval crosses over."""
    name = "detect"
    mode = Mode.THREAD
    policy = Policy.DROP_OLDEST
    per_camera_cap = 2
    budget_s = 2.0
    workers = 1
    defaults = {"interval_ms": 200, "diff_thresh": 18, "min_area_frac": 0.0008, "proc_width": 480}

    def __init__(self, stream: StreamPipeline):
        super().__init__()
        self.stream = stream
        self.dets: dict[str, object] = {}
        self._last: dict[str, float] = {}
        self.frames_in = 0

    def wants_frame(self, cam: str) -> bool:
        cfg = self.engine.settings.get(cam, "detect")
        if not cfg.get("enabled", True):
            return False
        now = time.monotonic()
        if now - self._last.get(cam, 0.0) < cfg.get("interval_ms", 200) / 1000.0:
            return False
        self._last[cam] = now
        return True

    def _det(self, cam: str, cfg: dict):
        det = self.dets.get(cam)
        if det is None:
            from video_service import MotionDetector
            det = MotionDetector(interval_ms=int(cfg["interval_ms"]), diff_thresh=int(cfg["diff_thresh"]),
                                 min_area_frac=float(cfg["min_area_frac"]), proc_width=int(cfg["proc_width"]))
            det.interval = 0.0                      # the hook already paces frames
            self.dets[cam] = det
            _log(cam, "detect", f"motion detector up (every {cfg['interval_ms']} ms, diff {cfg['diff_thresh']})")
        else:
            det.diff_thresh = int(cfg["diff_thresh"]); det.min_area_frac = float(cfg["min_area_frac"])
            det.proc_width = int(cfg["proc_width"])
        svc = self.stream.svc(cam)
        if svc is not None and getattr(svc, "_det", None) is not det:
            svc._det = det                          # routes: /motion, /snapshot fallback, /mjpeg
        return det

    def run_sync(self, req: Request, cfg: dict):
        if req.kind != Kind.FRAME:
            return None
        det = self._det(req.camera_id, cfg)
        frame = req.data                                    # nnvideo.frame.Frame: views, no copy
        out: list = []
        det.on_tick = lambda ts, ratio, boxes, fr: out.append((ts, ratio, boxes, fr))
        self.frames_in += 1
        det.feed_frame(frame)                               # releases the frame itself unless on_tick took it
        if not out:
            return None
        ts, ratio, boxes, fr = out[-1]
        return [req.emit("event", Kind.MOTION, {"ratio": ratio, "boxes": boxes, "frame": fr,
                                                "w": fr.w, "h": fr.h}, ts_ms=ts)]

    def reset(self, camera_id: str) -> None:
        self.dets.pop(camera_id, None)

    def describe(self) -> dict:
        return {"frames_in": self.frames_in,
                "detectors": {c: {"frames": d.frames, "ticks": d.ticks} for c, d in self.dets.items()}}


# ── event ────────────────────────────────────────────────────────────────────

class EventPipeline(Pipeline):
    """The A/V ring and the motion-gated recorder: one EventEngine per camera
    (thread-safe by design; mux+upload run on its own threads).  Settings
    are re-applied whenever the store version moves, so a threshold edit
    through the API is live on the next request without a restart."""
    name = "event"
    mode = Mode.THREAD
    policy = Policy.DROP_OLDEST
    per_camera_cap = 1024
    budget_s = 5.0
    workers = 1
    defaults = {"hub_url": "http://127.0.0.1:8769", "keep_s": 60, "max_s": 120,
                "motion_thresh": 0.02, "quiet_s": 3.0, "yolo_interval_s": 1.5, "min_score": 0.45,
                "detector": "auto", "yolo_url": "", "yolo_model": ""}
    LIVE = ("motion_thresh", "quiet_s", "yolo_interval_s", "min_score")

    def __init__(self, stream: StreamPipeline):
        super().__init__()
        self.stream = stream
        self.engines: dict[str, object] = {}
        self._cfg_version: dict[str, int] = {}
        self.caps: dict[str, dict | None] = {}

    def _detector(self, cam: str, cfg: dict):
        which = str(cfg.get("detector", "auto"))
        if which == "none":
            return None
        if which in ("auto", "inferd"):
            try:
                from inferd_client import ReconnectingInferd
                d = ReconnectingInferd(owner=cam)
                _log(cam, "event", f"detector: nn-inferd {d.devices}")
                return d
            except Exception as e:                           # noqa: BLE001
                _log(cam, "event", f"nn-inferd unavailable ({e})")
                if which == "inferd":
                    return None
        if which in ("auto", "npu") and cfg.get("yolo_url"):
            try:
                from yolox_detector import NpuDetector
                d = NpuDetector(cfg["yolo_url"])
                _log(cam, "event", f"detector: NPU at {cfg['yolo_url']}")
                return d
            except Exception as e:                           # noqa: BLE001
                _log(cam, "event", f"NPU unavailable ({e})")
        model = str(cfg.get("yolo_model") or "")
        if which in ("auto", "cpu", "local") and model:
            try:
                if model.endswith(".cix"):
                    from yolox_detector import YoloxNpuDetector
                    d = YoloxNpuDetector(model)
                    _log(cam, "event", "detector: NPU (CIX AIPU, local NOE_Engine)")
                else:
                    from yolox_detector import YoloxDetector
                    d = YoloxDetector(model)
                    _log(cam, "event", "detector: CPU onnxruntime")
                return d
            except Exception as e:                           # noqa: BLE001
                _log(cam, "event", f"yolox unavailable ({e}) — motion-only events")
        return None

    def _engine(self, cam: str, cfg: dict):
        eng = self.engines.get(cam)
        if eng is None:
            import event_engine as event_engine_mod
            detector = self._detector(cam, cfg)
            eng = event_engine_mod.EventEngine(
                hub_url=str(cfg["hub_url"]), detector=detector,
                keep_s=int(cfg["keep_s"]), max_s=int(cfg["max_s"]),
                motion_thresh=float(cfg["motion_thresh"]), quiet_s=float(cfg["quiet_s"]),
                yolo_interval_s=float(cfg["yolo_interval_s"]), min_score=float(cfg["min_score"]),
                cam_id=cam)
            self.engines[cam] = eng
            self.caps[cam] = ({"model": os.path.basename(str(cfg.get("yolo_model") or cfg.get("yolo_url") or "yolox")),
                               "labels": "coco80", "classes": 80, "max_agg": 30} if detector is not None else None)
            self._cfg_version[cam] = self.engine.settings.version
            _log(cam, "event", f"engine up: T={cfg['motion_thresh']} keep={cfg['keep_s']}s max={cfg['max_s']}s "
                               f"detector={'on' if detector else 'OFF'} hub={cfg['hub_url']}")
        elif self._cfg_version.get(cam) != self.engine.settings.version:
            self._cfg_version[cam] = self.engine.settings.version
            for k in self.LIVE:
                setattr(eng, k, float(cfg[k]))
            eng.max_ms = int(cfg["max_s"]) * 1000
            eng.hub_url = str(cfg["hub_url"]).rstrip("/")
        svc = self.stream.svc(cam)
        if svc is not None and getattr(svc, "event_engine", None) is not eng:
            svc.event_engine = eng                  # routes + policy/infer-mode application
            svc.service_caps = self.caps.get(cam)
            svc.apply_infer_mode()
        return eng

    def run_sync(self, req: Request, cfg: dict):
        cam, k = req.camera_id, req.kind
        if k == Kind.DISCONNECT:
            return None
        eng = self._engine(cam, cfg)
        if k == Kind.AU or k == Kind.AUDIO:
            eng.append("V" if k == Kind.AU else "A", int(req.meta.get("ts_ms") or 0),
                       int(req.meta.get("flags", 0)), req.data)
            return None
        if k == Kind.MOTION:
            d = req.data
            frame, ratio = d["frame"], d["ratio"]
            # the engine decides if/when it needs pixels (same gate as legacy);
            # a frame it takes is released by the inference path, otherwise here
            need = (eng._active is None and ratio >= eng.motion_thresh) or (eng._active is not None)
            if not need:
                frame.release()
            eng.motion_tick(int(req.meta.get("ts_ms") or time.time() * 1000), ratio, d["boxes"],
                            frame if need else None)
            return None
        if k == Kind.DETECT_RECORD:
            eng.edge_tick(req.meta.get("dev_ts", 0), req.data)
            return None
        return None

    def reset(self, camera_id: str) -> None:
        self.engines.pop(camera_id, None)
        self.caps.pop(camera_id, None)

    def describe(self) -> dict:
        return {"engines": {c: {k: v for k, v in e.stats.items() if k in ("events", "uploads_ok", "uploads_failed", "yolo_runs")}
                            for c, e in self.engines.items()}}


# ── control ──────────────────────────────────────────────────────────────────

class ControlPipeline(Pipeline):
    """The session back-channel: on CONNECT the adapt controller starts on
    that connection's session (from the registry) and the device config is
    pushed; on DISCONNECT it stops.  Commands (force IDR, push config) are
    coalesced per camera.  Sets `sess`/`ctrl` on the camera's graph object
    so the legacy reboot/clear/set routes keep working."""
    name = "control"
    mode = Mode.ASYNC
    policy = Policy.COALESCE
    budget_s = 30.0             # CONNECT may wait for the camera's graph to be built
    workers = 1
    defaults = {"adapt": True, "adapt_fps": 30, "start_bitrate": 3_000_000}

    def __init__(self, stream: StreamPipeline, registry):
        super().__init__()
        self.stream = stream
        self.registry = registry
        self.ctrls: dict[str, object] = {}

    def _stop(self, cam: str) -> None:
        c = self.ctrls.pop(cam, None)
        if c is not None:
            try: c.stop()
            except Exception: pass
        self.registry.update(cam, ctrl=None)
        svc = self.stream.svc(cam)
        if svc is not None:
            svc._adapt_ctrl = None; svc.ctrl = None; svc.sess = None

    async def run(self, req: Request, cfg: dict):
        cam = req.camera_id
        if req.kind == Kind.DISCONNECT:
            self._stop(cam)
            return None
        rec = self.registry.raw(cam)
        sess = rec.get("session") if rec else None
        if sess is None:
            return None                                  # file camera or not connected
        cs = await self.stream.ensure(cam, self.engine.settings.get(cam, "stream"))
        svc = cs.svc
        if req.kind == Kind.CONNECT:
            self._stop(cam)
            svc.sess = sess
            svc.device_policy_version = 0
            if cfg.get("adapt", True):
                import video_service as vs
                ctrl = vs.AdaptController(sess, fps=int(cfg["adapt_fps"]), start_bitrate=int(cfg["start_bitrate"]))
                ctrl.cam_id = cam
                ctrl.start()
                self.ctrls[cam] = ctrl
                svc._adapt_ctrl = ctrl; svc.ctrl = ctrl
                self.registry.update(cam, ctrl=ctrl)
                _log(cam, "control", f"adaptive control ON (fps {cfg['adapt_fps']})")
            svc.push_device_config()
            return None
        if req.kind == Kind.COMMAND:
            name = (req.data or {}).get("name")
            ctrl = self.ctrls.get(cam)
            if name == "force_idr" and ctrl is not None:
                ctrl.force_idr()
            elif name == "push_config":
                svc.push_device_config()
            return None
        if req.kind == Kind.TICK:
            # settings change: adapt switched off/on while connected
            has = cam in self.ctrls
            if has and not cfg.get("adapt", True):
                self._stop(cam); svc.sess = sess
                _log(cam, "control", "adaptive control OFF (setting)")
            elif not has and cfg.get("adapt", True):
                return [req.emit("control", Kind.CONNECT)]
        return None

    def reset(self, camera_id: str) -> None:
        self._stop(camera_id)

    def describe(self) -> dict:
        return {"sessions": {c: {"mode": getattr(k, "mode", None), "level": getattr(k, "level", None),
                                 "bitrate": getattr(k, "br", None), "fps_div": getattr(k, "fps_div", None),
                                 "net_kbps": getattr(k, "net_kbps", None)} for c, k in self.ctrls.items()}}


# ── hub ──────────────────────────────────────────────────────────────────────

class HubPipeline(Pipeline):
    """Everything said to the hub: the registration heartbeat (POST
    /api/v1/cameras {id, name, url, caps}) with the prefixed slot URL, the
    firmware identity ride-along from status records, and the inference
    policy pulled on the same beat and applied when its version moves."""
    name = "hub"
    mode = Mode.ASYNC
    policy = Policy.COALESCE
    budget_s = 10.0
    workers = 1
    defaults = {"hub_url": "http://127.0.0.1:8769", "interval_s": 10, "register": True,
                "policy_refresh_s": 30}

    def __init__(self, api_url: str, stream: StreamPipeline, event: Optional[EventPipeline] = None):
        super().__init__()
        self.api_url = api_url.rstrip("/")
        self.stream = stream
        self.event = event
        self.last: dict[str, dict] = {}
        self._http: Optional[ClientSession] = None
        self._fw_reported: dict[str, tuple] = {}

    async def http(self) -> ClientSession:
        if self._http is None or self._http.closed:
            self._http = ClientSession(timeout=ClientTimeout(total=8))
        return self._http

    async def _policy(self, cam: str, hub: str, cfg: dict) -> None:
        svc = self.stream.svc(cam)
        if svc is None:
            return
        st = self.last.setdefault(cam, {})
        if time.time() - st.get("policy_at", 0) < float(cfg.get("policy_refresh_s", 30)):
            return
        st["policy_at"] = time.time()
        try:
            async with (await self.http()).get(f"{hub}/api/v1/cameras/{cam}/inference") as r:
                doc = (await r.json()).get("policy") or {}
        except Exception as e:                                   # noqa: BLE001
            st["policy_error"] = str(e)[:120]
            return
        ver = int(doc.get("version") or 0)
        if ver and ver != getattr(svc, "policy_version", 0):
            svc.policy_doc = doc
            svc.policy_version = ver
            svc.apply_policy()
            st["policy_version"] = ver
            _log(cam, "hub", f"inference policy v{ver} applied")

    async def run(self, req: Request, cfg: dict):
        hub = str(cfg.get("hub_url") or "http://127.0.0.1:8769").rstrip("/")
        cam = req.camera_id
        if req.kind == Kind.STATUS:
            doc = req.data or {}
            fw, img = doc.get("fw"), doc.get("img")
            sig = (fw, doc.get("wdt_rc"))
            due = time.time() - self.last.get(cam, {}).get("fw_at", 0) > 900
            if fw and img and (sig != self._fw_reported.get(cam) or due):
                body = {"version": fw, "device_type": img}
                for k in ("rst", "wdt_rc"):
                    if k in doc:
                        body[k] = doc[k]
                try:
                    async with (await self.http()).put(f"{hub}/api/v1/cameras/{cam}/bundle", json=body) as r:
                        if r.status < 300:
                            self._fw_reported[cam] = sig
                            self.last.setdefault(cam, {})["fw_at"] = time.time()
                            _log(cam, "hub", f"reported firmware {fw} ({img})")
                except Exception as e:                           # noqa: BLE001
                    _log(cam, "hub", f"fw report failed: {e}")
            return None
        if req.kind in (Kind.TICK, Kind.CONNECT, Kind.DISCONNECT):
            if not cfg.get("register", True):
                return None
            camrow = self.engine.store.camera(cam) or {}
            name = camrow.get("name") or cam
            caps = {}
            svc = self.stream.svc(cam)
            if svc is not None:
                if svc.edge_caps:
                    caps["infer"] = svc.edge_caps
                err = (getattr(svc, "cam_settings", None) or {}).get("infer_error")
                if err and not svc.edge_caps:
                    caps["infer_error"] = str(err)[:200]
                if getattr(svc, "service_caps", None):
                    caps["service_infer"] = svc.service_caps
            body = {"id": cam, "name": name, "url": f"{self.api_url}/cam/{cam}", "caps": caps}
            try:
                async with (await self.http()).post(f"{hub}/api/v1/cameras", json=body) as r:
                    self.last.setdefault(cam, {}).update({"at": time.time(), "status": r.status})
            except Exception as e:                               # noqa: BLE001
                self.last.setdefault(cam, {}).update({"at": time.time(), "error": str(e)[:120]})
            await self._policy(cam, hub, cfg)
        return None

    def describe(self) -> dict:
        return {"registrations": self.last}
