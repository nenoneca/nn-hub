"""
Motion/AI event pipeline for the nn media server.

  ingest ──► AVRingBuffer (last L=60 s of A/V records, GOP-aware, in RAM)
                  │
  MotionDetector tick (every I=200 ms) ──► EventEngine
      pixel-diff ratio > T ──► Detector (YOLOX) on the tick frame
          objects found ──► event STARTS at the GOP containing the trigger
              records copied out of the ring while ACTIVE (event may exceed
              the ring length, up to V=120 s)
          detection quiet for QUIET_S or length ≥ V ──► finalize:
              mux H.264+AAC → MP4 (BytesIO, never touches disk)
              event log (JSONL, one line per tick) in RAM
              ask the hub for upload links, upload both, drop the memory.

Nothing in this module writes event media to persistent storage — the only
disk writes happen on the hub after upload.
"""

from __future__ import annotations

import io
import json
import logging
import os
import threading
import time
import urllib.request
from collections import deque
from typing import Callable, Optional

log = logging.getLogger("event_engine")

NAL_IDR, NAL_SPS = 5, 7


def _first_nal(buf: bytes) -> int:
    if buf[:4] == b"\x00\x00\x00\x01":
        return buf[4] & 0x1F
    if buf[:3] == b"\x00\x00\x01":
        return buf[3] & 0x1F
    return -1


# ── ring buffer ──────────────────────────────────────────────────────────────

class AVRingBuffer:
    """Last `keep_s` seconds of A/V records, grouped into GOP-aligned chunks.

    A chunk = one video GOP (starts at an SPS/IDR access unit) plus the audio
    records that arrived during it.  Eviction drops whole chunks, so a
    snapshot always starts at a decodable point.
    """

    def __init__(self, keep_s: int = 60, max_bytes: int = 48 * 1024 * 1024):
        self.keep_ms = keep_s * 1000
        self.max_bytes = max_bytes            # hard cap regardless of time
        self._chunks: deque[list] = deque()   # each: [(kind, ts, flags, bytes)]
        self._cur: list = []
        self._lock = threading.Lock()
        self.bytes = 0

    def append(self, kind: str, ts: int, flags: int, payload: bytes) -> None:
        with self._lock:
            # video records are fragments; flags bit1 (VID_START) marks a new
            # access unit — a new GOP begins when that AU starts with SPS/IDR
            if kind == "V" and (flags & 0x02) and _first_nal(payload) in (NAL_SPS, NAL_IDR):
                if self._cur:
                    self._chunks.append(self._cur)
                self._cur = []
            self._cur.append((kind, ts, flags, payload))
            self.bytes += len(payload)
            # evict whole chunks older than the window OR over the byte cap
            while self._chunks and (
                    ts - self._chunks[0][0][1] > self.keep_ms
                    or self.bytes > self.max_bytes):
                self.bytes -= sum(len(r[3]) for r in self._chunks.popleft())
            # a GOP that never closes (no IDR arrived) would grow _cur forever
            # and eviction only touches closed chunks — force-close an oversized
            # open chunk so it becomes evictable
            if sum(len(r[3]) for r in self._cur) > self.max_bytes:
                self._chunks.append(self._cur); self._cur = []

    def snapshot_from(self, trigger_ts: int) -> list:
        """All records from the start of the GOP containing `trigger_ts`."""
        with self._lock:
            out: list = []
            chunks = list(self._chunks) + ([self._cur] if self._cur else [])
            start = 0
            for i, ch in enumerate(chunks):
                if ch[0][1] <= trigger_ts:
                    start = i
            for ch in chunks[start:]:
                out.extend(ch)
            return out


# ── in-memory MP4 mux ────────────────────────────────────────────────────────

def mux_mp4(records: list, fps_hint: float = 25.0) -> bytes:
    """Remux H.264 (Annex-B fragments) + AAC (ADTS) records into a fragmented
    MP4 entirely in memory: ffmpeg -c copy fed through /dev/shm FIFOs (kernel
    pipes — nothing touches persistent storage).  PyAV was tried first but its
    bitstream-filter layer segfaults on this ADTS→ASC path."""
    import os
    import subprocess
    import tempfile

    # reassemble full video access units (fragments carry VID_START/END flags)
    v_aus: list[tuple[int, bytes]] = []
    cur: list[bytes] = []
    cur_ts = 0
    for kind, ts, flags, payload in records:
        if kind != "V":
            continue
        if flags & 0x02:                       # VID_START
            cur = [payload]; cur_ts = ts
        else:
            cur.append(payload)
        if flags & 0x04 and cur:               # VID_END
            v_aus.append((cur_ts, b"".join(cur)))
            cur = []
    if not v_aus:
        raise ValueError("no complete video access units in event window")
    audio = b"".join(pl for kind, ts, fl, pl in records if kind == "A")
    video = b"".join(au for _, au in v_aus)

    dur = (v_aus[-1][0] - v_aus[0][0]) / 1000.0
    fps = (len(v_aus) - 1) / dur if dur > 0.5 else fps_hint
    fps = max(5.0, min(60.0, fps))

    tmpd = tempfile.mkdtemp(dir="/dev/shm")
    fv = os.path.join(tmpd, "v.h264")
    fa = os.path.join(tmpd, "a.aac")
    os.mkfifo(fv)
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error",
           "-fflags", "+genpts", "-r", f"{fps:.3f}", "-f", "h264", "-i", fv]
    if audio:
        os.mkfifo(fa)
        cmd += ["-f", "aac", "-i", fa, "-bsf:a", "aac_adtstoasc"]
    cmd += ["-c", "copy",
            "-movflags", "frag_keyframe+empty_moov+default_base_moof",
            "-f", "mp4", "pipe:1"]

    def _feed(path, data):
        try:
            with open(path, "wb") as f:
                f.write(data)
        except (BrokenPipeError, OSError):
            pass

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    threading.Thread(target=_feed, args=(fv, video), daemon=True).start()
    if audio:
        threading.Thread(target=_feed, args=(fa, audio), daemon=True).start()
    try:
        mp4, err = proc.communicate(timeout=180)
    finally:
        for f in (fv, fa):
            try:
                os.unlink(f)
            except OSError:
                pass
        try:
            os.rmdir(tmpd)
        except OSError:
            pass
    if proc.returncode != 0 or not mp4:
        raise RuntimeError(f"ffmpeg mux failed rc={proc.returncode}: "
                           f"{err.decode(errors='replace')[-300:]}")
    return mp4


# ── hub upload ───────────────────────────────────────────────────────────────

def upload_event(hub_url: str, event_id: str, mp4: bytes, log_text: bytes,
                 timeout: int = 60) -> list[str]:
    """Ask the hub for upload links (generic descriptors) and execute them."""
    req = urllib.request.Request(
        f"{hub_url}/api/v1/media/uploads",
        data=json.dumps({
            "event_id": event_id,
            "files": [
                {"name": f"{event_id}.mp4", "size": len(mp4),
                 "content_type": "video/mp4"},
                {"name": f"{event_id}.jsonl", "size": len(log_text),
                 "content_type": "application/x-ndjson"},
            ],
        }).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        ups = json.load(r)["uploads"]

    stored = []
    for desc, blob in zip(ups, (mp4, log_text)):
        headers = dict(desc.get("headers") or {})
        rq = urllib.request.Request(desc["url"], data=blob, headers=headers,
                                    method=desc.get("method", "PUT"))
        with urllib.request.urlopen(rq, timeout=timeout) as r:
            body = json.load(r)
            stored.append(body.get("stored", desc["url"]))
    return stored


# ── event engine ─────────────────────────────────────────────────────────────

class EventEngine:
    """Motion-gated AI event recorder.  Thread-safe entry points:
       - append(kind, ts, flags, payload)  from the ingest thread
       - motion_tick(ts_ms, ratio, boxes, frame_rgb)  from the detector tick
    """

    def __init__(self, hub_url: str, detector=None,
                 keep_s: int = 60, max_s: int = 120,
                 motion_thresh: float = 0.02, quiet_s: float = 3.0,
                 yolo_interval_s: float = 1.5, min_score: float = 0.45,
                 cam_id: str | None = None):
        self.ring = AVRingBuffer(keep_s)
        self.hub_url = hub_url.rstrip("/")
        # Optional camera-id namespace for events.  When set, event ids become
        # "<cam>-ev-YYYYMMDD-HHMMSS" so the hub can attribute events to this
        # camera in a multi-camera deployment (the hub /devices webapp filters
        # by this prefix; unset → legacy "ev-..." attributed to the first cam).
        # (nnvideo passes it explicitly: one process hosts every camera)
        self.cam_id = (cam_id if cam_id is not None else os.environ.get("NN_CAM_ID", "")).strip()
        self.detector = detector               # None → motion-only events
        self.max_ms = max_s * 1000
        self.motion_thresh = motion_thresh     # T, adjustable at runtime
        self.quiet_s = quiet_s
        self.yolo_interval_s = yolo_interval_s
        self.min_score = min_score
        self._lock = threading.Lock()
        self._active: Optional[dict] = None    # event state
        self._uploading = False
        self._det_busy = threading.Event()     # a YOLO run is in flight
        self._det_result: Optional[list] = None
        self._det_ts = 0.0
        self._inflight = 0                      # mux+upload threads running
        self._inflight_lock = threading.Lock()
        self.last_dets = []                     # most recent YOLO detections (for UI)
        self.last_dets_ts = 0
        # False when an edge device (nn_infer) owns detection for this stream:
        # motion ticks then only do activity/quiet bookkeeping and edge_tick()
        # drives event start/keep-alive.  Set via VideoService.apply_infer_mode.
        self.local_infer = True
        # Classes that may START/extend an edge-triggered event.  A static
        # scene object (tv, chair) detected every frame must not spam
        # recordings; motion has no say in the edge path, so class-gate it.
        self.edge_alert_classes = {"person", "cat", "dog"}
        # Per-class capture policy (hub-configured).  While unset the legacy
        # behaviour above applies, so a camera whose policy hasn't arrived
        # yet still records.  set_policy() installs the real thing.
        self._policy = None          # {"classes": {name: ClassPolicy}}
        # set_policy() also owns this, but a motion tick can arrive before
        # the hub's policy does — reading it unset crashed the tick.
        self._motion_pol = None
        self._policy_version = 0
        # Inference rate cap (policy "fps"): at most one detector run per
        # this interval, however fast frames arrive.
        self._infer_gap = 1.0 / 5
        self._last_infer = 0.0
        self.frame_wh = (1280, 720)
        self.stats = {"events": 0, "uploads_ok": 0, "uploads_failed": 0,
                      "last_event": None, "yolo_runs": 0, "yolo_ms": 0}

    # ingest hook ------------------------------------------------------------
    def append(self, kind: str, ts: int, flags: int, payload: bytes) -> None:
        self.ring.append(kind, ts, flags, payload)
        with self._lock:
            ev = self._active
            if ev is not None:
                ev["records"].append((kind, ts, flags, payload))
                ev["bytes"] = ev.get("bytes", 0) + len(payload)
                if ts - ev["t0"] >= self.max_ms:
                    self._finalize_locked("max_length")
                elif ev["bytes"] > 96 * 1024 * 1024:   # runaway guard
                    self._finalize_locked("size_cap")

    # detector tick ----------------------------------------------------------
    def _motion_active(self, ratio: float) -> bool:
        """Instantaneous threshold when no motion policy is configured;
        aggregated hysteresis when one is.  Feeding EVERY tick (even 0) is
        what lets the aggregate decay and the stop threshold end an event —
        a single noisy tick can no longer re-arm the quiet timer."""
        if self._motion_pol is None:
            return ratio >= self.motion_thresh
        self._motion_pol.feed(min(1000, int(round(ratio * 1000))))
        return self._motion_pol.detected

    def motion_tick(self, ts_ms: int, ratio: float, boxes: list,
                    frame_rgb=None) -> None:
        now = time.time()
        motion_on = self._motion_active(ratio)
        with self._lock:
            ev = self._active
        detections = None

        if ev is None:
            if not self.local_infer:
                return                          # edge_tick() starts events
            if not motion_on:
                return
            if self._inflight >= 2:
                return                          # backpressure: uploads lagging
            # fire-and-collect: YOLO runs on a worker; the trigger tick that
            # finds a completed positive result starts the event (ticks must
            # never block — inference takes seconds on these cores)
            res = self._poll_detect(frame_rgb)
            if not res:
                return
            if self._policy is not None:
                firing = self._policy_capturing(res)
                res = [d for d in res if d.get("cls") in firing]
            else:
                # No policy yet (hub unreachable, or an old service): fall back
                # to the SAME conservative gate the edge path uses.  Recording
                # on any class the model happens to see is how a motionless
                # 'couch' at 0.499 started events.
                res = [d for d in res
                       if d.get("score", 0) >= self.min_score
                       and d.get("cls") in self.edge_alert_classes]
            if not res:
                return                  # nothing sustained/allowed enough
            self._start(ts_ms, ratio, boxes, res)
            return

        # active event: periodic re-detection, activity bookkeeping
        if self.local_infer and now - ev["last_yolo"] >= self.yolo_interval_s:
            res = self._poll_detect(frame_rgb)
            ev["last_yolo"] = now if self._det_busy.is_set() else ev["last_yolo"]
            if res is not None:
                detections = res
                if self._policy is not None:
                    firing = self._policy_capturing(res)
                    if firing:
                        ev["last_seen"] = now
                elif res:
                    ev["last_seen"] = now
        if motion_on:
            ev["last_motion"] = now
        ev["log"].append(json.dumps({
            "ts": ts_ms, "motion": round(ratio, 4), "boxes": boxes,
            "detections": detections}, separators=(",", ":")))
        if (now - max(ev["last_seen"], ev["last_motion"])) >= self.quiet_s:
            with self._lock:
                if self._active is ev:
                    self._finalize_locked("quiet")

    def set_policy(self, engine_doc: dict, version: int = 0) -> None:
        """Install the per-class policy for whichever engine feeds us.

        engine_doc: {"enabled": bool, "classes": {name: {capture, agg,
        start, stop}}} — the hub's doc for THIS engine (edge or service).
        Evaluators are rebuilt (a changed window has no valid history)."""
        from infer_policy import ClassPolicy
        classes = (engine_doc or {}).get("classes") or {}
        pol = {}
        for name, c in classes.items():
            if name == "motion":
                continue                      # pseudo-class, handled below
            if not c.get("capture", True):
                continue                      # tracked for display, never fires
            pol[name] = ClassPolicy(True, int(c.get("agg", 5) or 5),
                                    int(round(float(c.get("start", 0.6)) * 1000)),
                                    int(round(float(c.get("stop", 0.4)) * 1000)))
        # motion pseudo-class: gates the motion path itself (see motion_tick)
        self._motion_pol = None
        m = classes.get("motion")
        if m is not None and m.get("capture", True):
            self._motion_pol = ClassPolicy(
                True, int(m.get("agg", 5) or 5),
                int(round(float(m.get("start", 0.02)) * 1000)),
                int(round(float(m.get("stop", 0.02)) * 1000)))
        self._policy = pol
        self._policy_version = int(version or 0)
        try:
            fps = max(1, min(15, int((engine_doc or {}).get("fps", 5) or 5)))
        except (TypeError, ValueError):
            fps = 5
        self._infer_gap = 1.0 / fps
        log.info("inference policy v%s: %s @ max %d fps", self._policy_version,
                 ", ".join(sorted(pol)) or "(no capturing classes)", fps)

    def _policy_capturing(self, dets: list) -> list:
        """Feed one inference result through the per-class evaluators and
        return the classes currently in DETECT state.

        Every configured class is fed EVERY frame (absent = confidence 0) —
        that is what makes the aggregate decay and the stop threshold fire."""
        best = {}
        for d in dets or []:
            c = d.get("cls")
            v = int(round(float(d.get("score", 0)) * 1000))
            if c is not None and v > best.get(c, -1):
                best[c] = v
        firing = []
        for name, p in self._policy.items():
            _, changed = p.feed(best.get(name, 0))
            if p.detected:
                firing.append(name)
            if changed:
                log.info("class %s -> %s", name,
                         "DETECT" if p.detected else "clear")
        return firing

    def edge_tick(self, ts_ms: int, dets: list) -> None:
        """Detections from the DEVICE ('D' records, nn_infer).  Mirrors the
        local-YOLO bookkeeping: updates the UI store always; starts/extends
        events only while local inference is off (service override wins)."""
        now = time.time()
        self.last_dets = dets
        self.last_dets_ts = int(now * 1000)
        self.stats["yolo_src"] = "edge"
        if self.local_infer:
            return
        if self._policy is not None:
            firing = self._policy_capturing(dets)
            strong = [d for d in dets if d.get("cls") in firing]
        else:
            strong = [d for d in dets
                      if d.get("score", 0) >= self.min_score
                      and d.get("cls") in self.edge_alert_classes]
        with self._lock:
            ev = self._active
        if ev is None:
            if strong and self._inflight < 2:
                self._start(ts_ms, 0.0, [d["box"] for d in strong
                                         if "box" in d], strong)
            return
        if strong:
            ev["last_seen"] = now
        ev["log"].append(json.dumps({
            "ts": ts_ms, "edge": True, "detections": dets},
            separators=(",", ":")))
        if (now - max(ev["last_seen"], ev["last_motion"])) >= self.quiet_s:
            with self._lock:
                if self._active is ev:
                    self._finalize_locked("quiet")

    # internals ---------------------------------------------------------------
    def _poll_detect(self, frame_rgb):
        """Non-blocking: collect the last finished YOLO result (if fresh) and
        kick off a new run when the worker is idle.  Returns a completed
        result list, or None when nothing has finished since last poll."""
        out, self._det_result = self._det_result, None
        now = time.time()
        release = getattr(frame_rgb, "release", None)
        if now - self._last_infer < self._infer_gap:
            if release: release()           # not used: give the buffer back now
            return out                      # rate cap: too soon since the last run
        if frame_rgb is not None and not self._det_busy.is_set():
            self._last_infer = now
            self._det_busy.set()
            def _run(f=frame_rgb):
                try:
                    self._det_result = self._detect(f)
                    self._det_ts = time.time()
                finally:
                    self._det_busy.clear()
                    rel = getattr(f, "release", None)
                    if rel: rel()
            threading.Thread(target=_run, daemon=True, name=f"yolo:{self.cam_id}").start()
        elif release:
            release()
        return out

    def _detect(self, frame_rgb):
        if self.detector is None or frame_rgb is None:
            return [{"cls": "motion", "score": 1.0}] if self.detector is None else []
        t = time.time()
        try:
            self.frame_wh = (frame_rgb.shape[1], frame_rgb.shape[0])  # (w,h) box coord space
        except Exception:
            pass
        try:
            if hasattr(frame_rgb, "write_letterbox") and hasattr(self.detector, "infer_frame"):
                dets = self.detector.infer_frame(frame_rgb, self.min_score)   # straight into the pool
            elif hasattr(frame_rgb, "rgb"):
                dets = self.detector(frame_rgb.rgb(), self.min_score)         # local detectors want RGB
            else:
                dets = self.detector(frame_rgb, self.min_score)
        except Exception as e:
            log.warning("detector failed: %s", e)
            return []
        self.stats["yolo_runs"] += 1
        self.stats["yolo_ms"] = int((time.time() - t) * 1000)
        self.last_dets = dets
        self.last_dets_ts = int(time.time() * 1000)
        return dets

    def _start(self, ts_ms, ratio, boxes, detections):
        eid = time.strftime("ev-%Y%m%d-%H%M%S")
        if self.cam_id:
            eid = f"{self.cam_id}-{eid}"
        pre = self.ring.snapshot_from(ts_ms)
        now = time.time()
        with self._lock:
            self._active = {
                "id": eid, "t0": pre[0][1] if pre else ts_ms,
                "records": pre, "log": [json.dumps({
                    "ts": ts_ms, "start": True, "motion": round(ratio, 4),
                    "boxes": boxes, "detections": detections},
                    separators=(",", ":"))],
                "last_seen": now, "last_motion": now, "last_yolo": now,
            }
        log.info("EVENT %s started (motion=%.3f, %d dets, preroll %d recs)",
                 eid, ratio, len(detections), len(pre))

    def _finalize_locked(self, reason: str) -> None:
        ev = self._active
        self._active = None
        if ev is None:
            return
        ev["log"].append(json.dumps({"end": True, "reason": reason},
                                    separators=(",", ":")))
        self.stats["events"] += 1
        self.stats["last_event"] = ev["id"]
        threading.Thread(target=self._mux_upload, args=(ev, reason),
                         daemon=True, name="ev-upload").start()

    def _mux_upload(self, ev: dict, reason: str) -> None:
        eid = ev["id"]
        with self._inflight_lock:
            self._inflight += 1
        try:
            mp4 = mux_mp4(ev["records"])
            logb = ("\n".join(ev["log"]) + "\n").encode()
            log.info("EVENT %s finalized (%s): mp4=%dB log=%dB — uploading",
                     eid, reason, len(mp4), len(logb))
            stored = upload_event(self.hub_url, eid, mp4, logb)
            self.stats["uploads_ok"] += 1
            log.info("EVENT %s uploaded: %s", eid, stored)
        except Exception as e:
            self.stats["uploads_failed"] += 1
            log.error("EVENT %s upload failed: %s", eid, e)
        finally:
            ev["records"] = None; ev["log"] = None   # release memory
            with self._inflight_lock:
                self._inflight -= 1

    def force_event(self, duration_s: float = 6.0) -> str:
        """Test hook: start an event now (as if motion+detection fired) and
        auto-finalize after duration_s.  Exercises ring→mux→upload E2E."""
        ts = int(time.time() * 1000)
        self._start(ts, 1.0, [], [{"cls": "test", "score": 1.0}])
        def _end():
            time.sleep(duration_s)
            with self._lock:
                if self._active is not None:
                    self._finalize_locked("test")
        threading.Thread(target=_end, daemon=True).start()
        with self._lock:
            return self._active["id"] if self._active else "?"

    # runtime config ----------------------------------------------------------
    def status(self) -> dict:
        with self._lock:
            ev = self._active
            return dict(self.stats,
                        active=(ev and {"id": ev["id"],
                                        "ms": ev["records"][-1][1] - ev["t0"]
                                        if ev["records"] else 0}) or None,
                        ring_bytes=self.ring.bytes,
                        motion_thresh=self.motion_thresh,
                        detector=bool(self.detector))
