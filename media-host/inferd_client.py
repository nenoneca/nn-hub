"""Client for nn-inferd, shaped like the in-process detectors.

Drop-in for YoloxDetector/YoloxNpuDetector in event_engine: same
`det(rgb, min_score) -> [{cls, score, box}]` call, so nothing above it
changes.  The frame goes through a shared-memory pool (fd passed once via
SCM_RIGHTS); each request is ~25 bytes.

Falls back by raising on connect, so callers can keep their local detector
path when the daemon is absent.
"""
from __future__ import annotations

import mmap
import os
import socket
import struct
import sys
import threading

sys.path.insert(0, os.environ.get("NN_ACCEL_GEN", "/opt/nn-accel/gen"))
import nn_accel_pb2 as pb          # noqa: E402
import numpy as np                 # noqa: E402

MAGIC = 0x4E4E5031
HDR_BYTES = 800
FREE, OWNED, BUSY = 0, 1, 2

COCO = None                        # filled lazily from yolox_detector


class InferdDetector:
    def __init__(self, sock_path: str = None, size: int = 640, slots: int = 4,
                 owner: str = None, timeout: float = 5.0):
        self.size = size
        self.owner = owner or os.environ.get("NN_CAM_ID", "camera")
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.sock.settimeout(timeout)
        self.sock.connect(sock_path or os.environ.get("NN_INFERD_SOCK",
                                                      "/run/nn-inferd.sock"))
        self._lock = threading.Lock()       # one in-flight request per socket
        self._seq = 0

        self.sock.send(pb.Msg(id=1, hello=pb.HelloReq(
            client=self.owner, version="0.1")).SerializeToString())
        caps = pb.Msg(); caps.ParseFromString(self.sock.recv(65536))
        self.devices = [(d.id, d.parallelism) for d in caps.caps.devices]

        self._make_pool(slots)

    def _make_pool(self, slots: int):
        pg = os.sysconf("SC_PAGESIZE")
        slot_b = ((self.size * self.size * 3) + pg - 1) // pg * pg
        hdr_len = (HDR_BYTES + pg - 1) // pg * pg
        self.fd = os.memfd_create("nn_pool", 0)
        os.ftruncate(self.fd, hdr_len + slot_b * slots)
        self.map = mmap.mmap(self.fd, hdr_len + slot_b * slots, mmap.MAP_SHARED,
                             mmap.PROT_READ | mmap.PROT_WRITE)
        struct.pack_into("<8I", self.map, 0, MAGIC, slots, slot_b, hdr_len,
                         self.size, self.size, self.size * 3, 1)
        self.slots, self.slot_b, self.hdr_len = slots, slot_b, hdr_len
        self._padded: dict = {}              # slot -> (w, h) whose letterbox border is already written

        spec = pb.Msg(id=2, open_pool=pb.PoolSpec(
            owner=self.owner, slots=slots, slot_bytes=slot_b,
            format=pb.PIX_RGB888, width=self.size, height=self.size,
            stride=self.size * 3))
        socket.send_fds(self.sock, [spec.SerializeToString()], [self.fd])
        r = pb.Msg(); r.ParseFromString(self.sock.recv(65536))
        self.pool_id = r.pool_id.id

    def _acquire(self) -> int | None:
        for i in range(self.slots):
            if struct.unpack_from("<I", self.map, 32 + 4 * i)[0] == FREE:
                struct.pack_into("<I", self.map, 32 + 4 * i, OWNED)
                return i
        return None                          # all in flight: caller drops

    def infer_frame(self, frame, min_score: float = 0.45, deadline_ms: int = 1000) -> list[dict]:
        """Zero-copy submit: the frame scales and converts itself directly
        into the pool slot the daemon reads (pad written once per slot)."""
        global COCO
        if COCO is None:
            from yolox_detector import COCO as _c
            COCO = _c
        with self._lock:
            slot = self._acquire()
            if slot is None:
                frame.release()
                return []
            off = self.hdr_len + slot * self.slot_b
            dst = np.frombuffer(self.map, np.uint8, count=self.size * self.size * 3,
                                offset=off).reshape(self.size, self.size, 3)
            padded = self._padded.get(slot)
            s = frame.write_letterbox(dst, self.size, pad=(padded != (frame.w, frame.h)))
            self._padded[slot] = (frame.w, frame.h)
            frame.release()
            return self._request(slot, s, min_score, deadline_ms)

    def __call__(self, rgb: np.ndarray, min_score: float = 0.45,
                 deadline_ms: int = 1000) -> list[dict]:
        global COCO
        if COCO is None:
            # yolox_detector exposes the table as COCO (a tuple) — importing
            # the wrong name only failed at the FIRST inference, i.e. in
            # production rather than at startup.
            from yolox_detector import COCO as _c
            COCO = _c
        # letterbox to the model input, exactly like the local detectors
        h, w = rgb.shape[:2]
        s = min(self.size / w, self.size / h)
        nw, nh = int(w * s), int(h * s)
        canvas = np.full((self.size, self.size, 3), 114, np.uint8)
        if (nw, nh) != (w, h):
            from PIL import Image
            small = np.asarray(Image.fromarray(rgb).resize((nw, nh)))
        else:
            small = rgb
        canvas[:nh, :nw] = small

        with self._lock:
            slot = self._acquire()
            if slot is None:
                return []                    # backpressure, not an error
            off = self.hdr_len + slot * self.slot_b
            self.map[off:off + canvas.nbytes] = canvas.tobytes()
            self._padded[slot] = None        # the canvas rewrote the whole slot
            return self._request(slot, s, min_score, deadline_ms)

    def _request(self, slot: int, s: float, min_score: float, deadline_ms: int) -> list[dict]:
        if True:
            off = self.hdr_len + slot * self.slot_b
            self._seq += 1
            struct.pack_into("<Q", self.map, 288 + 8 * slot, self._seq)
            struct.pack_into("<I", self.map, 32 + 4 * slot, BUSY)
            import time as _t
            req = pb.Msg(id=self._seq & 0x7FFFFFFF, infer=pb.InferReq(
                pool=self.pool_id, slot=slot, seq=self._seq,
                ts_ms=int(_t.time() * 1000), deadline_ms=deadline_ms))
            self.sock.send(req.SerializeToString())
            data = self.sock.recv(65536)
            if not data:
                # daemon went away: an empty recv parses as an empty Msg,
                # which would look like "no detections" forever
                raise ConnectionError("nn-inferd closed the connection")
            resp = pb.Msg(); resp.ParseFromString(data)

        rr = resp.infer_resp
        if rr.dropped:
            return []
        out = []
        for d in rr.dets:
            score = d.conf_x1000 / 1000.0
            if score < min_score:
                continue
            # undo the letterbox so boxes are in the caller's frame
            out.append({"cls": COCO[d.class_id] if d.class_id < len(COCO)
                        else str(d.class_id),
                        "score": round(score, 3),
                        "class_id": int(d.class_id),
                        "box": [int(d.x / s), int(d.y / s),
                                int(d.w / s), int(d.h / s)]})
        return out

    def close(self):
        try:
            self.map.close()
        except Exception:
            pass
        for f in (self.fd,):
            try:
                os.close(f)
            except OSError:
                pass
        try:
            self.sock.close()
        except OSError:
            pass


class ReconnectingInferd:
    """InferdDetector that survives a daemon restart.

    Construction still raises if the daemon is absent — that keeps the
    service's detector-preference chain honest (it falls back to a private
    model at startup).  But once running, a daemon restart only costs the
    frames seen while it was down: each __call__ returns [] and a rebuild
    is attempted at most every retry_s seconds."""

    def __init__(self, owner: str = None, retry_s: float = 5.0):
        self._kw = {"owner": owner}
        self.retry_s = retry_s
        self._d = InferdDetector(**self._kw)
        self.devices = self._d.devices
        self._down_since = None
        self._next_try = 0.0

    def infer_frame(self, frame, min_score: float = 0.45, deadline_ms: int = 1000) -> list:
        return self.__call__(frame, min_score, deadline_ms, method="infer_frame")

    def __call__(self, rgb, min_score: float = 0.45,
                 deadline_ms: int = 1000, method: str = "__call__") -> list:
        import time as _t
        if self._d is None:
            now = _t.monotonic()
            if now < self._next_try:
                return []
            self._next_try = now + self.retry_s
            try:
                self._d = InferdDetector(**self._kw)
                self.devices = self._d.devices
                print(f">> nn-inferd reconnected after "
                      f"{now - self._down_since:.0f}s", flush=True)
                self._down_since = None
            except OSError:
                return []
        try:
            return getattr(self._d, method)(rgb, min_score, deadline_ms)
        except (OSError, ConnectionError, struct.error) as e:
            print(f">> nn-inferd connection lost ({e}); will retry every "
                  f"{self.retry_s:.0f}s", flush=True)
            try:
                self._d.close()
            except Exception:
                pass
            self._d = None
            self._down_since = _t.monotonic()
            self._next_try = _t.monotonic() + self.retry_s
            return []

    def close(self):
        if self._d is not None:
            self._d.close()
