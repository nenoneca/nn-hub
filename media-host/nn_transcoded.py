#!/usr/bin/env python3
"""nn-transcoded — shared hardware-transcode service.

Owns the WAVE5 / CIX video codec sessions so the camera pipelines don't.

ZERO COPY.  A camera writes each compressed access unit ONCE into a shared
pool slot and sends a 14-byte datagram naming it; this service reads those
bytes IN PLACE and wraps them into a GStreamer buffer with
Gst.Buffer.new_wrapped_full, which takes ownership of the mapping rather
than duplicating it.  The slot is released back to the producer only when
GStreamer drops its last reference, so the ring doubles as flow control.
Nothing but the notification crosses the socket.

Raw frames are better off still: decode and encode live in ONE process, so
the 93 MB/s of NV12 never crosses a process boundary at all.

NOTE on testing: the CIX hardware decoder REFUSES a synthetic stream from
videotestsrc ! v4l2h264enc (its own encoder), failing negotiation the same
way whether the source is appsrc or filesrc, while avdec_h264 decodes that
same file fine.  Real camera streams decode without trouble.  Test this
service with captured camera data, never with videotestsrc, or you will
chase a decoder bug that does not exist in production.

What it buys: a wedged codec is recovered by restarting one session (or this
service) instead of restarting a camera, and the recovery is automatic —
a session that stops producing bytes is rebuilt.

See nn-media-stream/docs/ACCEL_SERVICES_DESIGN.md.
"""
from __future__ import annotations

import argparse
import ctypes
import json
import mmap
import os
import socket
import struct
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.environ.get("NN_ACCEL_GEN", "/opt/nn-accel/gen"))

import nn_accel_pb2 as pb                     # noqa: E402

import gi                                     # noqa: E402
gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib           # noqa: E402

MAGIC = 0x4E4E5031
SLOTS_MAX = 64
FREE, OWNED, BUSY = 0, 1, 2
_STATE_OFF = 32
_SEQ_OFF = _STATE_OFF + 4 * SLOTS_MAX
_HDR_BYTES = _SEQ_OFF + 8 * SLOTS_MAX
AU_HDR = struct.Struct("<IIQ")                # bytes, flags, ts_ms

# PyGObject's Gst.Buffer.new_wrapped_full / Gst.Memory.new_wrapped cannot wrap
# BORROWED memory — both return NULL from Python (verified on this platform),
# and Gst.Buffer.new_wrapped() copies.  A copy per access unit is exactly what
# this service exists to avoid, so the wrap and the push are made through the
# C symbols with the pool's raw address.
_libc = ctypes.CDLL("libc.so.6", use_errno=True)
_libc.mmap.restype = ctypes.c_void_p
_libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                       ctypes.c_int, ctypes.c_int, ctypes.c_long]
_libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
_PROT_READ, _PROT_WRITE, _MAP_SHARED = 1, 2, 1

_gst = ctypes.CDLL("libgstreamer-1.0.so.0")
_gst.gst_buffer_new_wrapped_full.restype = ctypes.c_void_p
_gst.gst_buffer_new_wrapped_full.argtypes = [
    ctypes.c_int, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_size_t,
    ctypes.c_size_t, ctypes.c_void_p, ctypes.c_void_p]
_gstapp = ctypes.CDLL("libgstapp-1.0.so.0")
_gstapp.gst_app_src_push_buffer.restype = ctypes.c_int
_gstapp.gst_app_src_push_buffer.argtypes = [ctypes.c_void_p, ctypes.c_void_p]

_NOTIFY = ctypes.CFUNCTYPE(None, ctypes.c_void_p)

# Borrowed pool memory must be READONLY: elements are allowed to modify a
# writable buffer in place, and h264parse does exactly that when it rewrites
# start codes — corrupting the producer's ring behind its back.
_GST_MEMORY_FLAG_READONLY = 2          # GST_MINI_OBJECT_FLAG_LOCK_READONLY
_COPY_MODE = os.environ.get("NN_TC_COPY") == "1"


def _gobject_ptr(obj) -> int:
    """C pointer behind a PyGObject wrapper."""
    p = obj.__gpointer__
    if isinstance(p, int):
        return p
    ctypes.pythonapi.PyCapsule_GetPointer.restype = ctypes.c_void_p
    ctypes.pythonapi.PyCapsule_GetPointer.argtypes = [ctypes.py_object,
                                                      ctypes.c_char_p]
    return ctypes.pythonapi.PyCapsule_GetPointer(p, None)


class AuPool:
    """The producer's pool, mapped here.  Read in place — never copied."""

    def __init__(self, fd: int):
        head = mmap.mmap(fd, _HDR_BYTES, mmap.MAP_SHARED, mmap.PROT_READ)
        magic, slots, slot_b, data_off = struct.unpack_from("<4I", head, 0)
        head.close()
        if magic != MAGIC:
            raise ValueError(f"bad pool magic {magic:#x}")
        self.slots, self.slot_b, self.data_off = slots, slot_b, data_off
        self.size = data_off + slot_b * slots
        # Mapped WRITABLE through libc: releasing a slot is a store into the
        # shared header (how the producer learns the ring advanced), and the
        # raw address is what lets GStreamer borrow the bytes instead of
        # copying them.
        self.addr = _libc.mmap(None, self.size, _PROT_READ | _PROT_WRITE,
                               _MAP_SHARED, fd, 0)
        if not self.addr or self.addr == ctypes.c_void_p(-1).value:
            raise OSError(ctypes.get_errno(), "mmap of pool failed")
        self.buf = (ctypes.c_ubyte * self.size).from_address(self.addr)
        self.fd = fd

    def au_info(self, slot: int):
        """(address of payload, length, ts_ms, keyframe) — nothing read."""
        off = self.data_off + slot * self.slot_b
        n, flags, ts_ms = AU_HDR.unpack_from(self.buf, off)
        return (self.addr + off + AU_HDR.size, n, ts_ms, bool(flags & 1))

    def release(self, slot: int):
        struct.pack_into("<I", self.buf, _STATE_OFF + 4 * slot, FREE)

    def close(self):
        try:
            _libc.munmap(ctypes.c_void_p(self.addr), self.size)
        except Exception:
            pass
        try:
            os.close(self.fd)
        except Exception:
            pass


class Session:
    """One camera's transcode: shm(in) -> dec -> enc -> [ts] -> tcp(out)."""

    def __init__(self, spec: pb.TranscodeOpen, out_port: int, device_id: str,
                 pool: "AuPool" = None):
        self.owner = spec.owner
        self.pool = pool
        self.ready = False
        self.src = None
        self.src_ptr = None
        # The C side keeps this pointer: if Python garbage-collects the
        # thunk, the destroy notify jumps into freed memory.
        self._notify_cb = _NOTIFY(self._on_buffer_freed)
        self.in_flight = 0
        self.pushed = 0
        self.stage = {}
        self.last_pts_ms = -1
        self.in_port = spec.in_port
        self.out_port = out_port
        self.bitrate = spec.bitrate or 6_000_000
        self.gop = spec.gop or 30
        self.qp = spec.qp
        self.mux_ts = spec.mux_ts
        self.device_id = device_id
        self.pipeline = None
        self.error = ""
        self.restarts = 0
        self._bytes = 0
        self._last_bytes = 0
        self._stall_ticks = 0

    # -- construction ------------------------------------------------------
    def _desc(self) -> str:
        # extra-controls is set PROGRAMMATICALLY after parse_launch: quoting a
        # nested controls structure inside a launch string silently loses it,
        # and an encoder that ignores the QP runs ~10x the bitrate and falls
        # behind realtime (measured: 264 KB segments every 21 s instead of
        # 25 KB every 3 s).
        enc = "v4l2h264enc name=enc"
        # No videoconvert: both ends are V4L2 and negotiate dmabuf directly.
        # videoconvert CPU-maps every frame out of UNCACHED DMABuf memory,
        # measured at ~1/10 realtime at 1080p — the original cause of "HLS
        # buffers forever".
        tail = ("h264parse config-interval=-1 ! mpegtsmux ! "
                if self.mux_ts else "h264parse config-interval=-1 ! ")
        return (
            # NO alignment field: a camera feeds FRAGMENTS of access units,
            # not whole ones.  Declaring alignment=au tells h264parse the
            # input is already frame-aligned, so it mis-parses and the
            # decoder throws almost everything away (measured: 100 parsed,
            # 1 decoded).  video_service's own appsrc omits it for exactly
            # this reason.
            f"appsrc name=src is-live=true do-timestamp=true format=time "
            f"caps=video/x-h264,stream-format=byte-stream ! "
            f"queue max-size-buffers=0 max-size-bytes=0 max-size-time=3000000000 ! "
            f"h264parse config-interval=-1 ! identity name=parsed ! "
            f"v4l2h264dec ! video/x-raw,format=NV12 ! identity name=decoded ! "
            f"{enc} ! "
            f"video/x-h264,profile=constrained-baseline ! {tail}"
            f"identity name=meter ! "
            # Bound what the sink will hold for a consumer.  With the
            # defaults (units-max = -1) a single slow moment builds a
            # backlog that NEVER drains: cam1's HLS ran ~25 s behind live
            # with 4.6 s segments while the in-process cameras held ~1 s
            # (measured 2026-08-17).  Cap the queue at 2 s of media and
            # recover to a keyframe when a consumer falls behind — a
            # viewer would rather skip forward than watch the past.
            f"tcpserversink host=127.0.0.1 port={self.out_port} sync=false "
            f"unit-format=time units-max=2000000000 units-soft-max=1000000000 "
            f"recover-policy=keyframe sync-method=latest-keyframe")

    def start(self) -> bool:
        try:
            self.pipeline = Gst.parse_launch(self._desc())
        except GLib.Error as e:
            self.error = f"parse: {e}"
            return False
        self.src = self.pipeline.get_by_name("src")
        self.src_ptr = ctypes.c_void_p(_gobject_ptr(self.src))
        encoder = self.pipeline.get_by_name("enc")
        if encoder is not None:
            ec = Gst.Structure.new_empty("controls")
            ec.set_value("video_bitrate", int(self.bitrate))
            ec.set_value("h264_i_frame_period", int(self.gop))
            ec.set_value("video_gop_size", int(self.gop))
            ec.set_value("frame_level_rate_control_enable", 1)
            if self.qp:
                # CIX P1 ignores video_bitrate entirely; fixed QP is the only
                # knob that changes the output there.
                for k in ("h264_i_frame_qp_value", "h264_p_frame_qp_value",
                          "h264_b_frame_qp_value"):
                    ec.set_value(k, int(self.qp))
            encoder.set_property("extra-controls", ec)
        meter = self.pipeline.get_by_name("meter")
        meter.connect("handoff", self._on_buffer)
        meter.set_property("signal-handoffs", True)
        # Per-stage counters: when output is thin, these say whether frames
        # were lost before the decoder, inside it, or never encoded — the
        # question offline tests cannot answer about a live camera.
        for nm in ("parsed", "decoded"):
            el = self.pipeline.get_by_name(nm)
            if el is not None:
                el.set_property("signal-handoffs", True)
                el.connect("handoff", self._count(nm))
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message::error", self._on_error)
        if self.pipeline.set_state(Gst.State.PLAYING) == Gst.StateChangeReturn.FAILURE:
            self.error = "failed to start (codec busy?)"
            return False
        # Wait for the transition before letting anything in.  appsrc is fed
        # from the socket thread, and a caps event that reaches v4l2h264dec
        # before it has opened its device is refused as not-negotiated —
        # which kills the stream on the very first access unit.
        rc, _st, _pend = self.pipeline.get_state(5 * Gst.SECOND)
        if rc == Gst.StateChangeReturn.FAILURE:
            self.error = "pipeline failed to reach PLAYING"
            return False
        self.ready = True
        self.error = ""
        return True

    def stop(self):
        self.ready = False
        if self.pipeline is not None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None
        self.src = self.src_ptr = None

    # -- ingest ------------------------------------------------------------
    def push_au(self, slot: int, ts_ms: int):
        """Hand a pool slot to GStreamer WITHOUT copying its bytes.

        The buffer borrows the pool memory; its destroy notify fires when
        GStreamer drops the last reference, which is exactly when the
        producer may reuse that slot.  The ring is therefore also the flow
        control: a stalled codec stops freeing slots and the camera drops
        HLS frames rather than stalling its live uplink."""
        if not self.ready or self.src_ptr is None or self.pool is None:
            self.pool.release(slot)          # keep the ring moving
            return
        addr, n, _ts, _kf = self.pool.au_info(slot)
        if n == 0:
            self.pool.release(slot)
            return
        if _COPY_MODE:
            # Diagnostic path (NN_TC_COPY=1): copy out and free the slot at
            # once.  Only for proving whether borrowed memory is at fault.
            payload = bytes((ctypes.c_ubyte * n).from_address(addr))
            self.pool.release(slot)
            gbuf = Gst.Buffer.new_wrapped(payload)
            if self.src.emit("push-buffer", gbuf) != Gst.FlowReturn.OK:
                self.error = "appsrc rejected a buffer"
            self.pushed += 1
            return
        self.in_flight += 1
        buf = _gst.gst_buffer_new_wrapped_full(
            _GST_MEMORY_FLAG_READONLY, ctypes.c_void_p(addr), n, 0, n,
            ctypes.cast(ctypes.c_void_p(slot), ctypes.c_void_p),
            self._notify_cb)
        if not buf:
            self.in_flight -= 1
            self.pool.release(slot)
            self.error = "gst_buffer_new_wrapped_full failed"
            return
        # push_buffer takes ownership, so there is nothing to unref here
        if _gstapp.gst_app_src_push_buffer(self.src_ptr, ctypes.c_void_p(buf)) != 0:
            self.error = "appsrc rejected a buffer"
        self.pushed += 1

    def _on_buffer_freed(self, user_data):
        """GStreamer is done with the slot — hand it back to the camera."""
        slot = int(user_data or 0)
        self.in_flight -= 1
        if self.pool is not None:
            self.pool.release(slot)

    # -- health ------------------------------------------------------------
    def _count(self, name):
        def h(_el, buf):
            self.stage[name] = self.stage.get(name, 0) + 1
            if name == "decoded":
                self.last_pts_ms = (buf.pts // 1_000_000) if buf.pts != Gst.CLOCK_TIME_NONE else -1
        return h

    def _on_buffer(self, _el, buf):
        self._bytes += buf.get_size()
        self.stage["encoded"] = self.stage.get("encoded", 0) + 1
        if not (buf.get_flags() & Gst.BufferFlags.DELTA_UNIT):
            self.stage["keyframes"] = self.stage.get("keyframes", 0) + 1

    def _on_error(self, _bus, msg):
        err, _dbg = msg.parse_error()
        self.error = str(err)
        print(f"!! {self.owner}: {err}", file=sys.stderr, flush=True)

    def check(self):
        """Rebuild a session that has stopped producing.

        A wedged hardware codec does not always post an ERROR — it simply
        stops.  Silence for three ticks with a live input is treated as a
        wedge, which is exactly the failure that used to require restarting
        a camera (or the board)."""
        moved = self._bytes != self._last_bytes
        self._last_bytes = self._bytes
        if moved:
            self._stall_ticks = 0
            return
        self._stall_ticks += 1
        if self._stall_ticks < 3:
            return
        self._stall_ticks = 0
        self.restarts += 1
        print(f">> {self.owner}: no output for 3 ticks — rebuilding session "
              f"(restart #{self.restarts})", file=sys.stderr, flush=True)
        self.stop()
        self.start()

    def to_pb(self) -> pb.TranscodeSession:
        return pb.TranscodeSession(
            owner=self.owner, out_port=self.out_port,
            running=self.pipeline is not None and not self.error,
            device_id=self.device_id, bytes_out=self._bytes,
            restarts=self.restarts, error=self.error)


class TranscodeService:
    def __init__(self, sock_path: str, base_port: int, device_id: str):
        self.sock_path = sock_path
        self.base_port = base_port
        self.device_id = device_id
        self.sessions: dict[str, Session] = {}
        self._lock = threading.Lock()

    def _next_port(self) -> int:
        used = {s.out_port for s in self.sessions.values()}
        p = self.base_port
        while p in used:
            p += 1
        return p

    def open(self, spec: pb.TranscodeOpen,
             pool: "AuPool" = None) -> pb.TranscodeSession:
        with self._lock:
            old = self.sessions.get(spec.owner)
            if old is not None:
                # Idempotent: a camera restart re-opens with the same params
                # and must get the SAME port back, or its HLS consumer would
                # be left reading a dead one.
                if (old.mux_ts == spec.mux_ts and old.pipeline is not None
                        and pool is None):
                    return old.to_pb()
                old.stop()
                if old.pool is not None:
                    old.pool.close()
                del self.sessions[spec.owner]
            s = Session(spec, self._next_port(), self.device_id, pool)
            if not s.start():
                if pool is not None:
                    pool.close()
                return pb.TranscodeSession(owner=spec.owner, running=False,
                                           error=s.error or "start failed")
            self.sessions[spec.owner] = s
            print(f">> {s.owner}: shm({s.pool.slots} slots) -> tcp:{s.out_port}"
                  f"{' (ts)' if s.mux_ts else ''} bitrate={s.bitrate} "
                  f"gop={s.gop} qp={s.qp or '-'}", flush=True)
            return s.to_pb()

    def close(self, owner: str) -> pb.TranscodeSession:
        with self._lock:
            s = self.sessions.pop(owner, None)
            if s is None:
                return pb.TranscodeSession(owner=owner, running=False)
            s.stop()
            if s.pool is not None:
                s.pool.close()
            print(f">> {owner}: closed", flush=True)
            return s.to_pb()

    # -- serving -----------------------------------------------------------
    def serve(self):
        if os.path.exists(self.sock_path):
            os.unlink(self.sock_path)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        srv.bind(self.sock_path)
        os.chmod(self.sock_path, 0o660)
        srv.listen(16)
        print(f">> nn-transcoded listening on {self.sock_path}", flush=True)
        while True:
            conn, _ = srv.accept()
            threading.Thread(target=self._client, args=(conn,),
                             daemon=True).start()

    def _client(self, conn):
        owner = None
        try:
            while True:
                data, fds, _flags, _addr = socket.recv_fds(conn, 65536, 1)
                if not data:
                    return
                msg = pb.Msg()
                msg.ParseFromString(data)
                which = msg.WhichOneof("body")

                if which == "tc_au":
                    # Hot path: no reply, no copy — just hand the slot over.
                    s = self.sessions.get(owner)
                    if s is not None:
                        s.push_au(msg.tc_au.slot, msg.tc_au.ts_ms)
                    continue

                out = pb.Msg(id=msg.id)
                if which == "hello":
                    out.caps.service_version = "nn-transcoded/0.1"
                    owner = msg.hello.client or owner
                elif which == "tc_open":
                    pool = None
                    if fds:
                        try:
                            pool = AuPool(fds[0])
                        except (OSError, ValueError) as e:
                            for fd in fds:
                                os.close(fd)
                            out.error.code = 400
                            out.error.message = f"bad pool: {e}"
                            conn.send(out.SerializeToString())
                            continue
                    if pool is None:
                        out.error.code = 400
                        out.error.message = "tc_open needs a pool fd"
                        conn.send(out.SerializeToString())
                        continue
                    owner = msg.tc_open.owner or owner
                    out.tc_sess.CopyFrom(self.open(msg.tc_open, pool))
                elif which == "tc_close":
                    out.tc_sess.CopyFrom(self.close(msg.tc_close.owner))
                    owner = None
                else:
                    out.error.code = 400
                    out.error.message = f"unsupported: {which}"
                conn.send(out.SerializeToString())
        except OSError:
            pass
        finally:
            # A dropped connection means the camera is gone: its pool fd is
            # about to become the only reference to that memory.
            if owner:
                self.close(owner)
            conn.close()


def _watchdog(svc: TranscodeService, stats_path: str):
    while True:
        time.sleep(10)
        with svc._lock:
            for s in list(svc.sessions.values()):
                s.check()
            doc = {"ts": int(time.time()), "device": svc.device_id,
                   "sessions": {s.owner: {"pushed": s.pushed,
                                          "stage": s.stage,
                                          "last_pts_ms": s.last_pts_ms,
                                          "in_flight": s.in_flight,
                                          "out_port": s.out_port,
                                          "bytes_out": s._bytes,
                                          "restarts": s.restarts,
                                          "error": s.error}
                                for s in svc.sessions.values()}}
        try:
            tmp = stats_path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(doc, f)
            os.replace(tmp, stats_path)
            os.chmod(stats_path, 0o644)
        except OSError:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--socket", default="/run/nn-transcoded/sock")
    ap.add_argument("--base-port", type=int, default=5700,
                    help="first loopback port handed to a session")
    ap.add_argument("--device-id", default="wave5-0")
    args = ap.parse_args()

    Gst.init(None)
    svc = TranscodeService(args.socket, args.base_port, args.device_id)
    stats = os.path.join(os.path.dirname(args.socket), "stats.json")
    threading.Thread(target=_watchdog, args=(svc, stats), daemon=True).start()
    threading.Thread(target=svc.serve, daemon=True).start()
    GLib.MainLoop().run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
