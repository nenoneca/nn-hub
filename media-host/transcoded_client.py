"""Client for nn-transcoded — compressed AUs cross via SHARED MEMORY.

The camera writes each access unit ONCE into a pool slot and sends a 14-byte
datagram naming the slot; the service reads the bytes in place and wraps
them into a GStreamer buffer without copying.  Nothing but the notification
travels over the socket, and raw frames never leave the service at all.

Never raises at import or when the daemon is missing: the caller falls back
to transcoding in its own pipeline, so a camera cannot lose HLS because a
service is absent.
"""
from __future__ import annotations

import mmap
import os
import socket
import struct
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.environ.get("NN_ACCEL_GEN", "/opt/nn-accel/gen"))

import nn_accel_pb2 as pb                     # noqa: E402

SOCK_ENV = "NN_TRANSCODED_SOCK"
SOCK_DFLT = "/run/nn-transcoded/sock"

MAGIC = 0x4E4E5031                            # "NNP1" — must match shmpool.h
SLOTS_MAX = 64
FREE, OWNED, BUSY = 0, 1, 2
_STATE_OFF = 32
_SEQ_OFF = _STATE_OFF + 4 * SLOTS_MAX         # 288
_HDR_BYTES = _SEQ_OFF + 8 * SLOTS_MAX         # 800

# Per-slot in-band header.  The shared pool header has no length field and
# the C side defines the same layout, so widening it would risk silent
# corruption; an AU is variable-length, so its length rides with the data.
AU_HDR = struct.Struct("<IIQ")                # bytes, flags, ts_ms
AU_KEYFRAME = 1


class ShmAuWriter:
    """Writes access units into a shared pool.  Never blocks the caller."""

    def __init__(self, owner: str, slot_bytes: int = 256 * 1024,
                 slots: int = 48, timeout: float = 25.0):
        # The ring must hold every buffer the pipeline has in flight, and a
        # camera feeds FRAGMENTS, not whole frames: h264parse keeps each
        # fragment until the access unit completes, so one frame can pin
        # several slots.  With 8 slots the ring filled, pushes were refused,
        # and the surviving stream decoded to so few frames that HLS could
        # only cut a segment every ~20 s.
        # Generous: opening a session makes the service build a pipeline and
        # wait for the hardware codec to reach PLAYING, which is seconds, not
        # milliseconds.  Pushing AUs afterwards never blocks on a reply.
        self.owner = owner
        self.slots = slots
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.sock.settimeout(timeout)
        self.sock.connect(os.environ.get(SOCK_ENV, SOCK_DFLT))
        self.sock.send(pb.Msg(id=1, hello=pb.HelloReq(
            client=owner, version="0.1")).SerializeToString())
        r = pb.Msg(); r.ParseFromString(self.sock.recv(65536))
        self.version = r.caps.service_version

        pg = os.sysconf("SC_PAGESIZE")
        self.slot_b = (slot_bytes + pg - 1) // pg * pg
        self.hdr_len = (_HDR_BYTES + pg - 1) // pg * pg
        total = self.hdr_len + self.slot_b * slots
        self.fd = os.memfd_create("nn_tc_pool", 0)
        os.ftruncate(self.fd, total)
        self.map = mmap.mmap(self.fd, total, mmap.MAP_SHARED,
                             mmap.PROT_READ | mmap.PROT_WRITE)
        struct.pack_into("<8I", self.map, 0, MAGIC, slots, self.slot_b,
                         self.hdr_len, 0, 0, 0, 0)
        self._seq = 0
        self._lock = threading.Lock()
        self._acc = bytearray()
        self.dropped = 0

    def open_session(self, bitrate: int, gop: int, qp: int = 0,
                     mux_ts: bool = True) -> pb.TranscodeSession:
        m = pb.Msg(id=2, tc_open=pb.TranscodeOpen(
            owner=self.owner, in_port=0, bitrate=bitrate, gop=gop,
            qp=qp, mux_ts=mux_ts))
        socket.send_fds(self.sock, [m.SerializeToString()], [self.fd])
        data = self.sock.recv(65536)
        if not data:
            raise ConnectionError("nn-transcoded closed the connection")
        r = pb.Msg(); r.ParseFromString(data)
        if r.WhichOneof("body") == "error":
            raise RuntimeError(r.error.message)
        return r.tc_sess

    def _acquire(self):
        for i in range(self.slots):
            if struct.unpack_from("<I", self.map, _STATE_OFF + 4 * i)[0] == FREE:
                struct.pack_into("<I", self.map, _STATE_OFF + 4 * i, OWNED)
                return i
        return None

    def push_fragment(self, frag: bytes, ts_ms: int) -> bool:
        """Accumulate uplink fragments and emit ONE slot per access unit.

        A camera sends ~1.4 KB records, ~20 per frame.  One slot per fragment
        pins the whole ring while h264parse waits for the frame to complete
        (measured: 53% refused), so frames — not fragments — are what the
        ring should hold.

        These encoders emit NO access-unit delimiter, so a new AU is detected
        at the first parameter set or slice that follows a slice already in
        the buffer.  A size cap flushes anyway, so an unexpected stream can
        never grow without bound."""
        self._acc.extend(frag)
        cut = self._au_boundary(self._acc)
        if cut <= 0:
            cap = self.slot_b - AU_HDR.size          # must leave room for it
            if len(self._acc) > cap:
                au = bytes(self._acc[:cap]); self._acc = bytearray(self._acc[cap:])
                return self.push(au, ts_ms, self._is_kf(au))
            return True                              # still accumulating
        au = bytes(self._acc[:cut])
        self._acc = bytearray(self._acc[cut:])
        return self.push(au, ts_ms, self._is_kf(au)) if au else True

    @staticmethod
    def _au_boundary(buf: bytes) -> int:
        """Offset where the NEXT access unit starts, or -1.

        VCL NALs are 1 (slice) and 5 (IDR); 7/8 (SPS/PPS) belong to the AU
        that follows them, so a parameter set after a slice also opens one."""
        seen_vcl = False
        i = buf.find(b"\x00\x00\x01")
        while i != -1 and i + 3 < len(buf):
            nt = buf[i + 3] & 0x1f
            begin = i - 1 if (i > 0 and buf[i - 1] == 0) else i
            if nt in (1, 5):
                if seen_vcl:
                    return begin
                seen_vcl = True
            elif nt == 7 and seen_vcl:
                return begin
            i = buf.find(b"\x00\x00\x01", i + 3)
        return -1

    @staticmethod
    def _is_kf(au: bytes) -> bool:
        i = au.find(b"\x00\x00\x01")
        while i != -1 and i + 3 < len(au):
            if (au[i + 3] & 0x1f) in (5, 7):
                return True
            i = au.find(b"\x00\x00\x01", i + 3)
        return False

    def push(self, au: bytes, ts_ms: int, keyframe: bool = False) -> bool:
        """Copy the AU into a slot and tell the service which one.

        Returns False when every slot is still in flight — the caller drops
        rather than blocks, because this runs on the ingest thread and a
        stalled transcoder must never back-pressure the live uplink."""
        n = len(au)
        if n + AU_HDR.size > self.slot_b:
            self.dropped += 1
            return False
        with self._lock:
            slot = self._acquire()
            if slot is None:
                self.dropped += 1
                return False
            off = self.hdr_len + slot * self.slot_b
            AU_HDR.pack_into(self.map, off, n,
                             AU_KEYFRAME if keyframe else 0, ts_ms)
            self.map[off + AU_HDR.size:off + AU_HDR.size + n] = au
            self._seq += 1
            struct.pack_into("<Q", self.map, _SEQ_OFF + 8 * slot, self._seq)
            struct.pack_into("<I", self.map, _STATE_OFF + 4 * slot, BUSY)
            m = pb.Msg(id=self._seq & 0x7FFFFFFF, tc_au=pb.TranscodeAu(
                slot=slot, seq=self._seq, ts_ms=ts_ms, bytes=n,
                keyframe=keyframe))
            try:
                self.sock.send(m.SerializeToString())
            except OSError:
                # service gone: free the slot so we don't leak the ring
                struct.pack_into("<I", self.map, _STATE_OFF + 4 * slot, FREE)
                raise
        return True

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


def try_open(owner: str, bitrate: int, gop: int, qp: int = 0,
             mux_ts: bool = True):
    """Best-effort: returns (writer, out_port) or (None, None)."""
    try:
        w = ShmAuWriter(owner)
        sess = w.open_session(bitrate, gop, qp, mux_ts)
        if not sess.running or not sess.out_port:
            print(f">> nn-transcoded refused the session: "
                  f"{sess.error or 'unknown'}", file=sys.stderr, flush=True)
            w.close()
            return None, None
        print(f">> nn-transcoded {w.version}: {owner} shm -> tcp:{sess.out_port}",
              file=sys.stderr, flush=True)
        return w, sess.out_port
    except Exception as e:                     # noqa: BLE001 - optional dep
        print(f">> nn-transcoded unavailable ({e}) — transcoding in-process",
              file=sys.stderr, flush=True)
        return None, None
