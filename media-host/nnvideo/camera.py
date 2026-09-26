"""Camera handlers: the only code that knows the wire.

Ingress  — ONE listening port for every camera (plus legacy per-slot ingest
           ports as aliases so no camera needs re-provisioning).  A camera
           connects, sends its HELLO; the static device public key in it
           identifies the camera in the store, which also says which host key
           the camera was provisioned with.  A device the store does not know
           yet is identified the slow way: every host key is tried against the
           first record; the one that opens it names the camera (legacy
           per-slot keys), and the pair is recorded so the next connect is a
           lookup.
CameraHandler — one per live connection: decrypts records, splits them, and
           submits `Request(camera_id, "ingest", kind="record", ...)`.  Owns
           the back-channel (adapt controller, config push) through the
           session; nothing else.
FileHandler — loops a raw Annex-B file at a fixed fps for tests and gates.
"""
from __future__ import annotations

import asyncio
import os
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Callable, Optional

from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

from .request import Kind, Request

HERE = Path(__file__).resolve().parent.parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
from nn_sectun import SecureSession, hello_parts, read_hello, read_record_ct  # noqa: E402


def log(cam: str, msg: str) -> None:
    print(f"[{cam}/handler] {msg}", flush=True)


class Ingress:
    """Accept loop(s) on a thread; each accepted connection gets its own
    handler thread (blocking socket reads release the interpreter lock, and a
    thread per live camera is cheap)."""

    def __init__(self, engine, store, registry, *, ports: list[int],
                 on_session: Optional[Callable] = None):
        self.engine, self.store, self.registry = engine, store, registry
        self.ports = ports
        self.on_session = on_session          # optional (camera_id, sess) hook, unused since phase 2
        self._srv: list[socket.socket] = []
        self._threads: list[threading.Thread] = []
        self.handlers: dict[str, "CameraHandler"] = {}
        self.stopping = False
        self._loop = engine.loop

    def start(self) -> None:
        self.pending: list[int] = []
        for port in self.ports:
            if not self._listen(port):
                self.pending.append(port)
        if self.pending:
            threading.Thread(target=self._retry_pending, daemon=True, name="ingress-retry").start()

    def _listen(self, port: int) -> bool:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind(("0.0.0.0", port))
        except OSError as e:
            # a legacy alias still held by the old per-slot worker is not
            # fatal for every other camera: keep trying in the background
            srv.close()
            print(f"[nn-video] ingest port {port} busy ({e.strerror}); will retry", flush=True)
            return False
        srv.listen(8)
        self._srv.append(srv)
        t = threading.Thread(target=self._accept_loop, args=(srv, port),
                             daemon=True, name=f"ingress-{port}")
        t.start(); self._threads.append(t)
        print(f"[nn-video] ingest listening on 0.0.0.0:{port}", flush=True)
        return True

    def _retry_pending(self) -> None:
        while self.pending and not self.stopping:
            time.sleep(10)
            self.pending = [p for p in self.pending if not self._listen(p)]

    def stop(self) -> None:
        self.stopping = True
        for s in self._srv:
            try: s.close()
            except OSError: pass
        for h in list(self.handlers.values()):
            h.close()

    def _accept_loop(self, srv: socket.socket, port: int) -> None:
        while not self.stopping:
            try:
                conn, addr = srv.accept()
            except OSError:
                return
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            conn.settimeout(25)       # > the C6's 20 s send timeout (see legacy ingest)
            threading.Thread(target=self._serve, args=(conn, addr, port),
                             daemon=True, name=f"handshake-{addr[0]}").start()

    # ── identity ─────────────────────────────────────────────────────────
    def _host_priv(self, key_id: str) -> Optional[X25519PrivateKey]:
        hk = self.store.host_key(key_id)
        return X25519PrivateKey.from_private_bytes(hk[1]) if hk else None

    def identify(self, conn: socket.socket, hello: bytes, addr) -> tuple[str, SecureSession, bytes | None]:
        """(camera_id, session, first_plaintext_or_None).  Raises on failure."""
        _eph, dev_pub = hello_parts(hello)
        known = self.store.camera_for_device_pub(dev_pub)
        if known:
            cam, key_id = known
            priv = self._host_priv(key_id)
            if priv is None:
                raise ValueError(f"camera {cam} bound to unknown host key {key_id!r}")
            return cam, SecureSession.from_hello(conn, hello, priv), None
        # unknown device: try every host key against its first record
        ct = read_record_ct(conn)
        for key_id in self.store.host_key_ids():
            priv = self._host_priv(key_id)
            if priv is None:
                continue
            sess = SecureSession.from_hello(conn, hello, priv)
            pt = sess.try_open_first(ct)
            if pt is not None:
                cam = self.store.camera_for_host_key(key_id)
                if not cam:
                    raise ValueError(f"host key {key_id!r} opens the stream but names no camera")
                self.store.set_device_key(cam, dev_pub, key_id)
                log(cam, f"learned device key {dev_pub.hex()[:16]}… via host key {key_id!r}")
                return cam, sess, pt
        raise ValueError(f"no host key opens the first record from {addr} (device {dev_pub.hex()[:16]}…)")

    def _serve(self, conn: socket.socket, addr, port: int) -> None:
        try:
            hello = read_hello(conn)
            cam, sess, first = self.identify(conn, hello, addr)
        except Exception as e:                                   # noqa: BLE001
            print(f"[nn-video/ingress] rejected {addr} on :{port}: {e}", flush=True)
            try: conn.close()
            except OSError: pass
            return
        old = self.handlers.get(cam)
        if old is not None:
            log(cam, "reconnect: closing the previous session")
            old.close()
        h = CameraHandler(cam, sess, conn, addr, port, self, first)
        self.handlers[cam] = h
        try:
            h.run()                                              # this thread becomes the handler
        finally:
            if self.handlers.get(cam) is h:
                del self.handlers[cam]


class CameraHandler:
    def __init__(self, cam: str, sess: SecureSession, conn, addr, port: int,
                 ingress: Ingress, first_pt: bytes | None):
        self.cam, self.sess, self.conn, self.addr, self.port = cam, sess, conn, addr, port
        self.ingress = ingress
        self.first_pt = first_pt
        self.closed = False
        self.parser = None
        self.ctrl = None

    def submit(self, req: Request) -> None:
        loop = self.ingress._loop
        if loop is None or loop.is_closed():
            return
        asyncio.run_coroutine_threadsafe(self.ingress.engine.submit(req), loop)

    def close(self) -> None:
        self.closed = True
        try:
            self.conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.conn.close()
        except OSError:
            pass

    def run(self) -> None:
        from video_service import RecordParser
        reg = self.ingress.registry
        reg.connect(self.cam, peer=f"{self.addr[0]}:{self.addr[1]}", ingest_port=self.port,
                    device_pub=self.sess.device_pub.hex(), socket=self.conn, session=self.sess)
        log(self.cam, f"session up from {self.addr[0]}:{self.addr[1]} on :{self.port}")
        self.submit(Request(self.cam, "ingest", Kind.CONNECT,
                            {"peer": self.addr, "port": self.port}))
        self.parser = RecordParser()
        last, nbytes = time.time(), 0
        try:
            if self.first_pt is not None:
                self._records(self.first_pt)
            while not self.closed:
                data = self.sess.recv()
                nbytes += len(data)
                reg.touch(self.cam, len(data))
                self._records(data)
                if time.time() - last > 10:
                    last = time.time()
                    reg.update(self.cam, rx_bytes=nbytes)
        except Exception as e:                                   # noqa: BLE001
            if not self.closed:
                log(self.cam, f"session ended: {e!r}")
        finally:
            try: self.conn.close()
            except OSError: pass
            # a replaced session (reconnect) must not tear down its successor's
            # registry entry, ports or back-channel
            if self.ingress.handlers.get(self.cam) is self:
                reg.disconnect(self.cam)
                self.submit(Request(self.cam, "ingest", Kind.DISCONNECT, {"peer": self.addr}))

    def _records(self, data: bytes) -> None:
        rec = self.ingress.registry.raw(self.cam)
        ctrl = rec.get("ctrl") if rec else None          # the control pipeline's adapt controller
        for typ, flags, seq, ts, payload in self.parser.feed(data):
            if ctrl is not None and typ == 0x56:            # video: feed the adapt controller's stats
                try:
                    ctrl.on_video(seq, flags, len(payload), ts)
                except Exception:
                    pass
            self.submit(Request(self.cam, "ingest", "record",
                                {"typ": typ, "flags": flags, "seq": seq, "payload": payload},
                                ts_device=ts / 1000.0, meta={"seq": seq, "typ": typ}))


class FileHandler:
    """Loop a raw Annex-B .h264 at `fps`, emitting records exactly as the C6
    framing would: type 'V', absolute ms timestamps, each access unit split
    into ~4 KB fragments with VID_START (0x02) on the first and VID_END
    (0x04) on the last — the event ring reassembles AUs from those flags."""
    VID_START, VID_END = 0x02, 0x04

    def __init__(self, engine, registry, cam: str, path: str, fps: float = 30.0, chunk: int = 4096):
        self.engine, self.registry, self.cam, self.path, self.fps, self.chunk = engine, registry, cam, path, fps, chunk
        self._t: threading.Thread | None = None
        self.stopping = False

    def start(self) -> None:
        self._t = threading.Thread(target=self._loop, daemon=True, name=f"file-{self.cam}")
        self._t.start()

    def stop(self) -> None:
        self.stopping = True

    def _loop(self) -> None:
        from video_service import _split_access_units
        data = Path(self.path).read_bytes()
        aus = _split_access_units(data)
        if not aus:
            log(self.cam, f"no access units in {self.path}")
            return
        self.registry.connect(self.cam, peer=f"file:{os.path.basename(self.path)}", ingest_port=0)
        loop = self.engine.loop
        period = 1.0 / self.fps
        seq, t0 = 0, time.time()
        i = 0
        while not self.stopping:
            au = aus[i % len(aus)]; i += 1
            ts_ms = int(time.time() * 1000)
            frags = [au[j:j + self.chunk] for j in range(0, len(au), self.chunk)]
            for k, frag in enumerate(frags):
                flags = (self.VID_START if k == 0 else 0) | (self.VID_END if k == len(frags) - 1 else 0)
                req = Request(self.cam, "ingest", "record",
                              {"typ": 0x56, "flags": flags, "seq": seq, "payload": frag},
                              ts_device=ts_ms / 1000.0, meta={"seq": seq, "typ": 0x56})
                seq = (seq + 1) & 0xFFFF
                asyncio.run_coroutine_threadsafe(self.engine.submit(req), loop)
            self.registry.touch(self.cam, len(au))
            t0 += period
            delay = t0 - time.time()
            if delay > 0:
                time.sleep(delay)
        self.registry.disconnect(self.cam)
