"""nn_proto TCP server (hub side).

Accepts gateway TCP connections, parses streamed nn_proto frames, and
verifies the outer ECDSA P-256 signature against the registered gateway's
public key.

The first valid frame from a gateway authenticates the connection: the
frame's `device_id` must match a row in the `gateways` table whose
`pubkey_b64` verifies the sig.  Subsequent frames flow through to a
caller-provided handler.

Phase 2.C scope: connection lifecycle + frame I/O + sig verify.
Routing, gateway-command dispatch, and TLS land in later phases.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import socket
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

from . import proto
from .db import DB, Gateway

log = logging.getLogger("hub.proto_server")


@dataclass
class GatewayConn:
    gateway: Gateway
    writer:  asyncio.StreamWriter
    peername: str  # "host:port" of remote, for logs
    last_rx: float = field(default_factory=time.monotonic)


# A gateway sends a frame every 5 s (HUB_STATUS_QUERY + state).  One silent
# for this long is hung, frozen or cut off without a FIN (Wi-Fi drop, host
# crash): drop the connection so the hub stops "handing" frames into a dead
# socket and routes through another gateway instead.  Measured 2026-09-24:
# a frozen gateway kept looking online for its whole 60 s freeze.
LIVENESS_S = 20.0
DRAIN_TIMEOUT_S = 5.0


def _tune_socket(writer: asyncio.StreamWriter) -> None:
    """Kernel-level backstop: keepalive probes, and give up on data the
    peer never acknowledges after 15 s (TCP_USER_TIMEOUT)."""
    sock = writer.get_extra_info("socket")
    if sock is None:
        return
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        for opt, val in (("TCP_KEEPIDLE", 10), ("TCP_KEEPINTVL", 5),
                         ("TCP_KEEPCNT", 3), ("TCP_USER_TIMEOUT", 15000)):
            if hasattr(socket, opt):
                sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, opt), val)
    except OSError as e:
        log.debug("socket tuning failed: %s", e)


# Handler signature: (gateway_id, frame).  The handler is called from
# inside the asyncio loop; it should not block.
FrameHandler = Callable[[str, proto.Frame], Awaitable[None]]


class ProtoServer:
    def __init__(self, db: DB, host: str = "0.0.0.0",
                 port: int = 8767):
        self._db   = db
        self._host = host
        self._port = port
        self._conns: dict[str, GatewayConn] = {}  # gateway_id → conn
        self._handler: Optional[FrameHandler] = None
        self._server: Optional[asyncio.AbstractServer] = None

    def set_frame_handler(self, fn: FrameHandler):
        self._handler = fn

    async def start(self):
        self._server = await asyncio.start_server(
            self._on_client, host=self._host, port=self._port,
            family=socket.AF_INET,
        )
        addrs = ", ".join(str(s.getsockname()) for s in self._server.sockets)
        log.info("nn_proto TCP listening on %s", addrs)

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def serve_forever(self):
        if not self._server:
            await self.start()
        watchdog = asyncio.ensure_future(self._liveness_loop())
        try:
            async with self._server:
                await self._server.serve_forever()
        finally:
            watchdog.cancel()

    def _drop(self, conn: GatewayConn, why: str) -> None:
        gid = conn.gateway.id
        if self._conns.get(gid) is conn:
            self._conns.pop(gid, None)
        log.warning("[%s] gateway %s (%s): %s -- connection dropped",
                    conn.peername, gid, conn.gateway.name, why)
        try:
            conn.writer.transport.abort()
        except Exception:
            pass

    def check_liveness(self, now: Optional[float] = None) -> list[str]:
        """Drop every connection silent for LIVENESS_S.  Returns the ids."""
        now = time.monotonic() if now is None else now
        dropped = []
        for conn in list(self._conns.values()):
            idle = now - conn.last_rx
            if idle > LIVENESS_S:
                self._drop(conn, f"silent for {idle:.0f} s")
                dropped.append(conn.gateway.id)
        return dropped

    async def _liveness_loop(self):
        while True:
            await asyncio.sleep(5)
            try:
                self.check_liveness()
            except Exception as e:                       # noqa: BLE001
                log.warning("liveness check failed: %s", e)

    # ── outbound (hub → gateway) ────────────────────────────────────────

    def is_gateway_online(self, gateway_id: str) -> bool:
        return gateway_id in self._conns

    async def send_to_gateway(self, gateway_id: str, frame: bytes) -> bool:
        """Write a complete frame to the gateway's TCP socket.

        Returns True on success, False if the gateway isn't connected
        or the write fails.
        """
        conn = self._conns.get(gateway_id)
        if not conn:
            return False
        try:
            conn.writer.write(frame)
            # a peer that stopped reading fills the socket buffer and
            # drain() would wait forever
            await asyncio.wait_for(conn.writer.drain(), DRAIN_TIMEOUT_S)
            return True
        except asyncio.TimeoutError:
            self._drop(conn, f"not reading (send buffer full for {DRAIN_TIMEOUT_S:.0f} s)")
            return False
        except Exception as e:
            log.warning("send_to_gateway(%s) write failed: %s", gateway_id, e)
            return False

    # ── inbound ─────────────────────────────────────────────────────────

    async def _on_client(self, reader: asyncio.StreamReader,
                         writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        peer_str = f"{peer[0]}:{peer[1]}" if peer else "?"
        log.info("[%s] gateway connecting...", peer_str)

        gw: Optional[Gateway] = None
        try:
            gw = await self._handshake(reader, writer, peer_str)
            if not gw:
                return  # _handshake already logged + closed
            _tune_socket(writer)
            conn = GatewayConn(gateway=gw, writer=writer, peername=peer_str)
            old = self._conns.get(gw.id)
            if old is not None and old.writer is not writer:
                # the gateway reconnected: its previous socket is dead
                self._drop(old, "reconnected on a new socket")
            self._conns[gw.id] = conn
            self._db.touch_gateway(gw.id)
            log.info("[%s] gateway %s authenticated; serving frames",
                     peer_str, gw.id)
            await self._serve_frames(reader, conn)
        except Exception as e:
            log.warning("[%s] connection ended: %s", peer_str, e)
        finally:
            if gw and self._conns.get(gw.id) and \
               self._conns[gw.id].writer is writer:
                self._conns.pop(gw.id, None)
                log.info("[%s] gateway %s disconnected", peer_str, gw.id)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _read_frame(self,
                          reader: asyncio.StreamReader) -> Optional[tuple[proto.Frame, bytes]]:
        """Read exactly one frame from the stream.  Returns
        (frame, raw_bytes) or None on EOF.  Raises proto.ProtoError on
        malformed input."""
        # Read the 10-byte fixed header to learn pkt_size.
        header = await reader.readexactly(proto.HEADER_FIXED)
        if header[:2] != proto.MAGIC:
            raise proto.BadMagic(f"bad magic: {header[:2]!r}")
        import struct
        _type, pkt_size, _did_size = struct.unpack("<HIH", header[2:10])
        # Total frame = 8 + pkt_size; we already consumed 10 bytes,
        # so we need (pkt_size - 2) more.
        rest = await reader.readexactly(pkt_size - 2)
        raw = header + rest
        frame, consumed = proto.decode(raw)
        assert consumed == len(raw)
        return frame, raw

    async def _handshake(self, reader: asyncio.StreamReader,
                         writer: asyncio.StreamWriter,
                         peer_str: str) -> Optional[Gateway]:
        """Read the first frame, look up the gateway by its device_id,
        verify the sig.  Returns the Gateway on success, None on
        failure (and closes the writer)."""
        try:
            res = await asyncio.wait_for(self._read_frame(reader), timeout=10)
        except asyncio.IncompleteReadError:
            log.info("[%s] EOF before any frame", peer_str)
            return None
        except (proto.ProtoError, asyncio.TimeoutError) as e:
            log.warning("[%s] handshake read failed: %s", peer_str, e)
            return None
        if not res:
            return None
        frame, _raw = res
        if frame.device_id_size == 0:
            log.warning("[%s] handshake frame has empty device_id", peer_str)
            return None
        gw_id = frame.device_id.hex()
        gw = self._db.get_gateway(gw_id)
        if not gw:
            log.warning("[%s] handshake: gateway_id %s not registered",
                        peer_str, gw_id)
            return None
        try:
            pub_blob = base64.b64decode(gw.pubkey_b64)
            pubkey = proto.pubkey_from_uncompressed(pub_blob)
            proto.verify_sig(frame, pubkey)
        except Exception as e:
            log.warning("[%s] handshake sig verify failed for %s: %s",
                        peer_str, gw_id, e)
            return None
        # Hand the first frame off to the application handler too —
        # carries useful info (HUB_STATUS_QUERY, etc).
        if self._handler:
            try:
                await self._handler(gw.id, frame)
            except Exception as e:
                log.warning("[%s] handler raised on first frame: %s",
                            peer_str, e)
        return gw

    async def _serve_frames(self, reader: asyncio.StreamReader,
                            conn: GatewayConn):
        gw = conn.gateway
        pub_blob = base64.b64decode(gw.pubkey_b64)
        pubkey = proto.pubkey_from_uncompressed(pub_blob)

        while True:
            try:
                res = await self._read_frame(reader)
            except asyncio.IncompleteReadError:
                return  # peer closed
            except proto.ProtoError as e:
                log.warning("[%s] %s frame parse error: %s",
                            conn.peername, gw.id, e)
                return
            frame, _raw = res
            conn.last_rx = time.monotonic()

            # For frames the gateway forwards opaquely (D2H), the inner
            # signature is from the device, not the gateway — we can
            # only verify the outer envelope when the device_id matches
            # the gateway itself (D2G/G2D).  For Phase 2.C we accept
            # anything from an authenticated gateway socket and rely on
            # the connection-level auth.  Phase 2.E will plumb device
            # signature verification.
            if frame.device_id == bytes.fromhex(gw.id):
                try:
                    proto.verify_sig(frame, pubkey)
                except proto.BadSignature as e:
                    log.warning("[%s] %s gateway-signed frame "
                                "failed verify: %s", conn.peername, gw.id, e)
                    return
            self._db.touch_gateway(gw.id)
            if self._handler:
                try:
                    await self._handler(gw.id, frame)
                except Exception as e:
                    log.warning("[%s] handler raised on frame: %s",
                                conn.peername, e)
