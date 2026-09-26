"""SESS_INIT must not be fire-and-forget.

Found 2026-09-18: after a hub restart the hub re-derives each sensor's
session from the salt in its heartbeat and sends ONE SESS_INIT.  Over the
Thread mesh that frame is lost 5-30 % of the time; the device then never
probes, the hub keeps the session "derived but unverified", falls back to
plain field ops, and — the part that hurt — never pushes the daily group-key
rotation to that device.  Two rotations later the device cannot open the
cascade notifies from its neighbours: c6-s1's button stopped moving c6-s2,
and after the next restart c6-s3 drew the short straw instead.

Now the hub re-sends SESS_INIT on a schedule while unverified and on every
heartbeat that repeats a known salt, and stops the moment the probe verifies.
"""
import asyncio
import base64
import struct
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
from cryptography.hazmat.primitives import serialization

from hub.db import DB
from hub import proto
from hub.proto_router import HubProtoRouter

pytestmark = pytest.mark.asyncio

DEV = "aa00bb11cc22dd33"
GW = "gggg0000"


class _StubServer:
    def __init__(self):
        self.online_gw = {GW}
        self.sent: list[tuple[str, bytes]] = []

    def is_gateway_online(self, gw_id):
        return gw_id in self.online_gw

    async def send_to_gateway(self, gw_id, frame):
        self.sent.append((gw_id, frame))
        return True


def _router(tmp_path: Path):
    db = DB(tmp_path / "h.db")
    db.register_device(DEV, "synth", "sample_c6", "")
    dev_pub = X25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    db.set_provision_info(device_id=DEV, device_type="sample_c6",
                          enc_pubkey_b64=base64.b64encode(dev_pub).decode(),
                          gateway_id=GW)
    db.register_gateway(GW, "gw-stub", "x", "")
    srv = _StubServer()
    router = HubProtoRouter(db, srv, hub_privkey_path=tmp_path / "priv.bin")
    router.set_enc_key(X25519PrivateKey.generate())
    return router, srv


async def _heartbeat(router, salt: bytes, uptime_ms: int = 1234):
    inner = (int(proto.Cmd.DEVICE_HEARTBEAT).to_bytes(2, "little")
             + (0).to_bytes(4, "little")
             + struct.pack("<I", uptime_ms) + salt)
    priv = ec.generate_private_key(proto.CURVE)
    f_in, _ = proto.decode(proto.encode(proto.FrameType.D2H,
                                        bytes.fromhex(DEV), inner, priv))
    await router.handle_frame(GW, f_in)
    await asyncio.sleep(0.05)          # let the create_task()s run


def _sess_inits(srv) -> list[bytes]:
    out = []
    for gw, frame in srv.sent:
        f, _ = proto.decode(frame)
        cmd, _tid = struct.unpack_from("<HI", f.payload, 0)
        if cmd == proto.Cmd.SESS_INIT:
            out.append(bytes(f.payload[6:14]))      # the 8-byte hub salt
    return out


async def test_unverified_session_resends_init_on_heartbeat(tmp_path):
    router, srv = _router(tmp_path)
    router.sess_init_retry_s = ()             # isolate the heartbeat path
    salt = b"\x11" * 8
    await _heartbeat(router, salt)
    inits = _sess_inits(srv)
    assert len(inits) == 1, "first heartbeat with a new salt derives and sends one INIT"
    hub_salt = inits[0]
    st = router._sessions[DEV]
    assert st["sess"] is not None and not st["verified"]

    await _heartbeat(router, salt)            # same salt, still unverified
    inits = _sess_inits(srv)
    assert len(inits) == 2 and inits[1] == hub_salt, \
        "a repeated heartbeat while unverified must re-send the SAME INIT"

    st["verified"] = True                     # the probe landed
    await _heartbeat(router, salt)
    assert len(_sess_inits(srv)) == 2, "verified: heartbeats no longer re-send"


async def test_unverified_session_retries_init_on_a_timer(tmp_path):
    router, srv = _router(tmp_path)
    router.sess_init_retry_s = (0.05, 0.05)
    await _heartbeat(router, b"\x22" * 8)
    await asyncio.sleep(0.25)
    inits = _sess_inits(srv)
    assert len(inits) == 3, f"1 initial + 2 timed re-sends expected, got {len(inits)}"
    assert len(set(inits)) == 1, "every re-send carries the same hub salt (device re-probes, never re-derives)"


async def test_retry_stops_once_verified(tmp_path):
    router, srv = _router(tmp_path)
    router.sess_init_retry_s = (0.05, 0.05, 0.05)
    await _heartbeat(router, b"\x33" * 8)
    router._sessions[DEV]["verified"] = True
    await asyncio.sleep(0.3)
    assert len(_sess_inits(srv)) == 1


async def test_new_salt_supersedes_old_retry(tmp_path):
    """The device rebooted mid-handshake: the old timer must not keep
    re-sending the stale hub salt."""
    router, srv = _router(tmp_path)
    router.sess_init_retry_s = (0.05, 0.05)
    await _heartbeat(router, b"\x44" * 8)
    old = _sess_inits(srv)[0]
    await _heartbeat(router, b"\x55" * 8)     # new per-boot salt
    await asyncio.sleep(0.3)
    inits = _sess_inits(srv)
    assert inits.count(old) == 1, "the superseded hub salt is never re-sent"
    assert len(inits) >= 3                      # new handshake + its retries
