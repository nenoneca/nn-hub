"""The cascade group key must reach every device even on a lossy mesh.

SESS_GROUP_KEY is a sealed H2D frame with no acknowledgement.  c6-s3 missed
one (2026-09-20), kept a stale epoch and logged "AUTO_NOTIFY_S open rv=-22"
on every cascade until the next daily rotation.  The hub now sends a burst,
re-pushes periodically, and re-pushes at once when a device's uploaded log
line says it cannot open a notify.
"""
import asyncio
import struct
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from hub.db import DB
from hub import proto
from hub.proto_router import HubProtoRouter

pytestmark = pytest.mark.asyncio
DEV, GW = "aa00bb11cc22dd33", "gggg0000"


class _StubServer:
    def __init__(self):
        self.online_gw = {GW}
        self.sent = []

    def is_gateway_online(self, gw):
        return gw in self.online_gw

    async def send_to_gateway(self, gw, frame):
        self.sent.append((gw, frame))
        return True


class _FakeSess:
    """seal() just tags the payload; the device side is not under test."""
    def seal(self, aad, payload):
        return b"S" + payload


def _router(tmp_path: Path, verified=True):
    db = DB(tmp_path / "h.db")
    db.register_device(DEV, "synth", "sample_c6", "")
    db.set_provision_info(device_id=DEV, device_type="sample_c6",
                          enc_pubkey_b64="x", gateway_id=GW)
    db.register_gateway(GW, "gw-stub", "x", "")
    db.set_setting("group_key_epoch", "32")
    db.set_setting("group_key_hex", "ab" * 32)
    srv = _StubServer()
    r = HubProtoRouter(db, srv, hub_privkey_path=tmp_path / "priv.bin")
    r.GROUP_KEY_GAP_S = 0.0
    r._sessions[DEV] = {"sess": _FakeSess(), "verified": verified,
                        "dev_salt": b"\x01" * 8, "hub_salt": b"\x02" * 8}
    return r, srv, db


def _group_key_frames(srv):
    out = []
    for gw, frame in srv.sent:
        f, _ = proto.decode(frame)
        cmd, _tid = struct.unpack_from("<HI", f.payload, 0)
        if cmd == proto.Cmd.SESS_GROUP_KEY:
            out.append(f.payload[6:])
    return out


async def test_push_is_a_burst_and_is_recorded(tmp_path):
    r, srv, db = _router(tmp_path)
    await r._push_group_key(DEV, "verified")
    frames = _group_key_frames(srv)
    assert len(frames) == 3
    assert all(fr[1:5] == (32).to_bytes(4, "little") for fr in frames), "epoch 32 in every frame"
    st = r.session_status(DEV)
    assert st["group_key"]["epoch"] == 32 and st["group_key"]["pushes"] == 1
    assert st["group_key"]["reason"] == "verified" and st["hub_group_key_epoch"] == 32


async def test_unverified_device_is_not_pushed(tmp_path):
    r, srv, db = _router(tmp_path, verified=False)
    await r._push_group_key(DEV)
    assert _group_key_frames(srv) == []


async def test_device_log_about_unknown_epoch_triggers_repush(tmp_path):
    r, srv, db = _router(tmp_path)
    await r._on_log_line(DEV, 0, b"[wrn] auto_engine: AUTO_NOTIFY_S open rv=-22")
    await asyncio.sleep(0.05)
    assert len(_group_key_frames(srv)) == 3
    assert r.session_status(DEV)["group_key"]["reason"] == "device-log"
    # rate limited: a second identical line within 20 s does not push again
    await r._on_log_line(DEV, 0, b"[wrn] auto_engine: AUTO_NOTIFY_S open rv=-22")
    await asyncio.sleep(0.05)
    assert len(_group_key_frames(srv)) == 3
    # an unrelated line never pushes
    r.group_key_pushes.clear()
    await r._on_log_line(DEV, 0, b"[wrn] nn_proto_client: rel tid=86 gave up (rounds=4)")
    await asyncio.sleep(0.05)
    assert len(_group_key_frames(srv)) == 3


async def test_periodic_repush_reaches_every_verified_device(tmp_path):
    r, srv, db = _router(tmp_path)
    n = await r.repush_group_keys("periodic")
    assert n == 1 and len(_group_key_frames(srv)) == 3
    assert r.session_status(DEV)["group_key"]["reason"] == "periodic"


async def test_rotation_pushes_the_new_epoch(tmp_path):
    r, srv, db = _router(tmp_path)
    epoch = await r.rotate_group_key()
    assert epoch == 33
    frames = _group_key_frames(srv)
    assert frames and all(fr[1:5] == (33).to_bytes(4, "little") for fr in frames)
    assert r.session_status(DEV)["group_key"]["reason"] == "rotation"
