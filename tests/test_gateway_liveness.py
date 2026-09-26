"""Dead gateway connections: liveness drop, fallback routing, undeliverable re-send."""
import asyncio
import struct
from pathlib import Path

import pytest

from hub import proto
from hub import proto_server as ps
from hub.db import DB, Gateway
from hub.proto_router import HubProtoRouter

GW_A, GW_B, DEV = "aa" * 8, "bb" * 8, "d1" * 8


class _Transport:
    def __init__(self):
        self.aborted = False

    def abort(self):
        self.aborted = True


class _Writer:
    def __init__(self):
        self.transport = _Transport()


def _gw(gid, name):
    return Gateway(id=gid, name=name, pubkey_b64="", mdns_addr="", registered_at=0, last_seen=None)


def test_liveness_drops_silent_gateway_only(tmp_path: Path):
    srv = ps.ProtoServer(DB(tmp_path / "h.db"))
    a = ps.GatewayConn(_gw(GW_A, "a"), _Writer(), "x:1", last_rx=100.0)
    b = ps.GatewayConn(_gw(GW_B, "b"), _Writer(), "x:2", last_rx=115.0)
    srv._conns = {GW_A: a, GW_B: b}
    assert srv.check_liveness(now=100.0 + ps.LIVENESS_S + 1) == [GW_A]
    assert not srv.is_gateway_online(GW_A) and srv.is_gateway_online(GW_B)
    assert a.writer.transport.aborted and not b.writer.transport.aborted


class _Stub:
    def __init__(self, online):
        self.online_gw = set(online)
        self.sent = []

    def is_gateway_online(self, g):
        return g in self.online_gw

    async def send_to_gateway(self, g, frame):
        self.sent.append((g, frame))
        return True


def _router(tmp_path, online):
    db = DB(tmp_path / "h.db")
    db.register_device(DEV, "s1", "sample_c6", "")
    db.set_provision_info(device_id=DEV, device_type="synth",
                          enc_pubkey_b64="x", gateway_id=GW_A)
    db.register_gateway(GW_A, "gw-a", "x", "")
    db.register_gateway(GW_B, "gw-b", "x", "")
    srv = _Stub(online)
    return HubProtoRouter(db, srv, hub_privkey_path=tmp_path / "priv.bin"), srv


@pytest.mark.asyncio
async def test_send_h2d_falls_back_when_usual_gateway_offline(tmp_path):
    r, srv = _router(tmp_path, {GW_B})
    ok = await r.send_h2d(DEV, struct.pack("<HI", proto.Cmd.AUTO_EVENT_ACK, 7))
    assert ok and srv.sent[0][0] == GW_B
    assert r._resolve_gateway_for_device(DEV) == GW_B


@pytest.mark.asyncio
async def test_no_gateway_online_is_not_sent(tmp_path):
    r, srv = _router(tmp_path, set())
    assert await r.send_h2d(DEV, struct.pack("<HI", proto.Cmd.AUTO_EVENT_ACK, 7)) is False
    assert srv.sent == []


@pytest.mark.asyncio
async def test_undeliverable_resends_once_via_other_gateway(tmp_path):
    r, srv = _router(tmp_path, {GW_A, GW_B})
    payload = struct.pack("<HI", proto.Cmd.AUTO_EVENT_ACK, 9)
    assert await r.send_h2d(DEV, payload)
    assert srv.sent[-1][0] == GW_A
    nack = struct.pack("<bH", -113, 8) + bytes.fromhex(DEV) + struct.pack(
        "<HI", proto.Cmd.AUTO_EVENT_ACK, 9)
    await r._on_h2d_undeliverable(GW_A, nack)
    await asyncio.sleep(0.05)                        # _redeliver task
    assert srv.sent[-1][0] == GW_B and len(srv.sent) == 2
    f, _ = proto.decode(srv.sent[-1][1])
    assert f.payload == payload
    await r._on_h2d_undeliverable(GW_B, nack)        # only once
    await asyncio.sleep(0.05)
    assert len(srv.sent) == 2


@pytest.mark.asyncio
async def test_unreachable_pushes_route_then_resends_same_gateway(tmp_path):
    r, srv = _router(tmp_path, {GW_A})
    r._db.set_provision_info(device_id=DEV, device_type="synth", enc_pubkey_b64="x",
                             gateway_id=GW_A, ml_eid="fd79:1d92:6d00:1::99")
    calls = []

    async def fake_gw_request(gid, cmd, body=b"", timeout=10, attempts=2):
        calls.append((gid, cmd, body))
        return 0, b""
    r.gateway_request = fake_gw_request
    payload = struct.pack("<HI", proto.Cmd.AUTO_EVENT_ACK, 11)
    await r.send_h2d(DEV, payload)
    nack = struct.pack("<bH", -113, 8) + bytes.fromhex(DEV) + struct.pack(
        "<HI", proto.Cmd.AUTO_EVENT_ACK, 11)
    await r._on_h2d_undeliverable(GW_A, nack)
    await asyncio.sleep(0.05)
    assert calls and calls[0][1] == proto.Cmd.GW_ROUTE_SET
    assert calls[0][2][-16:] == bytes.fromhex("fd791d926d0000010000000000000099")
    assert [g for g, _ in srv.sent] == [GW_A, GW_A]


def test_gateway_recorded_once_when_provisioning_left_none(tmp_path):
    db = DB(tmp_path / "h.db")
    db.register_device(DEV, "s2", "end_device", "")
    db.set_provision_info(device_id=DEV, device_type="end_device", enc_pubkey_b64="x", gateway_id="")
    assert db.set_device_gateway_if_empty(DEV, GW_A) is True
    assert db.get_provision_info(DEV).gateway_id == GW_A
    assert db.set_device_gateway_if_empty(DEV, GW_B) is False       # never overwrites
    assert db.get_provision_info(DEV).gateway_id == GW_A
