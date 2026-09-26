"""Tests for hub.proto_router.HubProtoRouter Phase 6 FIELD_OP relay."""

import asyncio
import struct
from pathlib import Path

import pytest

from hub.db import DB
from hub import proto
from hub.proto_router import HubProtoRouter


pytestmark = pytest.mark.asyncio


class _StubServer:
    """Captures send_to_gateway calls instead of writing to a socket."""
    def __init__(self):
        self.online_gw: set[str] = set()
        self.sent: list[tuple[str, bytes]] = []

    def is_gateway_online(self, gw_id: str) -> bool:
        return gw_id in self.online_gw

    async def send_to_gateway(self, gw_id: str, frame: bytes) -> bool:
        self.sent.append((gw_id, frame))
        return True


async def test_request_field_round_trip(tmp_path: Path):
    db = DB(tmp_path / "h.db")
    # Pre-register a device + provision_info that points at a gateway.
    db.register_device("aa00bb11cc22dd33", "synth-A", "end_device", "")
    db.set_provision_info(
        device_id="aa00bb11cc22dd33",
        device_type="synth",
        enc_pubkey_b64="ignored-by-router",
        gateway_id="gggg0000",
    )
    db.register_gateway("gggg0000", "gw-stub", "irrelevant", "")

    srv = _StubServer()
    srv.online_gw.add("gggg0000")
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")

    envelope_in  = b'{"v":2,"epk":"...","ct":"req"}'
    envelope_out = b'{"v":2,"epk":"...","ct":"reply"}'

    # Schedule the FIELD_REPLY arrival just after request_field starts.
    async def fake_reply():
        # Wait for the H2D send to capture the tid we generated.
        for _ in range(50):
            if srv.sent:
                break
            await asyncio.sleep(0.01)
        assert srv.sent, "request_field never enqueued the H2D"

        # Decode the H2D frame to recover tid.
        sent_frame = srv.sent[-1][1]
        f, _ = proto.decode(sent_frame)
        cmd, tid = struct.unpack_from("<HI", f.payload, 0)
        assert cmd == proto.Cmd.FIELD_OP

        # Synthesize a D2H FIELD_REPLY into the router.
        inner = proto.Cmd.FIELD_REPLY.to_bytes(2, "little") \
              + tid.to_bytes(4, "little") + envelope_out
        from cryptography.hazmat.primitives.asymmetric import ec
        # Sign with throwaway P-256 key — handle_frame doesn't verify.
        priv = ec.generate_private_key(proto.CURVE)
        d2h = proto.encode(proto.FrameType.D2H,
                           bytes.fromhex("aa00bb11cc22dd33"),
                           inner, priv)
        f_in, _ = proto.decode(d2h)
        await router.handle_frame("gggg0000", f_in)

    task = asyncio.create_task(fake_reply())
    reply = await router.request_field("aa00bb11cc22dd33", envelope_in,
                                        timeout=2.0)
    await task
    assert reply == envelope_out


async def test_request_field_timeout(tmp_path: Path):
    db = DB(tmp_path / "h.db")
    db.register_device("aa00bb11cc22dd33", "synth-A", "end_device", "")
    db.set_provision_info(
        device_id="aa00bb11cc22dd33",
        device_type="synth",
        enc_pubkey_b64="x",
        gateway_id="gggg0000",
    )
    db.register_gateway("gggg0000", "gw-stub", "x", "")
    srv = _StubServer()
    srv.online_gw.add("gggg0000")
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")
    with pytest.raises(asyncio.TimeoutError):
        await router.request_field("aa00bb11cc22dd33", b"x", timeout=0.1)


async def test_request_field_concurrent_tids(tmp_path: Path):
    """Two outstanding requests; replies arrive out of order."""
    db = DB(tmp_path / "h.db")
    for i, name in enumerate(["aa", "bb"]):
        did = (name * 8)[:16]
        db.register_device(did, f"d-{i}", "end_device", "")
        db.set_provision_info(device_id=did, device_type="x",
                              enc_pubkey_b64="x", gateway_id="gggg0000")
    db.register_gateway("gggg0000", "gw-stub", "x", "")
    srv = _StubServer()
    srv.online_gw.add("gggg0000")
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")

    async def reply_after(target_did: str, marker: bytes, delay: float):
        # Wait for the matching H2D, then synthesize the D2H reply.
        end = asyncio.get_event_loop().time() + 2.0
        while asyncio.get_event_loop().time() < end:
            for gw_id, frame in srv.sent:
                f, _ = proto.decode(frame)
                if f.device_id.hex() == target_did:
                    cmd, tid = struct.unpack_from("<HI", f.payload, 0)
                    if cmd == proto.Cmd.FIELD_OP:
                        await asyncio.sleep(delay)
                        inner = (proto.Cmd.FIELD_REPLY.to_bytes(2, "little")
                                 + tid.to_bytes(4, "little") + marker)
                        from cryptography.hazmat.primitives.asymmetric import ec
                        priv = ec.generate_private_key(proto.CURVE)
                        d2h = proto.encode(proto.FrameType.D2H,
                                            bytes.fromhex(target_did),
                                            inner, priv)
                        f_in, _ = proto.decode(d2h)
                        await router.handle_frame("gggg0000", f_in)
                        return
            await asyncio.sleep(0.01)

    did_a = "aa" * 8
    did_b = "bb" * 8
    asyncio.create_task(reply_after(did_a, b"reply-A", 0.10))  # A is slower
    asyncio.create_task(reply_after(did_b, b"reply-B", 0.02))  # B replies first

    res_a, res_b = await asyncio.gather(
        router.request_field(did_a, b"req-A", timeout=2.0),
        router.request_field(did_b, b"req-B", timeout=2.0),
    )
    assert res_a == b"reply-A"
    assert res_b == b"reply-B"


async def test_field_reply_triggers_ack(tmp_path: Path):
    """When an inbound D2H FIELD_REPLY arrives, the hub must send back
    an H2D FIELD_REPLY_ACK with the same tid + empty body so the
    sensor's reliable-send retry loop retires its slot."""
    db = DB(tmp_path / "h.db")
    db.register_device("aa00bb11cc22dd33", "synth-A", "end_device", "")
    db.set_provision_info(
        device_id="aa00bb11cc22dd33",
        device_type="synth",
        enc_pubkey_b64="x",
        gateway_id="gggg0000",
    )
    db.register_gateway("gggg0000", "gw-stub", "x", "")

    srv = _StubServer()
    srv.online_gw.add("gggg0000")
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")

    # Synthesize an unsolicited D2H FIELD_REPLY (no pending future).
    from cryptography.hazmat.primitives.asymmetric import ec
    tid = 0xCAFE1234
    inner = (proto.Cmd.FIELD_REPLY.to_bytes(2, "little")
             + tid.to_bytes(4, "little") + b"opaque-body")
    priv = ec.generate_private_key(proto.CURVE)
    d2h = proto.encode(proto.FrameType.D2H,
                       bytes.fromhex("aa00bb11cc22dd33"),
                       inner, priv)
    f_in, _ = proto.decode(d2h)
    await router.handle_frame("gggg0000", f_in)

    # _send_field_reply_ack is scheduled via asyncio.create_task — let it run.
    for _ in range(50):
        if srv.sent:
            break
        await asyncio.sleep(0.01)
    assert srv.sent, "hub never emitted the FIELD_REPLY_ACK"

    ack_frame = srv.sent[-1][1]
    f, _ = proto.decode(ack_frame)
    assert f.type == proto.FrameType.H2D
    cmd, ack_tid = struct.unpack_from("<HI", f.payload, 0)
    assert cmd     == proto.Cmd.FIELD_REPLY_ACK
    assert ack_tid == tid
    assert len(f.payload) == 6, "FIELD_REPLY_ACK body must be empty"


async def test_field_reply_ack_fires_even_on_duplicate(tmp_path: Path):
    """A FIELD_REPLY that doesn't match any pending future must still
    trigger a FIELD_REPLY_ACK — duplicates only exist because an
    earlier ACK was lost, and re-acking short-circuits the next
    sensor retry round."""
    db = DB(tmp_path / "h.db")
    db.register_device("aa00bb11cc22dd33", "synth-A", "end_device", "")
    db.set_provision_info(
        device_id="aa00bb11cc22dd33", device_type="synth",
        enc_pubkey_b64="x", gateway_id="gggg0000",
    )
    db.register_gateway("gggg0000", "gw-stub", "x", "")
    srv = _StubServer()
    srv.online_gw.add("gggg0000")
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")

    from cryptography.hazmat.primitives.asymmetric import ec
    priv = ec.generate_private_key(proto.CURVE)
    tid = 0xDEADBEEF

    # Send the same FIELD_REPLY twice, the second as a sensor RETRY (after
    # the burst-copy window) — both arrivals should ACK.
    for i in range(2):
        if i:
            c, t, first = router._burst_first["aa00bb11cc22dd33"]
            router._burst_first["aa00bb11cc22dd33"] = (c, t, first - 1.0)
        inner = (proto.Cmd.FIELD_REPLY.to_bytes(2, "little")
                 + tid.to_bytes(4, "little") + b"dup")
        d2h = proto.encode(proto.FrameType.D2H,
                           bytes.fromhex("aa00bb11cc22dd33"),
                           inner, priv)
        f_in, _ = proto.decode(d2h)
        await router.handle_frame("gggg0000", f_in)

    for _ in range(50):
        if len(srv.sent) >= 2:
            break
        await asyncio.sleep(0.01)
    assert len(srv.sent) >= 2, f"expected 2 FIELD_REPLY_ACK frames, got {len(srv.sent)}"
    for gw_id, frame in srv.sent[:2]:
        f, _ = proto.decode(frame)
        cmd, _ = struct.unpack_from("<HI", f.payload, 0)
        assert cmd == proto.Cmd.FIELD_REPLY_ACK


# ── D2G DEVICE_THREAD_STATE (cmd 0x0006) auto-refresh ml_eid ────────────
#
# Background: every sensor reboot rerolls its Thread ML-EID's random IID.
# The gateway caches each device's source IPv6 from every inbound D2H/D2G
# and periodically replays the cache to us as D2G DEVICE_THREAD_STATE so
# the hub can self-heal each device's ml_eid (previously sticky from
# provisioning time).  See feedback_mleid_drift_breaks_d2d.md.

import ipaddress


def _d2g_device_thread_state(gw_did: bytes, target_did: bytes,
                             ml_eid: str) -> 'proto.Frame':
    """Build a synthetic D2G DEVICE_THREAD_STATE frame for the router."""
    from cryptography.hazmat.primitives.asymmetric import ec
    args = (len(target_did).to_bytes(2, "little")
            + target_did
            + ipaddress.IPv6Address(ml_eid).packed)
    inner = proto.encode_inner(proto.Cmd.DEVICE_THREAD_STATE, args)
    priv = ec.generate_private_key(proto.CURVE)
    raw = proto.encode(proto.FrameType.D2G, gw_did, inner, priv)
    frame, _ = proto.decode(raw)
    return frame


async def test_device_thread_state_updates_ml_eid(tmp_path: Path):
    """Inbound DEVICE_THREAD_STATE with a fresh address triggers an UPDATE."""
    db = DB(tmp_path / "h.db")
    did_hex = "aa00bb11cc22dd33"
    db.register_device(did_hex, "dev", "end_device", "")
    db.set_provision_info(device_id=did_hex, device_type="x",
                          gateway_id="gggg0000",
                          ml_eid="fd00:1::1")
    db.register_gateway("gggg0000", "gw-stub", "x", "")
    srv = _StubServer()
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")

    fresh = "fd44:8b73:6d00:1:e008:e670:2cc7:29bf"
    frame = _d2g_device_thread_state(bytes.fromhex("a1b2c3d400000000"),
                                     bytes.fromhex(did_hex), fresh)
    await router.handle_frame("gggg0000", frame)

    pi = db.get_provision_info(did_hex)
    assert pi.ml_eid == fresh


async def test_device_thread_state_no_op_when_unchanged(tmp_path: Path):
    """If the inbound address matches what we already store, no UPDATE."""
    db = DB(tmp_path / "h.db")
    did_hex = "aa00bb11cc22dd33"
    db.register_device(did_hex, "dev", "end_device", "")
    same = "fd44:8b73:6d00:1:abcd::1"
    db.set_provision_info(device_id=did_hex, device_type="x",
                          gateway_id="gggg0000", ml_eid=same)
    db.register_gateway("gggg0000", "gw-stub", "x", "")
    srv = _StubServer()
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")

    # Patch update_ml_eid so we can assert it was NOT called.
    called: list[tuple[str, str]] = []
    orig = db.update_ml_eid
    db.update_ml_eid = lambda d, a: called.append((d, a)) or orig(d, a)

    frame = _d2g_device_thread_state(bytes.fromhex("a1b2c3d400000000"),
                                     bytes.fromhex(did_hex), same)
    await router.handle_frame("gggg0000", frame)

    assert called == [], "ml_eid was UPDATEd despite no change"


async def test_device_thread_state_unknown_device_dropped(tmp_path: Path):
    """DEVICE_THREAD_STATE for a never-provisioned device is silently ignored."""
    db = DB(tmp_path / "h.db")
    db.register_gateway("gggg0000", "gw-stub", "x", "")
    srv = _StubServer()
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")

    ghost = "deadbeefdeadbeef"
    frame = _d2g_device_thread_state(bytes.fromhex("a1b2c3d400000000"),
                                     bytes.fromhex(ghost),
                                     "fd44::ffff")
    await router.handle_frame("gggg0000", frame)  # must not raise
    assert db.get_provision_info(ghost) is None


async def test_device_thread_state_truncated_payload_rejected(tmp_path: Path):
    """Short inner args don't crash the handler."""
    db = DB(tmp_path / "h.db")
    did_hex = "aa00bb11cc22dd33"
    db.register_device(did_hex, "dev", "end_device", "")
    db.set_provision_info(device_id=did_hex, device_type="x",
                          gateway_id="gggg0000", ml_eid="fd00:1::1")
    db.register_gateway("gggg0000", "gw-stub", "x", "")
    srv = _StubServer()
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")

    # Build a DEVICE_THREAD_STATE frame whose inner declares did_size=8
    # but only carries 4 bytes of did and no ml_eid.
    from cryptography.hazmat.primitives.asymmetric import ec
    args = (8).to_bytes(2, "little") + b"\x01\x02\x03\x04"
    inner = proto.encode_inner(proto.Cmd.DEVICE_THREAD_STATE, args)
    priv = ec.generate_private_key(proto.CURVE)
    gw_did = bytes.fromhex("a1b2c3d400000000")
    raw = proto.encode(proto.FrameType.D2G, gw_did, inner, priv)
    frame, _ = proto.decode(raw)
    await router.handle_frame("gggg0000", frame)  # must not raise

    # The original ml_eid survives.
    assert db.get_provision_info(did_hex).ml_eid == "fd00:1::1"


# ── DB migration: coap_addr → ml_eid ─────────────────────────────────────


async def test_legacy_coap_addr_column_migrates(tmp_path: Path):
    """A pre-rename hub.db has its provision_info.coap_addr column
    renamed to ml_eid by DB._migrate(), preserving row data."""
    import sqlite3
    p = tmp_path / "legacy.db"
    c = sqlite3.connect(str(p))
    c.executescript("""
        CREATE TABLE provision_info (
            device_id TEXT PRIMARY KEY,
            device_type TEXT NOT NULL,
            enc_pubkey_b64 TEXT NOT NULL DEFAULT '',
            mdns_addr TEXT NOT NULL DEFAULT '',
            coap_addr TEXT NOT NULL DEFAULT '',
            gateway_id TEXT NOT NULL DEFAULT '',
            ble_addr TEXT NOT NULL DEFAULT '',
            eui64 TEXT NOT NULL DEFAULT '',
            capabilities TEXT NOT NULL DEFAULT '[]',
            provisioned_at INTEGER NOT NULL,
            last_info_at INTEGER
        );
        CREATE TABLE gateways (
            id TEXT PRIMARY KEY,
            role INTEGER,
            rloc16 INTEGER,
            mleid_hex TEXT,
            last_thread_state_at INTEGER
        );
    """)
    c.execute(
        "INSERT INTO provision_info VALUES "
        "('aa','t','','','fd00:legacy::1','','','','[]',0,NULL)"
    )
    c.commit()
    c.close()

    db = DB(p)
    cols = [r[1] for r in db._conn.execute(
        "PRAGMA table_info(provision_info)")]
    assert "ml_eid" in cols
    assert "coap_addr" not in cols
    pi = db.get_provision_info("aa")
    assert pi is not None
    assert pi.ml_eid == "fd00:legacy::1"


# ── INFO_REPLY device-initiated config sync ─────────────────────────────
#
# After an OTA reboot the device emits an unsolicited INFO_REPLY with
# `firmware` set to its running version.  Hub uses this to flip the OTA
# state-machine from 'applying' to 'done'.  See
# feedback_hub_ota_state_running_version_stale.


async def test_unsolicited_info_reply_updates_running_version(tmp_path: Path):
    db = DB(tmp_path / "h.db")
    did_hex = "aa00bb11cc22dd33"
    db.register_device(did_hex, "dev", "end_device", "")
    db.set_provision_info(device_id=did_hex, device_type="sample_c6",
                          gateway_id="a1b2c3d400000000", ml_eid="fd00:1::1")
    db.register_gateway("a1b2c3d400000000", "gw-stub", "x", "")
    srv = _StubServer()
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")

    # Seed an in-flight OTA state so the test can assert the apply→done
    # transition.
    router._ota_state[did_hex] = {
        "running_version": "",
        "target_version":  "0.0.1",
        "in_flight":       True,
        "highest_block":   1996,
        "total_blocks":    1997,
        "started_at":      0,
        "last_block_at":   0,
    }

    # Build a synthetic D2H INFO_REPLY with the new firmware version.
    from cryptography.hazmat.primitives.asymmetric import ec
    body = b'{"firmware":"0.0.1","uptime_ms":4321,"name":"c6-s3"}'
    inner = (proto.Cmd.INFO_REPLY.to_bytes(2, "little")
             + (0).to_bytes(4, "little")  # tid=0 → unsolicited
             + body)
    priv = ec.generate_private_key(proto.CURVE)
    raw  = proto.encode(proto.FrameType.D2H,
                         bytes.fromhex(did_hex), inner, priv)
    frame, _ = proto.decode(raw)
    await router.handle_frame("a1b2c3d400000000", frame)

    st = router._ota_state[did_hex]
    assert st["running_version"] == "0.0.1"
    assert st["in_flight"] is False, "OTA should auto-complete when running matches target"


async def test_unsolicited_info_reply_strips_tweak_suffix(tmp_path: Path):
    """Zephyr's APP_VERSION_TWEAK_STRING adds `+TWEAK` to the version
    string; the hub stores target as plain semver.  Make sure the
    apply→done flip still fires when firmware='0.0.2+0' and
    target='0.0.2'."""
    db = DB(tmp_path / "h.db")
    did_hex = "aa00bb11cc22dd33"
    db.register_device(did_hex, "dev", "end_device", "")
    db.set_provision_info(device_id=did_hex, device_type="sample_c6",
                          gateway_id="a1b2c3d400000000", ml_eid="fd00:1::1")
    db.register_gateway("a1b2c3d400000000", "gw-stub", "x", "")
    srv = _StubServer()
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")
    router._ota_state[did_hex] = {
        "running_version": "0.0.1+0",
        "target_version":  "0.0.2",
        "in_flight":       True,
    }
    from cryptography.hazmat.primitives.asymmetric import ec
    body = b'{"firmware":"0.0.2+0","uptime_ms":99}'
    inner = (proto.Cmd.INFO_REPLY.to_bytes(2, "little")
             + (0).to_bytes(4, "little")
             + body)
    priv = ec.generate_private_key(proto.CURVE)
    raw  = proto.encode(proto.FrameType.D2H,
                         bytes.fromhex(did_hex), inner, priv)
    frame, _ = proto.decode(raw)
    await router.handle_frame("a1b2c3d400000000", frame)
    st = router._ota_state[did_hex]
    assert st["running_version"] == "0.0.2+0"
    assert st["in_flight"] is False, "tweak suffix should not block apply→done flip"


async def test_info_reply_with_no_firmware_field_is_noop(tmp_path: Path):
    db = DB(tmp_path / "h.db")
    did_hex = "aa00bb11cc22dd33"
    db.register_device(did_hex, "dev", "end_device", "")
    db.set_provision_info(device_id=did_hex, device_type="sample_c6",
                          gateway_id="a1b2c3d400000000", ml_eid="fd00:1::1")
    db.register_gateway("a1b2c3d400000000", "gw-stub", "x", "")
    srv = _StubServer()
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")

    router._ota_state[did_hex] = {
        "running_version": "0.0.1",
        "target_version":  "0.0.2",
        "in_flight":       True,
    }

    # body has no `firmware` field — handler should leave state untouched.
    from cryptography.hazmat.primitives.asymmetric import ec
    body = b'{"name":"c6-s3","uptime_ms":4321}'
    inner = (proto.Cmd.INFO_REPLY.to_bytes(2, "little")
             + (0).to_bytes(4, "little")
             + body)
    priv = ec.generate_private_key(proto.CURVE)
    raw  = proto.encode(proto.FrameType.D2H,
                         bytes.fromhex(did_hex), inner, priv)
    frame, _ = proto.decode(raw)
    await router.handle_frame("a1b2c3d400000000", frame)

    assert router._ota_state[did_hex]["running_version"] == "0.0.1"
    assert router._ota_state[did_hex]["in_flight"] is True


async def test_info_reply_bad_json_does_not_crash(tmp_path: Path):
    db = DB(tmp_path / "h.db")
    did_hex = "aa00bb11cc22dd33"
    db.register_device(did_hex, "dev", "end_device", "")
    db.set_provision_info(device_id=did_hex, device_type="sample_c6",
                          gateway_id="a1b2c3d400000000", ml_eid="fd00:1::1")
    db.register_gateway("a1b2c3d400000000", "gw-stub", "x", "")
    srv = _StubServer()
    router = HubProtoRouter(db, srv,
                            hub_privkey_path=tmp_path / "priv.bin")

    from cryptography.hazmat.primitives.asymmetric import ec
    body = b'not-json-at-all'
    inner = (proto.Cmd.INFO_REPLY.to_bytes(2, "little")
             + (0).to_bytes(4, "little")
             + body)
    priv = ec.generate_private_key(proto.CURVE)
    raw  = proto.encode(proto.FrameType.D2H,
                         bytes.fromhex(did_hex), inner, priv)
    frame, _ = proto.decode(raw)
    await router.handle_frame("a1b2c3d400000000", frame)  # must not raise


# ── OTA chunk checksums + OTA_READY (chunk-diff / armed-apply) ──────────


def _register_dev_with_fw(db, tmp_path, fw_bytes: bytes,
                           did_hex="aa00bb11cc22dd33",
                           version="0.0.5"):
    # devices.type carries the firmware device_type (matches production
    # rows + what _fw_target_for_device resolves first).
    db.register_device(did_hex, "dev", "sample_c6", "")
    db.set_provision_info(device_id=did_hex, device_type="sample_c6",
                          gateway_id="a1b2c3d400000000", ml_eid="fd00:1::1")
    db.register_gateway("a1b2c3d400000000", "gw-stub", "x", "")
    import hashlib
    fw = tmp_path / "fw.bin"
    fw.write_bytes(fw_bytes)
    db.set_firmware_target("sample_c6", version, str(fw),
                            len(fw_bytes),
                            hashlib.sha256(fw_bytes).hexdigest())
    return did_hex, fw


async def test_chunksums_req_serves_correct_page(tmp_path: Path):
    import hashlib
    fw_bytes = bytes(range(256)) * 16  # 4096 B = 8 blocks of 512
    db = DB(tmp_path / "h.db")
    did, fw = _register_dev_with_fw(db, tmp_path, fw_bytes)
    srv = _StubServer()
    srv.online_gw.add("a1b2c3d400000000")
    router = HubProtoRouter(db, srv, hub_privkey_path=tmp_path / "priv.bin")

    await router._on_ota_chunksums_req(did, tid=42,
                                        body=struct.pack("<IH", 0, 8))
    assert srv.sent, "no OTA_CHUNKSUMS reply enqueued"
    _, frame_bytes = srv.sent[-1]
    f, _ = proto.decode(frame_bytes)
    cmd, tid = struct.unpack_from("<HI", f.payload, 0)
    assert cmd == proto.Cmd.OTA_CHUNKSUMS
    assert tid == 42
    body = f.payload[6:]
    first_block = struct.unpack_from("<I", body, 0)[0]
    assert first_block == 0
    sums = body[4:]
    assert len(sums) == 8 * 8  # 8 blocks × 8B
    # spot-check block 3
    blk3 = fw_bytes[3*512:4*512]
    expect = hashlib.sha256(blk3).digest()[:8]
    assert sums[3*8:4*8] == expect
    # sidecar cache created
    assert (tmp_path / "fw.bin.chunksums").is_file()


async def test_chunksums_req_clamps_page_and_bounds(tmp_path: Path):
    fw_bytes = b"\xAB" * (512 * 100)  # 100 blocks
    db = DB(tmp_path / "h.db")
    did, fw = _register_dev_with_fw(db, tmp_path, fw_bytes)
    srv = _StubServer()
    srv.online_gw.add("a1b2c3d400000000")
    router = HubProtoRouter(db, srv, hub_privkey_path=tmp_path / "priv.bin")

    # ask for 1000 starting at 90 → clamp to remaining 10, capped ≤63
    await router._on_ota_chunksums_req(did, tid=1,
                                        body=struct.pack("<IH", 90, 1000))
    _, frame_bytes = srv.sent[-1]
    f, _ = proto.decode(frame_bytes)
    body = f.payload[6:]
    assert struct.unpack_from("<I", body, 0)[0] == 90
    assert len(body[4:]) == 10 * 8

    # past-EOF request: silently dropped (no new send)
    n_before = len(srv.sent)
    await router._on_ota_chunksums_req(did, tid=2,
                                        body=struct.pack("<IH", 100, 1))
    assert len(srv.sent) == n_before


async def test_ota_ready_arms_state_and_watchdog_skips(tmp_path: Path):
    fw_bytes = b"\x01" * 1024
    db = DB(tmp_path / "h.db")
    did, fw = _register_dev_with_fw(db, tmp_path, fw_bytes, version="0.0.6")
    srv = _StubServer()
    router = HubProtoRouter(db, srv, hub_privkey_path=tmp_path / "priv.bin")
    # device finished download, reports ready
    body = b'{"version":"0.0.6","sha256":"5a648d8015900d89664e00e125df179636301a2d8fa191c1aa2bd9358ea53a69"}'
    await router._on_ota_ready(did, tid=0, body=body)

    st = router._ota_state[did]
    assert st["armed"] is True
    assert st["armed_version"] == "0.0.6"
    assert st["in_flight"] is False

    # get_ota_status surfaces 'armed' (running differs from target)
    st["running_version"] = "0.0.5+0"
    st["target_version"]  = "0.0.6"
    snap = router.get_ota_status(did)
    assert snap["state"] == "armed"
    assert snap["armed_version"] == "0.0.6"


async def test_armed_state_clears_to_done_after_apply(tmp_path: Path):
    """After the operator applies and the device reboots into the
    armed version, INFO_REPLY flips running_version; state → done."""
    db = DB(tmp_path / "h.db")
    did, fw = _register_dev_with_fw(db, tmp_path, b"\x01" * 1024,
                                     version="0.0.6")
    srv = _StubServer()
    router = HubProtoRouter(db, srv, hub_privkey_path=tmp_path / "priv.bin")
    await router._on_ota_ready(did, tid=0,
                                body=b'{"version":"0.0.6","sha256":"5a648d8015900d89664e00e125df179636301a2d8fa191c1aa2bd9358ea53a69"}')
    st = router._ota_state[did]
    st["target_version"] = "0.0.6"
    st["running_version"] = "0.0.5+0"
    assert router.get_ota_status(did)["state"] == "armed"

    # post-apply INFO_REPLY arrives with the new version
    router._consume_info_reply(did, b'{"firmware":"0.0.6+0"}')
    snap = router.get_ota_status(did)
    # running now matches target → no longer armed-pending; state=done
    assert snap["running_version"] == "0.0.6+0"
    assert snap["state"] == "done"


async def test_info_reply_armed_field_arms_and_clears(tmp_path: Path):
    """INFO_REPLY's `armed` field is the reliable backstop for OTA_READY:
    non-empty arms the hub-side state, empty clears it."""
    db = DB(tmp_path / "h.db")
    did, fw = _register_dev_with_fw(db, tmp_path, b"\x01" * 1024,
                                     version="0.0.6")
    srv = _StubServer()
    router = HubProtoRouter(db, srv, hub_privkey_path=tmp_path / "priv.bin")

    router._consume_info_reply(
        did, b'{"firmware":"0.0.5+0","armed":"0.0.6"}')
    st = router._ota_state[did]
    assert st["armed"] is True
    assert st["armed_version"] == "0.0.6"

    # post-apply: armed comes back empty + firmware bumped
    router._consume_info_reply(
        did, b'{"firmware":"0.0.6+0","armed":""}')
    assert router._ota_state[did]["armed"] is False


# ── detools binary-delta OTA (OTA_PATCH) ────────────────────────────────


async def test_ota_check_offers_patch_when_from_image_cached(tmp_path: Path):
    """When the device's running-version image is cached, OTA_CHECK reply
    carries a `patch` offer (size+sha+from)."""
    import hashlib, json
    # Two distinct 'images' so detools produces a non-trivial patch.
    from hub.firmware_sources.base import MCUBOOT_IMAGE_MAGIC
    img_a = MCUBOOT_IMAGE_MAGIC + bytes(range(256)) * 40        # 'from' (0.0.1)
    img_b = MCUBOOT_IMAGE_MAGIC + bytes(range(255, -1, -1)) * 40  # 'to' (0.0.2)

    db = DB(tmp_path / "h.db")
    did = "aa00bb11cc22dd33"
    db.register_device(did, "dev", "sample_c6", "")
    db.set_provision_info(device_id=did, device_type="sample_c6",
                          gateway_id="a1b2c3d400000000", ml_eid="fd00:1::1")
    db.register_gateway("a1b2c3d400000000", "gw-stub", "x", "")

    # cache dir layout: <cache>/<dt>/<ver>/image.<sha8>.signed.bin
    cache = tmp_path / "firmware-cache"
    a_path = cache / "sample_c6" / "0.0.1" / "image.aaaa1111.signed.bin"
    a_path.parent.mkdir(parents=True); a_path.write_bytes(img_a)
    b_path = cache / "sample_c6" / "0.0.2" / "image.bbbb2222.signed.bin"
    b_path.parent.mkdir(parents=True); b_path.write_bytes(img_b)

    # catalog row for the from-version, marked cached
    db.upsert_catalog_entry("sample_c6", "0.0.1", "src",
                            "uri", "{}", hashlib.sha256(img_a).hexdigest(),
                            len(img_a))
    db.mark_catalog_downloaded("sample_c6", "0.0.1", "src", str(a_path))
    # active target = 0.0.2
    db.set_firmware_target("sample_c6", "0.0.2", str(b_path),
                            len(img_b), hashlib.sha256(img_b).hexdigest())

    srv = _StubServer()
    srv.online_gw.add("a1b2c3d400000000")
    router = HubProtoRouter(db, srv, hub_privkey_path=tmp_path / "priv.bin")

    body = json.dumps({"type": "sample_c6", "version": "0.0.1+0"}).encode()
    await router._on_ota_check(did, tid=7, body=body)

    # decode the OTA_MANIFEST reply
    _, frame = srv.sent[-1]
    f, _ = proto.decode(frame)
    cmd, tid = struct.unpack_from("<HI", f.payload, 0)
    assert cmd == proto.Cmd.OTA_MANIFEST
    reply = json.loads(f.payload[6:].decode())
    assert reply["update"] is True
    assert "patch" in reply, "no patch offered despite cached from-image"
    # The offer is deliberately COMPACT (da4a2d7): "from" was dropped to shave a
    # 6LoWPAN fragment off the multi-fragment reply that weak-mesh nodes lost.
    assert "from" not in reply["patch"]
    assert reply["patch"]["sha256"]
    assert reply["patch"]["size"] > 0
    assert reply["patch"]["size"] < len(img_b)   # patch smaller than full
    # hub recorded the patch path for serving
    assert router._ota_state[did]["patch_path"]


async def test_ota_patch_req_serves_patch_bytes(tmp_path: Path):
    """After a patch offer, OTA_PATCH_REQ serves bytes from the cached
    sidecar; reconstructing with detools yields the target image."""
    import hashlib, json, detools, io
    from hub.firmware_sources.base import MCUBOOT_IMAGE_MAGIC
    img_a = MCUBOOT_IMAGE_MAGIC + bytes(range(256)) * 40
    img_b = MCUBOOT_IMAGE_MAGIC + bytes(range(255, -1, -1)) * 40

    db = DB(tmp_path / "h.db")
    did = "aa00bb11cc22dd33"
    db.register_device(did, "dev", "sample_c6", "")
    db.set_provision_info(device_id=did, device_type="sample_c6",
                          gateway_id="a1b2c3d400000000", ml_eid="fd00:1::1")
    db.register_gateway("a1b2c3d400000000", "gw-stub", "x", "")
    cache = tmp_path / "firmware-cache"
    a_path = cache / "sample_c6" / "0.0.1" / "image.aaaa1111.signed.bin"
    a_path.parent.mkdir(parents=True); a_path.write_bytes(img_a)
    b_path = cache / "sample_c6" / "0.0.2" / "image.bbbb2222.signed.bin"
    b_path.parent.mkdir(parents=True); b_path.write_bytes(img_b)
    db.upsert_catalog_entry("sample_c6", "0.0.1", "src", "uri", "{}",
                            hashlib.sha256(img_a).hexdigest(), len(img_a))
    db.mark_catalog_downloaded("sample_c6", "0.0.1", "src", str(a_path))
    db.set_firmware_target("sample_c6", "0.0.2", str(b_path),
                            len(img_b), hashlib.sha256(img_b).hexdigest())

    srv = _StubServer()
    srv.online_gw.add("a1b2c3d400000000")
    router = HubProtoRouter(db, srv, hub_privkey_path=tmp_path / "priv.bin")

    await router._on_ota_check(
        did, tid=1, body=json.dumps({"type": "sample_c6",
                                     "version": "0.0.1+0"}).encode())
    patch_size = router._ota_state[did]["patch_size"]

    # device streams the patch in 512B requests
    patch = bytearray()
    off = 0
    while off < patch_size:
        srv.sent.clear()
        await router._on_ota_patch_req(
            did, tid=off, body=struct.pack("<IH", off, 512))
        _, frame = srv.sent[-1]
        f, _ = proto.decode(frame)
        cmd, _t = struct.unpack_from("<HI", f.payload, 0)
        assert cmd == proto.Cmd.OTA_PATCH
        rep_off = struct.unpack_from("<I", f.payload, 6)[0]
        assert rep_off == off
        chunk = f.payload[10:]
        patch += chunk
        off += len(chunk)
    assert len(patch) == patch_size

    # device-side reconstruction (Python detools == the C apply lib)
    recon = io.BytesIO()
    detools.apply_patch(io.BytesIO(img_a), io.BytesIO(bytes(patch)), recon)
    assert recon.getvalue() == img_b


@pytest.mark.asyncio
async def test_field_reply_burst_copy_not_reacked(tmp_path: Path):
    """A copy of the same send arriving inside the burst window (the
    sensor fires each message 3x, 30 ms apart) is dropped, not re-acked."""
    db = DB(tmp_path / "h.db")
    db.register_device("aa00bb11cc22dd33", "synth-A", "end_device", "")
    db.set_provision_info(
        device_id="aa00bb11cc22dd33", device_type="synth",
        enc_pubkey_b64="x", gateway_id="gggg0000",
    )
    db.register_gateway("gggg0000", "gw-stub", "x", "")
    srv = _StubServer()
    srv.online_gw.add("gggg0000")
    router = HubProtoRouter(db, srv, hub_privkey_path=tmp_path / "priv.bin")

    from cryptography.hazmat.primitives.asymmetric import ec
    priv = ec.generate_private_key(proto.CURVE)
    inner = (proto.Cmd.FIELD_REPLY.to_bytes(2, "little")
             + (0xDEADBEEF).to_bytes(4, "little") + b"dup")
    for _ in range(3):
        f_in, _ = proto.decode(proto.encode(proto.FrameType.D2H,
                                            bytes.fromhex("aa00bb11cc22dd33"),
                                            inner, priv))
        await router.handle_frame("gggg0000", f_in)
    await asyncio.sleep(0.1)
    assert len(srv.sent) == 1, f"expected 1 FIELD_REPLY_ACK, got {len(srv.sent)}"
    assert router._rx_counts["aa00bb11cc22dd33"]["hub_burst_copies"] == 2
