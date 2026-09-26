"""Phase 1 of nn-video Pipelines: handler, ingest and hub pipelines.

Covers: camera identity on the single ingest port (store lookup by device
key, trial-decrypt against the imported per-slot host keys and learning),
the ingest pipeline's record → AU/AUDIO/STATUS routing with A/V-sync order
and record flags kept, the hub heartbeat with the prefixed URL and the
side-by-side `register` guard, the slots.yaml import, and the file handler.
The GStreamer graph itself (stream pipeline) is covered by the hardware gate
(test_pipeline.py --attach), not here.
"""
import asyncio
import json
import os
import socket
import sys
import threading
import time
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "media-host"))
from nnvideo import Engine, Kind, Pipeline, Registry, Request, Store  # noqa: E402
from nnvideo.camera import FileHandler, Ingress  # noqa: E402
from nnvideo.pipes import HubPipeline, IngestPipeline, NN_REC_STATUS, NN_REC_VIDEO, NN_REC_AUDIO  # noqa: E402
from nnvideo.service import import_slots  # noqa: E402
import nn_sectun  # noqa: E402

pytestmark = pytest.mark.asyncio


class Sink(Pipeline):
    """Collects whatever is routed to it."""
    name = "stream"
    budget_s = 5.0

    def __init__(self, name="stream"):
        super().__init__()
        self.name = name
        self.got: list[Request] = []

    async def run(self, req, cfg):
        self.got.append(req)
        return None


def _record(typ: int, flags: int, seq: int, ts_ms: int, payload: bytes) -> bytes:
    """One record in the C6 framing: [type][flags][seq LE16][ts LE64][len LE32][payload]."""
    import struct
    return struct.pack("<BBHQI", typ, flags, seq, ts_ms, len(payload)) + payload


# ── ingest pipeline ──────────────────────────────────────────────────────────

async def _engine(tmp_path, *pipes):
    store = Store(tmp_path / "s.db")
    eng = Engine(store, exit_fn=lambda c: None)
    for p in pipes:
        eng.add(p)
    await eng.start()
    return store, eng


async def _drain(eng, n=20):
    for _ in range(n):
        await asyncio.sleep(0.02)


async def test_ingest_orders_video_by_device_ts_and_keeps_flags(tmp_path):
    stream, hub = Sink("stream"), Sink("hub")
    store, eng = await _engine(tmp_path, IngestPipeline(), stream, hub)
    store.set_settings("cam", "ingest", {"avsync_window_ms": 50})
    # three video fragments arriving out of order (later ts first), then a
    # much later one that pushes the window past all of them
    for ts, flags, pl in ((110, 0, b"b"), (100, 1, b"a"), (120, 1, b"c"), (1000, 1, b"z")):
        await eng.submit(Request("cam", "ingest", "record",
                                 {"typ": NN_REC_VIDEO, "flags": flags, "seq": 0, "payload": pl},
                                 ts_device=ts / 1000.0))
    await _drain(eng)
    aus = [r for r in stream.got if r.kind == Kind.AU]
    assert [r.data for r in aus] == [b"a", b"b", b"c"], "released in device-ts order; the newest stays windowed"
    assert [r.meta["flags"] for r in aus] == [1, 0, 1], "record flags reach the stream pipeline"
    assert [r.meta["ts_ms"] for r in aus] == [100, 110, 120]
    assert aus[0].ts_device == 0.1
    await eng.stop()


async def test_ingest_routes_status_to_stream_and_hub_and_audio_can_be_off(tmp_path):
    stream, hub = Sink("stream"), Sink("hub")
    store, eng = await _engine(tmp_path, IngestPipeline(), stream, hub)
    store.set_settings("cam", "ingest", {"audio": False})
    doc = {"fw": "1.2.3", "img": "p4", "infer": {"sw": 640}}
    await eng.submit(Request("cam", "ingest", "record",
                             {"typ": NN_REC_STATUS, "flags": 0, "seq": 1, "payload": json.dumps(doc).encode()},
                             ts_device=5.0))
    await eng.submit(Request("cam", "ingest", "record",
                             {"typ": NN_REC_AUDIO, "flags": 0, "seq": 2, "payload": b"aac"}, ts_device=5.0))
    await eng.submit(Request("cam", "ingest", Kind.DISCONNECT, {"peer": "x"}))
    await _drain(eng)
    assert [r.kind for r in stream.got] == [Kind.STATUS, Kind.DISCONNECT]
    assert stream.got[0].data == doc and stream.got[0].meta["dev_ts"] == 5000
    assert [r.kind for r in hub.got] == [Kind.STATUS, Kind.DISCONNECT]
    assert not any(r.kind == Kind.AUDIO for r in stream.got), "audio off drops audio records"
    await eng.stop()


# ── hub pipeline ─────────────────────────────────────────────────────────────

async def _fake_hub():
    posts, puts = [], []
    app = web.Application()

    async def cameras(r):
        posts.append(await r.json()); return web.json_response({"ok": True})

    async def bundle(r):
        puts.append((r.match_info["cam"], await r.json())); return web.json_response({"ok": True})
    app.router.add_post("/api/v1/cameras", cameras)
    app.router.add_put("/api/v1/cameras/{cam}/bundle", bundle)
    srv = TestServer(app)
    await srv.start_server()
    return srv, posts, puts


async def test_hub_tick_registers_prefixed_url_and_register_guard(tmp_path):
    srv, posts, puts = await _fake_hub()
    from nnvideo.pipes import StreamPipeline
    stream = StreamPipeline(Registry())
    hub = HubPipeline("http://127.0.0.1:8880", stream)
    store, eng = await _engine(tmp_path, hub)
    eng.add(stream)  # never fed here: no graph is built
    store.add_camera("cam4", "BeagleY cam4")
    store.set_settings("*", "hub", {"hub_url": str(srv.make_url("")), "register": False})
    await eng.submit(Request("cam4", "hub", Kind.TICK))
    await _drain(eng)
    assert posts == [], "register=false (side-by-side) never touches the hub's slot URL"
    store.set_settings("cam4", "hub", {"register": True})
    await eng.submit(Request("cam4", "hub", Kind.TICK))
    await _drain(eng)
    assert posts and posts[-1]["id"] == "cam4" and posts[-1]["url"] == "http://127.0.0.1:8880/cam/cam4"
    assert posts[-1]["name"] == "BeagleY cam4"
    # firmware identity ride-along from a status record
    await eng.submit(Request("cam4", "hub", Kind.STATUS, {"fw": "2.0", "img": "byai", "wdt_rc": 1}))
    await eng.submit(Request("cam4", "hub", Kind.STATUS, {"fw": "2.0", "img": "byai", "wdt_rc": 1}))
    await _drain(eng)
    assert puts == [("cam4", {"version": "2.0", "device_type": "byai", "wdt_rc": 1})], "reported once per signature"
    await eng.stop(); await srv.close()


# ── identity on the single ingest port ───────────────────────────────────────

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0)); return s.getsockname()[1]


async def test_ingress_identifies_known_device_and_learns_unknown_by_trial_decrypt(tmp_path):
    ingest = Sink("ingest")
    store, eng = await _engine(tmp_path, ingest)
    reg = Registry()
    host_a, host_b = X25519PrivateKey.generate(), X25519PrivateKey.generate()
    for cid, k in (("cam0", host_a), ("cam3", host_b)):
        store.add_camera(cid, cid)
        store.set_host_key(cid, k.public_key().public_bytes_raw(), k.private_bytes_raw())
        store.set_setting(cid, "stream", "host_key_id", cid)
    dev_known, dev_new = X25519PrivateKey.generate(), X25519PrivateKey.generate()
    store.set_device_key("cam0", dev_known.public_key().public_bytes_raw(), "cam0")
    port = _free_port()
    ing = Ingress(eng, store, reg, ports=[port])
    ing.start()

    def connect(dev_priv, server_pub, first_records: list[bytes]):
        c = socket.create_connection(("127.0.0.1", port), timeout=5)
        sess = nn_sectun.SecureSession.connect(c, dev_priv, X25519PublicKey.from_public_bytes(server_pub))
        for rec in first_records:
            sess.send(rec)
        return c

    rec = _record(NN_REC_VIDEO, 1, 1, 42, b"frame")
    # 1. known device key → lookup, no trial decrypt
    c1 = connect(dev_known, host_a.public_key().public_bytes_raw(), [rec])
    await _drain(eng, 25)
    assert "cam0" in ing.handlers and reg.get("cam0")["ingest_port"] == port
    kinds = [(r.kind, r.data.get("payload") if isinstance(r.data, dict) else None) for r in ingest.got]
    assert (Kind.CONNECT, None) in kinds and ("record", b"frame") in kinds
    # 2. unknown device provisioned with cam3's legacy slot key → trial decrypt names cam3, key learned
    ingest.got.clear()
    c2 = connect(dev_new, host_b.public_key().public_bytes_raw(), [rec, rec])
    await _drain(eng, 25)
    assert "cam3" in ing.handlers
    assert store.camera_for_device_pub(dev_new.public_key().public_bytes_raw()) == ("cam3", "cam3")
    payloads = [r.data.get("payload") for r in ingest.got if r.kind == "record"]
    assert payloads == [b"frame", b"frame"], "the first record used for identification is not lost"
    # 3. a device no host key opens is rejected and never registered
    stranger = X25519PrivateKey.generate()
    c3 = socket.create_connection(("127.0.0.1", port), timeout=5)
    nn_sectun.SecureSession.connect(c3, stranger, X25519PrivateKey.generate().public_key()).send(rec)
    await _drain(eng, 25)
    assert set(ing.handlers) == {"cam0", "cam3"}
    # 4. a reconnect of cam0 replaces the previous session
    c4 = connect(dev_known, host_a.public_key().public_bytes_raw(), [rec])
    await _drain(eng, 25)
    assert ing.handlers["cam0"].conn is not c1 and reg.get("cam0") is not None
    for c in (c1, c2, c3, c4):
        c.close()
    ing.stop()
    await eng.stop()


# ── slots.yaml import ────────────────────────────────────────────────────────

def test_import_slots_carries_keys_ports_and_args(tmp_path):
    kd = tmp_path / "keys3"; kd.mkdir()
    priv = X25519PrivateKey.generate()
    (kd / "hub_enc_key.raw").write_bytes(priv.private_bytes_raw())
    y = tmp_path / "slots.yaml"
    y.write_text(f"""
env: {{NN_HLS_PDT_MAX_SKEW: "12"}}
slots:
  - id: cam3
    name: BeagleY Wide NoIR
    stream_port: 8892
    control_port: 8902
    keydir: {kd}
    hls_dir: /dev/shm/nn-hls-cam3
    snapshot_dir: /tmp/snap3
    env: {{NN_HLS_REENCODE: "0"}}
    args: ["--motion-interval-ms", "200", "--motion-decoder", "auto", "--yolo-url", "",
           "--enc-qp", "36", "--yolo-model", "/models/yolox_s.cix"]
""")
    store = Store(tmp_path / "s.db")
    assert import_slots(store, y) == ["cam3"]
    assert store.camera("cam3")["name"] == "BeagleY Wide NoIR"
    cfg = store.settings("cam3", "stream")
    assert cfg["legacy_ingest_port"] == 8892 and cfg["hls_dir"] == "/dev/shm/nn-hls-cam3"
    assert cfg["hls_reencode"] == "0" and cfg["hls_pdt_max_skew"] == "12"
    assert cfg["enc_qp"] == 36 and cfg["motion_decoder"] == "auto"
    ev, det = store.settings("cam3", "event"), store.settings("cam3", "detect")
    assert ev["yolo_model"] == "/models/yolox_s.cix" and ev["yolo_url"] == "", "detector args become event settings"
    assert det["interval_ms"] == 200, "motion cadence becomes a detect setting"
    assert cfg["keydir"] == str(kd) and cfg["host_key_id"] == "cam3"
    pub, raw = store.host_key("cam3")
    assert raw == priv.private_bytes_raw() and pub == priv.public_key().public_bytes_raw()
    assert store.camera_for_host_key("cam3") == "cam3"


# ── file handler ─────────────────────────────────────────────────────────────

async def test_file_handler_emits_fragmented_records_with_last_flag(tmp_path):
    ingest = Sink("ingest")
    store, eng = await _engine(tmp_path, ingest)
    reg = Registry()
    # two access units: SPS+PPS+IDR then a P slice, each > one 4 KB fragment
    au1 = b"\x00\x00\x00\x01\x67" + b"s" * 10 + b"\x00\x00\x00\x01\x68" + b"p" * 4 + b"\x00\x00\x00\x01\x65\x88" + b"i" * 9000
    au2 = b"\x00\x00\x00\x01\x41\x9a" + b"q" * 5000
    f = tmp_path / "x.h264"; f.write_bytes(au1 + au2)
    fh = FileHandler(eng, reg, "camtest", str(f), fps=200.0)
    fh.start()
    await asyncio.sleep(0.3)
    fh.stop()
    recs = [r for r in ingest.got if r.kind == "record"]
    assert len(recs) >= 5
    first = recs[:3]                                   # au1 = 3 fragments (9000+ bytes)
    assert [r.data["flags"] for r in first] == [0x02, 0, 0x04], "VID_START on the first fragment, VID_END on the last"
    assert b"".join(r.data["payload"] for r in first) == au1
    assert recs[3].data["payload"] + recs[4].data["payload"] == au2
    assert (recs[3].data["flags"], recs[4].data["flags"]) == (0x02, 0x04)
    assert reg.get("camtest") and reg.get("camtest")["peer"] == "file:x.h264"
    await eng.stop()


def test_secure_session_send_is_safe_across_threads():
    """Two writers on one session (adapt thread + config push) must never
    interleave a record's encrypt and send: the receiver checks every
    record in nonce order.  Without the lock this failed within a few
    hundred records (cam3's uplink drops, 2026-09-22)."""
    import socket
    import threading
    host = X25519PrivateKey.generate(); dev = X25519PrivateKey.generate()
    a, b = socket.socketpair()
    srv_side = {}
    def accept():
        srv_side["s"] = nn_sectun.SecureSession.accept(a, host)
    t = threading.Thread(target=accept); t.start()
    cli = nn_sectun.SecureSession.connect(b, dev, host.public_key())
    t.join(5)
    srv = srv_side["s"]
    N = 400
    def writer(tag):
        for i in range(N):
            srv.send(bytes([tag]) + i.to_bytes(4, "big") + b"x" * 40)
    ws = [threading.Thread(target=writer, args=(k,)) for k in (1, 2, 3)]
    got = []
    def reader():
        for _ in range(3 * N):
            got.append(cli.recv())
    r = threading.Thread(target=reader); r.start()
    for w in ws: w.start()
    for w in ws: w.join(20)
    r.join(20)
    assert len(got) == 3 * N, "every record authenticated in order"
    for tag in (1, 2, 3):
        seq = [int.from_bytes(m[1:5], "big") for m in got if m[0] == tag]
        assert seq == list(range(N)), f"writer {tag} records arrived whole and in order"
    a.close(); b.close()
