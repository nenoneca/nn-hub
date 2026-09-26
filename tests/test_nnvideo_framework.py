"""Phase 0 of nn-video Pipelines: the framework against fake pipelines.

Covers the decided behaviour: flat FIFO per pipeline with per-camera order
kept across N workers, drop/block/coalesce policies, settings read at the
start of every run and live on the next request, failure counting and
reset, stall detection, the stuck-thread exit path, the volatile registry
and the hub-facing API.
"""
import asyncio
import base64
import sys
import threading
import time
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "media-host"))
from nnvideo import Engine, Kind, Mode, Pipeline, PipelineQueue, Policy, Registry, Request, Store  # noqa: E402
from nnvideo.api import make_app  # noqa: E402

pytestmark = pytest.mark.asyncio


# ── queue ────────────────────────────────────────────────────────────────────

async def test_fifo_keeps_per_camera_order_across_workers():
    q = PipelineQueue("p", policy=Policy.DROP_OLDEST, per_camera_cap=100)
    for i in range(6):
        await q.put(Request("camA", "p", Kind.AU, i))
        await q.put(Request("camB", "p", Kind.AU, i))
    a = await q.take()                      # camA #0 in flight
    b = await q.take()                      # camB #0 in flight (interleaves freely)
    assert (a.camera_id, a.data) == ("camA", 0) and (b.camera_id, b.data) == ("camB", 0)
    # a third worker must NOT get camA #1 or camB #1 while both are in flight
    third = asyncio.create_task(q.take())
    await asyncio.sleep(0.05)
    assert not third.done(), "requests of in-flight cameras are held, not handed to another worker"
    assert q.stats.held >= 1
    await q.release("camA")
    nxt = await third
    assert (nxt.camera_id, nxt.data) == ("camA", 1), "the held request is released in order"
    await q.release("camB"); await q.release("camA")
    seq = []
    while q.depth:
        r = await q.take(); seq.append((r.camera_id, r.data)); await q.release(r.camera_id)
    assert [d for c, d in seq if c == "camA"] == [2, 3, 4, 5]
    assert [d for c, d in seq if c == "camB"] == [1, 2, 3, 4, 5]


async def test_drop_oldest_keeps_the_newest_per_camera_kind():
    q = PipelineQueue("p", policy=Policy.DROP_OLDEST, per_camera_cap=3)
    for i in range(6):
        await q.put(Request("camA", "p", Kind.AU, i))
    await q.put(Request("camA", "p", Kind.AUDIO, "x"))      # other kind: its own cap
    assert q.stats.dropped == 3 and q.depth == 4
    got = []
    while q.depth:
        r = await q.take(); got.append(r.data); await q.release("camA")
    assert got == [3, 4, 5, "x"]


async def test_coalesce_keeps_only_the_latest():
    q = PipelineQueue("ctl", policy=Policy.COALESCE)
    for v in (1, 2, 3):
        await q.put(Request("camA", "ctl", Kind.COMMAND, {"v": v}))
    await q.put(Request("camB", "ctl", Kind.COMMAND, {"v": 9}))
    assert q.depth == 2 and q.stats.coalesced == 2
    r = await q.take(); assert r.data == {"v": 3}


async def test_block_policy_applies_backpressure():
    q = PipelineQueue("ev", policy=Policy.BLOCK, per_camera_cap=2)
    await q.put(Request("camA", "ev", Kind.UPLOAD, 1))
    await q.put(Request("camA", "ev", Kind.UPLOAD, 2))
    blocked = asyncio.create_task(q.put(Request("camA", "ev", Kind.UPLOAD, 3)))
    await asyncio.sleep(0.05)
    assert not blocked.done() and q.stats.dropped == 0
    r = await q.take()
    await asyncio.sleep(0.02)
    assert blocked.done() and q.depth == 2
    await q.release("camA")


async def test_purge_drops_a_cameras_requests_only():
    q = PipelineQueue("p")
    for c in ("camA", "camB", "camA"):
        await q.put(Request(c, "p", Kind.AU))
    assert await q.purge("camA") == 2 and q.depth == 1


# ── settings ─────────────────────────────────────────────────────────────────

def test_settings_read_at_run_start_are_cached_by_version(tmp_path):
    from nnvideo.settings import Settings
    st = Store(tmp_path / "m.db")
    s = Settings(st); s.register_defaults("detect", {"interval_ms": 200, "enabled": True})
    st.add_camera("cam3", "BeagleY")
    a = s.get("cam3", "detect"); b = s.get("cam3", "detect")
    assert a == {"interval_ms": 200, "enabled": True} and a is b and s.reloads == 1
    st.set_setting("cam3", "detect", "interval_ms", 500)          # the hub edits
    c = s.get("cam3", "detect")
    assert c["interval_ms"] == 500 and s.reloads == 2, "live on the next read"
    st.set_setting("*", "detect", "enabled", False)                 # global row
    assert s.get("cam3", "detect")["enabled"] is False
    st.set_setting("cam3", "detect", "enabled", True)               # camera wins over global
    assert s.get("cam3", "detect")["enabled"] is True
    # survives a restart
    st2 = Store(tmp_path / "m.db"); s2 = Settings(st2); s2.register_defaults("detect", {"interval_ms": 200})
    assert s2.get("cam3", "detect")["interval_ms"] == 500 and st2.version == st.version


def test_store_keys_and_cameras(tmp_path):
    st = Store(tmp_path / "m.db")
    st.add_camera("cam3", "B")
    st.set_device_key("cam3", b"\x07" * 32, "slot3")
    assert st.camera_for_device_pub(b"\x07" * 32) == ("cam3", "slot3")
    assert st.camera_for_device_pub(b"\x08" * 32) is None
    st.set_host_key("slot3", b"P" * 32, b"S" * 32)
    assert st.host_key("slot3") == (b"P" * 32, b"S" * 32) and st.host_key_ids() == ["slot3"]
    assert st.remove_camera("cam3") and st.camera_for_device_pub(b"\x07" * 32) is None
    assert st.incr_runtime("camX", "detect", "resets") == 1 and st.runtime("camX") == {"detect": {"resets": 1}}


# ── engine ───────────────────────────────────────────────────────────────────

class Echo(Pipeline):
    name = "echo"; budget_s = 0.5; workers = 2
    defaults = {"multiply": 2}

    def __init__(self):
        super().__init__(); self.seen = []; self.resets = []

    async def run(self, req, cfg):
        if req.data == "boom":
            raise ValueError("boom")
        if req.data == "slow":
            await asyncio.sleep(2)
        self.seen.append((req.camera_id, req.data, cfg["multiply"]))
        return [req.emit("sink", Kind.MOTION, req.data * cfg["multiply"] if isinstance(req.data, int) else None)]

    def reset(self, camera_id):
        self.resets.append(camera_id)


class Sink(Pipeline):
    name = "sink"
    def __init__(self):
        super().__init__(); self.got = []
    async def run(self, req, cfg):
        self.got.append((req.camera_id, req.data)); return None


class Crunch(Pipeline):
    name = "crunch"; mode = Mode.THREAD; budget_s = 0.3
    def __init__(self):
        super().__init__(); self.block = threading.Event(); self.resets = []
    def run_sync(self, req, cfg):
        if req.data == "stick":
            self.block.wait(5)
        return None
    def reset(self, camera_id):
        self.resets.append(camera_id)


async def _engine(tmp_path, *pipes, **kw):
    st = Store(tmp_path / "m.db")
    eng = Engine(st, **kw)
    for p in pipes:
        eng.add(p)
    await eng.start()
    return eng


async def _until(cond, timeout=3.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        await asyncio.sleep(0.02)
    return False


async def test_engine_routes_and_reads_settings_each_run(tmp_path):
    echo, sink = Echo(), Sink()
    eng = await _engine(tmp_path, echo, sink)
    try:
        await eng.submit(Request("cam3", "echo", Kind.AU, 5))
        assert await _until(lambda: sink.got == [("cam3", 10)])
        eng.store.set_setting("cam3", "echo", "multiply", 3)     # the hub edits mid-flight
        await eng.submit(Request("cam3", "echo", Kind.AU, 5))
        assert await _until(lambda: sink.got == [("cam3", 10), ("cam3", 15)])
        eng.store.set_setting("cam3", "echo", "enabled", False)
        await eng.submit(Request("cam3", "echo", Kind.AU, 5))
        await asyncio.sleep(0.2)
        assert len(sink.got) == 2, "a disabled pipeline runs nothing for that camera"
        h = eng.health()
        assert h["pipelines"]["echo"]["workers"] == 2 and h["pipelines"]["echo"]["cameras"]["cam3"]["runs"] == 2
    finally:
        await eng.stop()


async def test_repeated_failures_reset_one_camera_only(tmp_path):
    echo, sink = Echo(), Sink()
    echo.failures_before_reset = 3
    eng = await _engine(tmp_path, echo, sink)
    try:
        for _ in range(3):
            await eng.submit(Request("camA", "echo", Kind.AU, "boom"))
        await eng.submit(Request("camB", "echo", Kind.AU, 1))
        assert await _until(lambda: echo.resets == ["camA"] and sink.got == [("camB", 2)])
        st = eng.health()["pipelines"]["echo"]["cameras"]
        assert st["camA"]["resets"] == 1 and "boom" in st["camA"]["last_error"]
        assert "camB" not in st or st["camB"]["resets"] == 0
        assert eng.store.runtime("camA") == {"echo": {"resets": 1}}
    finally:
        await eng.stop()


async def test_async_run_over_budget_is_cancelled_and_counted(tmp_path):
    echo, sink = Echo(), Sink()
    echo.budget_s = 0.1
    eng = await _engine(tmp_path, echo, sink)
    try:
        await eng.submit(Request("camA", "echo", Kind.AU, "slow"))
        assert await _until(lambda: eng.health()["pipelines"]["echo"]["cameras"].get("camA", {}).get("stalls") == 1)
        await eng.submit(Request("camA", "echo", Kind.AU, 4))
        assert await _until(lambda: sink.got == [("camA", 8)]), "the worker is free again after the cancel"
    finally:
        await eng.stop()


async def test_thread_run_over_budget_marks_stalled_then_resets_on_return(tmp_path):
    crunch = Crunch()
    eng = await _engine(tmp_path, crunch, pool_workers=2)
    try:
        await eng.submit(Request("camA", "crunch", Kind.FRAME, "stick"))
        assert await _until(lambda: eng.health()["pipelines"]["crunch"]["cameras"].get("camA", {}).get("stalled") is True)
        crunch.block.set()                                       # the thread finally returns
        assert await _until(lambda: crunch.resets == ["camA"])
        assert eng.health()["pipelines"]["crunch"]["cameras"]["camA"]["stalled"] is False
    finally:
        await eng.stop()


async def test_stuck_thread_exits_the_process(tmp_path):
    exits = []
    crunch = Crunch()
    eng = await _engine(tmp_path, crunch, pool_workers=1, stall_exit_s=0.4, exit_fn=exits.append)
    try:
        await eng.submit(Request("camA", "crunch", Kind.FRAME, "stick"))
        assert await _until(lambda: exits == [46], timeout=4.0), "stuck beyond stall_exit_s -> exit for systemd"
        assert any("stuck" in e for e in eng.events)
    finally:
        crunch.block.set()
        await eng.stop()


async def test_worker_count_changes_live(tmp_path):
    echo, sink = Echo(), Sink()
    eng = await _engine(tmp_path, echo, sink)
    try:
        assert len(eng.workers["echo"]) == 2
        assert await eng.set_workers("echo", 4) == 4 and len(eng.workers["echo"]) == 4
        assert eng.settings.get("*", "echo")["workers"] == 4, "persisted as a global setting"
        await eng.set_workers("echo", 1)
        await asyncio.sleep(0.05)
        assert len(eng.workers["echo"]) == 1
        await eng.submit(Request("camA", "echo", Kind.AU, 1))
        assert await _until(lambda: sink.got == [("camA", 2)])
    finally:
        await eng.stop()


# ── registry ─────────────────────────────────────────────────────────────────

def test_registry_is_volatile_and_allocates_ports():
    reg = Registry(ephemeral_range=(45000, 45003))
    reg.connect("cam3", peer="192.0.2.44:51234")
    p = reg.allocate_port("cam3", "hls_feed")
    assert 45000 <= p <= 45003 and reg.get("cam3")["ports"] == {"hls_feed": p}
    reg.touch("cam3", 100); reg.touch("cam3", 50)
    assert reg.get("cam3")["records"] == 2 and reg.get("cam3")["bytes"] == 150
    reg.connect("cam3", peer="192.0.2.44:51999")          # reconnect: fresh facts, the graph's ports stay
    assert reg.get("cam3")["records"] == 0 and reg.get("cam3")["ports"] == {"hls_feed": p} and p in reg.ports()
    reg.disconnect("cam3")
    assert reg.get("cam3") is None and reg.connected() == [] and p in reg.ports(), "ports outlive the connection"
    reg.forget("cam3")
    assert reg.ports() == {}, "removing the camera releases them"


# ── api ──────────────────────────────────────────────────────────────────────

async def test_api_the_hub_manages_everything(tmp_path):
    echo, sink = Echo(), Sink()
    eng = await _engine(tmp_path, echo, sink)
    reg = Registry()
    try:
        async with TestClient(TestServer(make_app(eng, reg))) as cli:
            r = await cli.post("/cameras", json={"id": "cam3", "name": "BeagleY",
                                                 "device_pub_b64": base64.b64encode(b"\x07" * 32).decode(),
                                                 "settings": {"echo": {"multiply": 7}}})
            assert r.status == 201
            assert eng.store.camera_for_device_pub(b"\x07" * 32) == ("cam3", "default")
            d = await (await cli.get("/cameras/cam3/settings/echo")).json()
            assert d["effective"]["multiply"] == 7 and d["stored"]["multiply"]["scope"] == "camera"
            r = await cli.put("/cameras/cam3/settings/echo", json={"multiply": 9})
            assert (await r.json())["effective"]["multiply"] == 9
            await eng.submit(Request("cam3", "echo", Kind.AU, 1))
            assert await _until(lambda: sink.got == [("cam3", 9)]), "the API write is live on the next run"
            r = await cli.put("/pipelines/echo", json={"workers": 3})
            assert (await r.json())["workers"] == 3
            reg.connect("cam3", peer="10.0.0.9:1")
            d = await (await cli.get("/cameras/cam3/runtime")).json()
            assert d["connection"]["peer"] == "10.0.0.9:1"
            h = await (await cli.get("/health")).json()
            assert h["service"] == "nn-video" and "cam3" in h["connections"]
            r = await cli.post("/cameras", json={"id": "cam3"})
            assert r.status == 409
            r = await cli.delete("/cameras/cam3")
            assert r.status == 200 and eng.store.camera("cam3") is None and reg.get("cam3") is None
    finally:
        await eng.stop()


async def test_api_operator_reset_rebuilds_one_camera_one_pipeline(tmp_path):
    """POST /cameras/{id}/reset/{pipeline} takes the same path as a failure
    storm: purge that camera's queue, call the pipeline's reset, count it."""
    resets = []

    class P(Pipeline):
        name = "p"

        async def run(self, req, cfg):
            await asyncio.sleep(10)

        def reset(self, camera_id):
            resets.append(camera_id)
    store = Store(tmp_path / "s.db")
    eng = Engine(store, exit_fn=lambda c: None)
    eng.add(P()); await eng.start()
    store.add_camera("a", "a"); store.add_camera("b", "b")
    for _ in range(3):
        await eng.submit(Request("a", "p", Kind.AU, b"x")); await eng.submit(Request("b", "p", Kind.AU, b"x"))
    await asyncio.sleep(0.05)
    reg = Registry()
    async with TestClient(TestServer(make_app(eng, reg))) as c:
        r = await c.post("/cameras/a/reset/p")
        assert r.status == 200 and (await r.json()) == {"ok": True, "camera": "a", "pipeline": "p", "resets": 1}
        assert resets == ["a"], "only the asked camera is reset"
        assert eng.queues["p"].snapshot()["per_camera"].get("a") in (None, 0)
        assert eng.queues["p"].snapshot()["per_camera"].get("b"), "the other camera's queue is untouched"
        assert (await c.post("/cameras/zz/reset/p")).status == 404
        assert (await c.post("/cameras/a/reset/nope")).status == 404
    await eng.stop()


def test_registry_free_port_returns_one_port_early():
    reg = Registry(ephemeral_range=(45000, 45003))
    reg.connect("cam", peer="p", ingest_port=1)
    a = reg.allocate_port("cam", "hls_feed"); b = reg.allocate_port("cam", "control")
    assert reg.get("cam")["ports"] == {"hls_feed": a, "control": b}
    reg.free_port(a)
    assert a not in reg.ports() and reg.get("cam")["ports"] == {"control": b}
    c = reg.allocate_port("cam", "hls_feed")          # a rebuilt graph gets a fresh one
    assert c != b and reg.get("cam")["ports"]["hls_feed"] == c
    reg.free_port(99999)                              # unknown port: no-op
    reg.forget("cam")
    assert reg.ports() == {}


async def test_api_debug_memory_routes(tmp_path):
    store = Store(tmp_path / "s.db")
    eng = Engine(store, exit_fn=lambda c: None, malloc_trim_s=0)
    await eng.start()
    async with TestClient(TestServer(make_app(eng, Registry()))) as c:
        d = await (await c.get("/debug/objects")).json()
        assert d["rss_kb"] > 0 and d["gc_tracked_objects"] > 100 and d["tracemalloc"] is False
        assert (await c.post("/debug/tracemalloc", json={"on": True})).status == 400, "refused without i_know"
        assert (await (await c.post("/debug/tracemalloc", json={"on": True, "i_know": True})).json())["tracing"] is True
        keep = [bytes(200_000) for _ in range(5)]                 # 1 MB the tracer must see
        d = await (await c.get("/debug/objects")).json()
        assert d["tracemalloc"] is True and d["python_allocated_mb"] >= 0.9 and d["top_sites"]
        del keep
        assert (await (await c.post("/debug/tracemalloc", json={"on": False})).json())["tracing"] is False
        t = await (await c.post("/debug/malloc_trim")).json()
        assert t["rc"] in (0, 1) and t["rss_before_kb"] > 0
        h = await (await c.get("/health")).json()
        assert "rss_kb" in h and "malloc_trims" in h
    await eng.stop()


async def test_provinfo_reports_the_default_host_key_and_ingest_port(tmp_path):
    from nnvideo.service import ensure_default_host_key
    store = Store(tmp_path / "s.db")
    pub = ensure_default_host_key(store)
    assert len(pub) == 32 and ensure_default_host_key(store) == pub, "generated once, stable"
    eng = Engine(store, exit_fn=lambda c: None, malloc_trim_s=0)
    await eng.start()
    store.add_camera("camA", "A")
    async with TestClient(TestServer(make_app(eng, Registry(), provinfo={"ingest_port": 8886, "ingest_aliases": [8888],
                                                                         "api_url": "http://127.0.0.1:8880"}))) as c:
        d = await (await c.get("/provinfo")).json()
        assert d["ingest_port"] == 8886 and d["ingest_aliases"] == [8888] and d["host_key_id"] == "default"
        assert base64.b64decode(d["host_pub_b64"]) == pub and d["host_pub_hex"] == pub.hex() and d["cameras"] == ["camA"]
    await eng.stop()
