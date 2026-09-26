"""Phase 5: hourly camera metrics on the hub.

The collector is fed by an injected fetch (no network): two samples with
moving counters, a session drop, a stalled HLS, then the hour closes and
ONE report holds every camera and the host; the accumulators reset; the
hour in progress survives a collector restart; the API serves it."""
import json
import sys
import time
from pathlib import Path

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hub.db import DB  # noqa: E402
from hub import metrics as M  # noqa: E402

pytestmark = pytest.mark.asyncio


def _health(runs, resets, dropped, rss, uptime, age3=100.0, cam3_connected=True, fails=0):
    conns = {"cam0": {"peer": "a", "age_s": 5000.0, "records": runs * 3, "bytes": runs * 4000}}
    if cam3_connected:
        conns["cam3"] = {"peer": "b", "age_s": age3, "records": runs * 2, "bytes": runs * 3000}
    return {"uptime_s": uptime, "rss_kb": rss, "cpu_pct": 30.0, "malloc_trims": uptime // 60,
            "connections": conns,
            "pipelines": {p: {"queue": {"depth": 0, "dropped": dropped if p == "event" else 0, "avg_wait_ms": 0.7},
                              "cameras": {c: {"runs": runs, "failures_recent": fails if c == "cam3" else 0, "stalls": 0,
                                              "resets": resets if c == "cam0" else 0, "last_latency_ms": 1.5}
                                          for c in ("cam0", "cam3")}}
                          for p in M.PIPELINES}}


class Feed:
    """What the media host answers, per URL, advanced by the test."""
    def __init__(self):
        self.health = _health(100, 0, 0, 250000, 1000)
        self.events = {"cam0": {"yolo_runs": 10, "yolo_ms": 60, "events": 1, "uploads_ok": 1, "uploads_failed": 0},
                       "cam3": {"yolo_runs": 0, "yolo_ms": 0, "events": 0, "uploads_ok": 0, "uploads_failed": 0}}
        self.down = False

    async def __call__(self, url):
        if self.down:
            return 503, None
        if url.endswith("/health"):
            return 200, self.health
        for c in ("cam0", "cam3"):
            if f"/cam/{c}/" in url:
                return 200, self.events[c]
        return 404, None


def _collector(db, feed, hub_view=None):
    c = M.MetricsCollector(db, media_host_url=lambda _db: "http://mh", fetch=feed)
    if hub_view is not None:
        c._hub_view = hub_view
    return c


async def test_deltas_accumulate_and_the_hour_rolls_into_one_report(tmp_path):
    db = DB(tmp_path / "h.db"); feed = Feed()
    views = {"cam0": (True, 0.5), "cam3": (True, 0.5)}
    c = _collector(db, feed, hub_view=lambda cam: views.get(cam, (None, None)))
    t0 = M._hour_floor(time.time()) + 60
    await c.sample(now=t0)                                      # baseline: deltas are the raw values once
    feed.health = _health(160, 1, 5, 260000, 1060, age3=7.0, fails=2)   # cam3 reconnected, cam0 reset, 5 drops, cam3 failing
    feed.events["cam0"] = {"yolo_runs": 25, "yolo_ms": 70, "events": 2, "uploads_ok": 1, "uploads_failed": 1}
    views["cam3"] = (False, 30.0)                                # hub sees cam3's HLS stale
    await c.sample(now=t0 + 60)
    cur = c.current()
    a0, a3 = cur["cameras"]["cam0"], cur["cameras"]["cam3"]
    assert a0["minutes"] == 2 and a0["connected_minutes"] == 2 and a0["session_drops"] == 0
    assert a0["pipelines"]["stream"]["runs"] == 160 and a0["pipelines"]["stream"]["resets"] == 1
    assert a0["inferences"] == 25 and a0["inference_ms_max"] == 70 and a0["events"] == 2 and a0["uploads_failed"] == 1
    assert a3["session_drops"] == 1, "connection age went backwards: a reconnect"
    assert a3["hls_stale_minutes"] == 1 and a3["no_video_minutes"] == 1
    assert a3["pipelines"]["detect"]["failure_minutes"] == 1 and a3["pipelines"]["detect"]["failures_max"] == 2
    h = cur["host"]
    assert h["samples"] == 2 and h["rss_mb_min"] == 244 and h["rss_mb_max"] == 254 and h["queue_drops"]["event"] == 5
    assert h["cpu_pct_avg"] == 30.0 and h["restarts"] == 0
    # the hour ends: a sample in the next hour closes the previous one first
    feed.health = _health(170, 1, 5, 240000, 10, age3=67.0)     # media host restarted (uptime fell)
    await c.sample(now=t0 + 3600)
    reps = db.list_metric_reports()
    assert len(reps) == 1
    rep = reps[0]["report"]
    assert rep["period_start"] == t0 - 60 and rep["reason"] == "hour"
    assert set(rep["cameras"]) == {"cam0", "cam3"} and rep["cameras"]["cam0"]["uptime_pct"] == 100.0
    assert rep["cameras"]["cam3"]["hls_fresh_pct"] == 50.0 and rep["host"]["samples"] == 2
    # and the new hour started fresh, with the restart counted there
    cur = c.current()
    assert cur["period_start"] == t0 + 3600 - 60 - 60 + 60 or cur["period_start"] == M._hour_floor(t0 + 3600)
    assert cur["cameras"]["cam0"]["minutes"] == 1 and cur["host"]["restarts"] == 1
    assert cur["cameras"]["cam0"]["pipelines"]["stream"]["runs"] == 10, "delta from the previous sample, not the raw counter"
    ev = db.recent_events(5)
    assert any(e["type"] == "metrics_report" and "cam0 100.0% up" in e["detail"] for e in ev)


async def test_unreachable_media_host_counts_minutes_and_restart_resumes_the_hour(tmp_path):
    db = DB(tmp_path / "h.db"); feed = Feed()
    c = _collector(db, feed, hub_view=lambda cam: (None, None))
    t0 = M._hour_floor(time.time()) + 120
    await c.sample(now=t0)
    feed.down = True
    await c.sample(now=t0 + 60)
    assert c.host["unreachable_minutes"] == 1 and c.last_error == "/health -> 503"
    feed.down = False
    await c.sample(now=t0 + 120)
    assert c.cams["cam0"]["minutes"] == 2
    # a new collector (hub restart) picks the hour in progress up from the DB
    c2 = _collector(db, feed, hub_view=lambda cam: (None, None))
    assert c2.period_start == c.period_start and c2.cams["cam0"]["minutes"] == 2 and c2.host["unreachable_minutes"] == 1
    await c2.sample(now=t0 + 180)
    assert c2.cams["cam0"]["minutes"] == 3 and c2.cams["cam0"]["pipelines"]["ingest"]["runs"] == 100, "no double count after resume"
    # an operator rollup stores what there is and resets
    rep = c2.rollup(now=t0 + 200, reason="operator")
    assert rep["reason"] == "operator" and rep["cameras"]["cam0"]["minutes"] == 3 and c2.cams == {}
    assert db.get_metric_report(rep["id"])["report"]["host"]["unreachable_minutes"] == 1


@pytest_asyncio.fixture
async def hub(tmp_path):
    from hub import api as hub_api
    from hub.log_store import LogStore
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    db = DB(tmp_path / "h.db"); feed = Feed()
    coll = _collector(db, feed, hub_view=lambda cam: (True, 0.3))
    app = hub_api.make_app(db, LogStore(tmp_path / "logs", rotate_interval_sec=3600), X25519PrivateKey.generate(),
                           auth_token=None, proto_router=None, metrics=coll)
    async with TestClient(TestServer(app)) as cli:
        yield cli, coll, feed, db


async def test_metrics_api_routes(hub):
    cli, coll, feed, db = hub
    await coll.sample()
    d = await (await cli.get("/api/v1/metrics/current")).json()
    assert "cam0" in d["cameras"] and d["media_host"] == "http://mh" and d["host"]["samples"] == 1
    assert (await (await cli.get("/api/v1/metrics/reports")).json())["reports"] == []
    r = await (await cli.post("/api/v1/metrics/rollup")).json()
    assert r["ok"] and r["report"]["reason"] == "operator" and r["report"]["cameras"]["cam0"]["minutes"] == 2
    reps = await (await cli.get("/api/v1/metrics/reports?limit=5")).json()
    assert len(reps["reports"]) == 1 and reps["reports"][0]["report"]["host"]["samples"] == 2
    one = await (await cli.get(f"/api/v1/metrics/reports/{reps['reports'][0]['id']}")).json()
    assert one["report"]["cameras"]["cam3"]["uptime_pct"] == 100.0
    assert (await cli.get("/api/v1/metrics/reports/999")).status == 404
    assert (await (await cli.get("/api/v1/metrics/current")).json())["cameras"] == {}, "reset after the rollup"


async def test_empty_slots_are_not_reported(tmp_path):
    """cam4 exists on the media host as an unregistered slot: only the hub
    heartbeat runs for it and nothing ever connects — it is not a
    provisioned camera and must not appear with 0 % uptime."""
    db = DB(tmp_path / "h.db"); feed = Feed()
    feed.health["pipelines"]["hub"]["cameras"]["cam4"] = {"runs": 12, "failures_recent": 0, "stalls": 0, "resets": 0, "last_latency_ms": 5}
    db.set_setting("cameras_blocked", json.dumps(["cam4"]))
    c = _collector(db, feed, hub_view=lambda cam: (None, None))
    await c.sample(now=M._hour_floor(time.time()) + 60)
    assert set(c.cams) == {"cam0", "cam3"}
    # a camera that was streaming and then disconnected keeps being counted (its downtime is the point)
    feed.health = _health(120, 0, 0, 250000, 1100, cam3_connected=False)
    await c.sample(now=M._hour_floor(time.time()) + 120)
    assert c.cams["cam3"]["minutes"] == 2 and c.cams["cam3"]["connected_minutes"] == 1 and c.cams["cam3"]["session_drops"] == 1
