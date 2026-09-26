"""Phase 5 of nn-video pipelines: hourly camera metrics on the hub.

Requested 2026-09-22: "each camera provisioned has its own metrics; each hour
these metrics aggregate into one report and the metrics reset."

Every `interval_s` the collector reads the media host once (`/health`: every
pipeline's per-camera state, queues, connections, process figures) and each
camera's event status, turns the counters into deltas since the previous
sample, and accumulates them per camera for the current wall-clock hour.
At the top of the hour the accumulators become ONE report (all cameras +
the host), stored in the hub DB, and the accumulators reset.  The current
hour is persisted as a setting after every sample so a hub restart resumes
it instead of losing it.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Optional

log = logging.getLogger("nn-hub.metrics")

PIPELINES = ("ingest", "stream", "detect", "event", "control", "hub")
HLS_STALE_S = 8.0
REPORT_KEEP = 24 * 30                    # hourly reports kept: 30 days
CURRENT_KEY = "metrics_current"          # setting: the hour in progress


def _hour_floor(t: float) -> int:
    return int(t // 3600) * 3600


def _delta(cur: Optional[float], prev: Optional[float]) -> float:
    """Counter delta; a counter that went backwards (reconnect, restart)
    contributes its new value."""
    if cur is None:
        return 0.0
    if prev is None or cur < prev:
        return float(cur)
    return float(cur - prev)


def _new_cam() -> dict:
    return {"minutes": 0, "connected_minutes": 0, "session_drops": 0,
            "hls_stale_minutes": 0, "no_video_minutes": 0,
            "records": 0, "bytes": 0,
            "inferences": 0, "inference_ms_max": 0, "events": 0, "uploads_ok": 0, "uploads_failed": 0,
            "pipelines": {p: {"runs": 0, "failure_minutes": 0, "failures_max": 0, "stalls": 0, "resets": 0,
                              "latency_ms_max": 0.0} for p in PIPELINES}}


def _new_host() -> dict:
    return {"samples": 0, "rss_kb_min": None, "rss_kb_max": 0, "rss_kb_sum": 0,
            "cpu_pct_sum": 0.0, "cpu_pct_max": 0.0, "cpu_samples": 0,
            "restarts": 0, "unreachable_minutes": 0, "malloc_trims": 0,
            "queue_drops": {p: 0 for p in PIPELINES}, "queue_wait_ms_max": {p: 0.0 for p in PIPELINES}}


class MetricsCollector:
    def __init__(self, db, *, interval_s: float = 60.0, media_host_url=None, fetch=None):
        self.db = db
        self.interval_s = interval_s
        self._media_host_url = media_host_url        # callable(db) -> str, or None for the default
        self._fetch = fetch                          # async (url) -> (status, json) — injectable for tests
        self.period_start = _hour_floor(time.time())
        self.cams: dict[str, dict] = {}
        self.host = _new_host()
        self._prev: dict[str, dict] = {}             # cam -> last raw sample
        self._prev_host: dict = {}
        self.last_sample_at = 0.0
        self.last_error = ""
        self.reports_made = 0
        self._restore()

    # ── persistence of the hour in progress ─────────────────────────────
    def _restore(self) -> None:
        try:
            raw = self.db.get_setting(CURRENT_KEY)
            doc = json.loads(raw) if raw else None
        except Exception:
            doc = None
        if doc and doc.get("period_start") == self.period_start:
            self.cams = doc.get("cams") or {}
            self.host = {**_new_host(), **(doc.get("host") or {})}
            self._prev = doc.get("prev") or {}
            self._prev_host = doc.get("prev_host") or {}

    def _persist(self) -> None:
        try:
            self.db.set_setting(CURRENT_KEY, json.dumps({
                "period_start": self.period_start, "cams": self.cams, "host": self.host,
                "prev": self._prev, "prev_host": self._prev_host, "at": int(time.time())}))
        except Exception as e:                       # noqa: BLE001
            log.warning("metrics: persist failed: %s", e)

    # ── media host access ────────────────────────────────────────────────
    def _url(self) -> str:
        from . import media_host as _mh
        if self._media_host_url is not None:
            return self._media_host_url(self.db)
        return _mh.media_host_url(self.db)

    async def _get(self, path: str) -> tuple[int, Any]:
        url = self._url() + path
        if self._fetch is not None:
            return await self._fetch(url)
        from aiohttp import ClientSession, ClientTimeout
        async with ClientSession(timeout=ClientTimeout(total=8)) as cs:
            async with cs.get(url) as r:
                try:
                    return r.status, await r.json()
                except Exception:
                    return r.status, None

    def _hub_view(self, cam: str) -> tuple[Optional[bool], Optional[float]]:
        """(streaming, hls_age) as the hub's own flow sampler sees it."""
        try:
            from . import api as _api
            return _api._video_flow_state(cam)
        except Exception:
            return None, None

    # ── one sample ───────────────────────────────────────────────────────
    async def sample(self, now: Optional[float] = None) -> None:
        now = now or time.time()
        if _hour_floor(now) != self.period_start:
            self.rollup(now)
        st, health = await self._get("/health")
        if st != 200 or not isinstance(health, dict):
            self.host["unreachable_minutes"] += 1
            self.last_error = f"/health -> {st}"
            self._persist()
            return
        self.last_error = ""
        self.last_sample_at = now
        h = self.host
        # host figures
        h["samples"] += 1
        rss = int(health.get("rss_kb") or 0)
        if rss:
            h["rss_kb_min"] = rss if h["rss_kb_min"] is None else min(h["rss_kb_min"], rss)
            h["rss_kb_max"] = max(h["rss_kb_max"], rss); h["rss_kb_sum"] += rss
        cpu = health.get("cpu_pct")
        if isinstance(cpu, (int, float)) and cpu > 0:
            h["cpu_pct_sum"] += float(cpu); h["cpu_pct_max"] = max(h["cpu_pct_max"], float(cpu)); h["cpu_samples"] += 1
        up = health.get("uptime_s")
        if isinstance(up, (int, float)) and self._prev_host.get("uptime_s") is not None and up < self._prev_host["uptime_s"]:
            h["restarts"] += 1
        h["malloc_trims"] += int(_delta(health.get("malloc_trims"), self._prev_host.get("malloc_trims")))
        pipes = health.get("pipelines") or {}
        prev_q = self._prev_host.get("queues") or {}
        queues = {}
        for p in PIPELINES:
            q = (pipes.get(p) or {}).get("queue") or {}
            queues[p] = q.get("dropped", 0)
            h["queue_drops"][p] += int(_delta(q.get("dropped"), prev_q.get(p)))
            h["queue_wait_ms_max"][p] = max(h["queue_wait_ms_max"].get(p, 0.0), float(q.get("avg_wait_ms") or 0))
        self._prev_host = {"uptime_s": up, "malloc_trims": health.get("malloc_trims"), "queues": queues}
        # cameras: every camera the media host knows plus every one connected
        cam_ids = set(health.get("connections") or {})
        for p in pipes.values():
            cam_ids |= set((p.get("cameras") or {}).keys())
        cam_ids.discard("*")
        # "each camera provisioned has its own metrics": a slot with no
        # camera (unregistered/free on the hub, never connected, nothing but
        # the hub heartbeat running for it) is not a camera — leave it out
        # rather than report 0 % uptime for an empty slot
        blocked = self._blocked()
        for cam in sorted(cam_ids):
            connected = cam in (health.get("connections") or {})
            active = any(((pipes.get(p) or {}).get("cameras") or {}).get(cam, {}).get("runs")
                         for p in PIPELINES if p != "hub")
            if not connected and not active and (cam in blocked or cam not in self.cams):
                continue
            await self._sample_camera(cam, health, pipes)
        self._persist()

    def _blocked(self) -> set:
        try:
            return set(json.loads(self.db.get_setting("cameras_blocked") or "[]"))
        except Exception:
            return set()

    async def _sample_camera(self, cam: str, health: dict, pipes: dict) -> None:
        acc = self.cams.setdefault(cam, _new_cam())
        prev = self._prev.get(cam) or {}
        raw: dict = {}
        acc["minutes"] += 1
        conn = (health.get("connections") or {}).get(cam)
        if conn:
            acc["connected_minutes"] += 1
            age = float(conn.get("age_s") or 0)
            if prev.get("age_s") is not None and age < prev["age_s"]:
                acc["session_drops"] += 1
            acc["records"] += int(_delta(conn.get("records"), prev.get("records")))
            acc["bytes"] += int(_delta(conn.get("bytes"), prev.get("bytes")))
            raw.update({"age_s": age, "records": conn.get("records"), "bytes": conn.get("bytes")})
        elif prev.get("age_s") is not None:
            acc["session_drops"] += 1           # was connected at the previous sample, gone now
        for p in PIPELINES:
            stp = ((pipes.get(p) or {}).get("cameras") or {}).get(cam)
            if not stp:
                continue
            a = acc["pipelines"][p]
            a["runs"] += int(_delta(stp.get("runs"), (prev.get("p") or {}).get(p, {}).get("runs")))
            a["stalls"] += int(_delta(stp.get("stalls"), (prev.get("p") or {}).get(p, {}).get("stalls")))
            a["resets"] += int(_delta(stp.get("resets"), (prev.get("p") or {}).get(p, {}).get("resets")))
            fr = int(stp.get("failures_recent") or 0)
            if fr:
                a["failure_minutes"] += 1; a["failures_max"] = max(a["failures_max"], fr)
            a["latency_ms_max"] = max(a["latency_ms_max"], float(stp.get("last_latency_ms") or 0))
            raw.setdefault("p", {})[p] = {"runs": stp.get("runs"), "stalls": stp.get("stalls"), "resets": stp.get("resets")}
        # the camera's own event status (inference, events, uploads)
        st, ev = await self._get(f"/cam/{cam}/api/event/status")
        if st == 200 and isinstance(ev, dict):
            acc["inferences"] += int(_delta(ev.get("yolo_runs"), prev.get("yolo_runs")))
            acc["inference_ms_max"] = max(acc["inference_ms_max"], int(ev.get("yolo_ms") or 0))
            acc["events"] += int(_delta(ev.get("events"), prev.get("events")))
            acc["uploads_ok"] += int(_delta(ev.get("uploads_ok"), prev.get("uploads_ok")))
            acc["uploads_failed"] += int(_delta(ev.get("uploads_failed"), prev.get("uploads_failed")))
            raw.update({"yolo_runs": ev.get("yolo_runs"), "events": ev.get("events"),
                        "uploads_ok": ev.get("uploads_ok"), "uploads_failed": ev.get("uploads_failed")})
        # the hub's own view of the video
        streaming, hls_age = self._hub_view(cam)
        if streaming is False:
            acc["no_video_minutes"] += 1
        if hls_age is not None and hls_age > HLS_STALE_S:
            acc["hls_stale_minutes"] += 1
        self._prev[cam] = raw

    # ── the hourly report ────────────────────────────────────────────────
    def current(self) -> dict:
        return {"period_start": self.period_start, "period_end": self.period_start + 3600,
                "now": int(time.time()), "last_sample_at": int(self.last_sample_at), "last_error": self.last_error,
                "media_host": self._url(), "cameras": self.cams, "host": self._host_summary()}

    def _host_summary(self) -> dict:
        h = self.host
        n = max(h["samples"], 1)
        return {"samples": h["samples"], "unreachable_minutes": h["unreachable_minutes"], "restarts": h["restarts"],
                "rss_mb_min": round((h["rss_kb_min"] or 0) / 1024), "rss_mb_max": round(h["rss_kb_max"] / 1024),
                "rss_mb_avg": round(h["rss_kb_sum"] / n / 1024),
                "cpu_pct_avg": round(h["cpu_pct_sum"] / max(h["cpu_samples"], 1), 1), "cpu_pct_max": h["cpu_pct_max"],
                "malloc_trims": h["malloc_trims"], "queue_drops": h["queue_drops"], "queue_wait_ms_max": h["queue_wait_ms_max"]}

    def rollup(self, now: Optional[float] = None, reason: str = "hour") -> Optional[dict]:
        """One report for the period just ended, stored; accumulators reset."""
        now = now or time.time()
        if not self.host["samples"] and not self.cams:
            self.period_start = _hour_floor(now)
            self._persist()
            return None
        report = {"period_start": self.period_start, "period_end": min(self.period_start + 3600, int(now)),
                  "made_at": int(now), "reason": reason, "media_host": self._url(),
                  "cameras": self.cams, "host": self._host_summary()}
        for cam, acc in report["cameras"].items():
            acc["uptime_pct"] = round(100.0 * acc["connected_minutes"] / max(acc["minutes"], 1), 1)
            acc["hls_fresh_pct"] = round(100.0 * (acc["minutes"] - acc["hls_stale_minutes"]) / max(acc["minutes"], 1), 1)
        try:
            rid = self.db.add_metric_report(report["period_start"], report["period_end"], json.dumps(report))
            report["id"] = rid
            self.db.prune_metric_reports(REPORT_KEEP)
            self.db.log_event(None, "metrics_report",
                              f"{time.strftime('%Y-%m-%d %H:%M', time.localtime(report['period_start']))}: "
                              + ", ".join(f"{c} {a['uptime_pct']}% up, {a['inferences']} inf, {a['events']} ev"
                                          for c, a in sorted(report["cameras"].items())))
        except Exception as e:                       # noqa: BLE001
            log.warning("metrics: report not stored: %s", e)
        self.reports_made += 1
        self.cams, self.host = {}, _new_host()
        self.period_start = _hour_floor(now)
        self._persist()
        return report

    async def run(self) -> None:
        while True:
            try:
                await self.sample()
            except Exception as e:                   # noqa: BLE001
                self.last_error = str(e)[:200]
                log.warning("metrics: sample failed: %s", e)
            # next sample on the minute boundary, so the hour rolls at :00
            await asyncio.sleep(max(1.0, self.interval_s - (time.time() % self.interval_s)))
