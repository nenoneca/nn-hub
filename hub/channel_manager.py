"""Thread channel management: scan, vote, migrate -- manual and automatic.

Scan job
    Every online gateway (H2G GW_CHANNEL_SCAN) and every sensor that sent a
    radio report in the last hour (H2D CHANNEL_SCAN_REQ) energy-scans
    channels 11..26, one device at a time: a scanning radio is off the mesh
    channel for 16 x dwell, so staggering keeps the mesh reachable.
Vote
    hub/channel_vote.py: Borda count over the scans (k best per voter),
    veto for any channel a voter hears at the busy level, allowed-channel
    list, and hysteresis (decide()).  Gateways weigh more: all traffic
    crosses them.
Migrate
    H2G GW_CHANNEL_SET to one gateway: MGMT_PENDING_SET with a delay timer,
    so the whole mesh (sensors included) switches together when it
    expires -- standard Thread, no per-device command.  After the delay the
    hub reads the active dataset back from the gateways and, when the mesh
    really moved, stores channel + dataset in its network table (new
    gateways and devices are provisioned from it).
Automatic trigger
    Off by default.  Every check_s: when at least half of the reporting
    sensors (and min_devices) show the chosen metric (channel busy % or no
    ack %) at or above threshold_pct over the last sustain_min, and the
    cooldown has passed, run a scan job and migrate if the vote says so.

Settings (hub settings table, prefix radio.channel.):
    k, dwell_ms, veto_dbm, quiet_dbm, allowed, margin, delay_s, gateway_weight,
    sensor_gap_s, cooldown_h, auto.enabled, auto.metric, auto.threshold_pct,
    auto.sustain_min, auto.min_devices, auto.check_s, auto.rescan_min
State (setting radio.channel.state, JSON): pending migration, history,
last job summary, last automatic evaluation.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import secrets
import struct
import time
from typing import Optional

from . import channel_vote as cv
from . import proto
from . import radio_health

log = logging.getLogger("hub.channel")

PREFIX = "radio.channel."
DEFAULTS = {
    "k": 3,
    "dwell_ms": 150,
    "veto_dbm": -60,
    "quiet_dbm": -85,
    "allowed": "11-26",
    "margin": 1.0,
    "delay_s": 300,
    "gateway_weight": 2.0,
    "sensor_gap_s": 3.0,
    "cooldown_h": 6.0,
    "auto.enabled": 0,
    "auto.metric": "busy",          # busy | no_ack
    # 60: at 30 the trigger fired on self-inflicted load (a gateway-freeze
    # test pushed busy to ~72 % and moved the mesh 25 -> 20, 2026-09-25)
    "auto.threshold_pct": 60.0,
    "auto.sustain_min": 30,
    "auto.min_devices": 2,
    "auto.check_s": 300,
    "auto.rescan_min": 60,          # after a "stay" vote, wait before scanning again
}
_METRIC_KEY = {"busy": "busy_pct", "no_ack": "no_ack_pct"}
HISTORY_KEEP = 20


def _num(v, default):
    try:
        return type(default)(float(v)) if isinstance(default, int) else float(v)
    except (TypeError, ValueError):
        return default


def dataset_channel(tlvs: bytes) -> Optional[int]:
    """Channel from an OT dataset TLV blob (type 0, len 3: page + u16 BE)."""
    i = 0
    while i + 2 <= len(tlvs):
        t, ln = tlvs[i], tlvs[i + 1]
        v = tlvs[i + 2:i + 2 + ln]
        if len(v) < ln:
            return None
        if t == 0 and ln == 3:
            return (v[1] << 8) | v[2]
        i += 2 + ln
    return None


class ChannelManager:
    def __init__(self, db, router):
        self._db = db
        self._router = router
        self._jobs: dict[str, dict] = {}
        self._job_task: Optional[asyncio.Task] = None
        self._verify_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    # ── settings / state ────────────────────────────────────────────────
    def settings(self) -> dict:
        raw = self._db.settings_with_prefix(PREFIX)
        out = {}
        for k, d in DEFAULTS.items():
            v = raw.get(PREFIX + k)
            if v is None:
                out[k] = d
            elif isinstance(d, str):
                out[k] = v
            else:
                out[k] = _num(v, d)
        return out

    def put_settings(self, patch: dict) -> dict:
        for k, v in patch.items():
            if k not in DEFAULTS:
                raise ValueError(f"unknown setting {k!r}")
            d = DEFAULTS[k]
            if k == "allowed":
                if not cv.parse_allowed(str(v)):
                    raise ValueError("allowed: no channel in 11..26")
                v = str(v)
            elif k == "auto.metric":
                if v not in _METRIC_KEY:
                    raise ValueError("auto.metric must be busy or no_ack")
            elif k == "k" and not 1 <= int(v) <= 16:
                raise ValueError("k must be 1..16")
            elif k == "dwell_ms" and not 50 <= int(v) <= 1000:
                raise ValueError("dwell_ms must be 50..1000")
            elif k == "delay_s" and not 60 <= int(v) <= 3600:
                raise ValueError("delay_s must be 60..3600")
            elif k == "auto.threshold_pct" and not 1 <= float(v) <= 100:
                raise ValueError("auto.threshold_pct must be 1..100")
            elif not isinstance(d, str):
                float(v)
            self._db.set_setting(PREFIX + k, str(v))
        return self.settings()

    def _state(self) -> dict:
        try:
            return json.loads(self._db.get_setting(PREFIX + "state") or "{}")
        except ValueError:
            return {}

    def _save_state(self, st: dict) -> None:
        self._db.set_setting(PREFIX + "state", json.dumps(st, separators=(",", ":")))

    def current_channel(self) -> Optional[int]:
        from .network import get_network
        net = get_network(self._db)
        return net.channel if net else None

    def status(self) -> dict:
        st = self._state()
        s = self.settings()
        last = st.get("last_migration_at") or 0
        return {
            "channel": self.current_channel(),
            "pending": st.get("pending"),
            "history": st.get("history", []),
            "last_job": st.get("last_job"),
            "auto": st.get("auto"),
            "cooldown_until": int(last + s["cooldown_h"] * 3600) if last else None,
            "job_running": bool(self._job_task and not self._job_task.done()),
            "settings": s,
        }

    # ── scan job ────────────────────────────────────────────────────────
    def get_job(self, job_id: str) -> Optional[dict]:
        job = self._jobs.get(job_id)
        if job is None:                       # finished before a hub restart
            full = self._state().get("last_job_full")
            if full and full.get("id") == job_id:
                job = full
        return job

    def start_scan(self, source: str = "manual") -> dict:
        if self._job_task and not self._job_task.done():
            raise RuntimeError("a scan is already running")
        job_id = secrets.token_hex(4)
        job = {"id": job_id, "state": "running", "source": source,
               "started_at": int(time.time()), "finished_at": None,
               "scans": [], "ranking": None, "decision": None, "error": None}
        self._jobs[job_id] = job
        for old in list(self._jobs)[:-5]:             # oldest first (insertion order)
            if self._jobs[old]["state"] != "running":
                self._jobs.pop(old, None)
        self._job_task = asyncio.ensure_future(self._run_scan(job))
        return job

    def _scan_targets(self) -> tuple[list, list]:
        gws = [g for g in self._db.list_gateways()
               if self._router._server.is_gateway_online(g.id)]
        since = int(time.time()) - 3600
        seen = []
        for r in self._db.radio_stats_since(None, since):
            # only devices still registered: an archived or replaced id
            # keeps its old reports and would just time out
            if r["device_id"] not in seen and self._db.get_device(r["device_id"]):
                seen.append(r["device_id"])
        return gws, seen

    async def _run_scan(self, job: dict) -> dict:
        s = self.settings()
        dwell = int(s["dwell_ms"])
        body = struct.pack("<H", dwell)
        scan_s = 16 * dwell / 1000.0
        try:
            gws, devs = self._scan_targets()
            job["targets"] = {"gateways": [g.name or g.id for g in gws],
                              "sensors": len(devs)}
            for g in gws:
                entry = {"voter": g.name or g.id, "kind": "gateway",
                         "weight": s["gateway_weight"], "dbm": None, "error": None}
                try:
                    st, rb = await self._router.gateway_request(
                        g.id, proto.Cmd.GW_CHANNEL_SCAN, body, timeout=scan_s + 10)
                    if st != 0 or len(rb) < 17:
                        entry["error"] = f"gateway status {st}"
                    else:
                        entry["channel"] = rb[0]
                        entry["dbm"] = cv.scan_from_bytes(rb[1:17])
                except Exception as e:                       # noqa: BLE001
                    entry["error"] = str(e) or type(e).__name__
                job["scans"].append(entry)
            for i, did in enumerate(devs):
                d = self._db.get_device(did)
                entry = {"voter": d.name if d else did, "kind": "sensor",
                         "weight": 1.0, "dbm": None, "error": None}
                if i or gws:
                    await asyncio.sleep(s["sensor_gap_s"])
                try:
                    rb = await self._router.request_h2d(
                        did, proto.Cmd.CHANNEL_SCAN_REQ, body,
                        proto.Cmd.CHANNEL_SCAN_REPLY,
                        timeout=scan_s + 5, max_attempts=2, backoff_base=1.0)
                    st = struct.unpack_from("<b", rb, 0)[0] if rb else -1
                    if st != 0 or len(rb) < 18:
                        entry["error"] = f"device status {st}"
                    else:
                        entry["channel"] = rb[1]
                        entry["dbm"] = cv.scan_from_bytes(rb[2:18])
                except Exception as e:                       # noqa: BLE001
                    entry["error"] = ("no reply (firmware without channel scan?)"
                                      if isinstance(e, asyncio.TimeoutError)
                                      else str(e) or type(e).__name__)
                job["scans"].append(entry)
            voters = [x for x in job["scans"] if x["dbm"]]
            if not voters:
                raise RuntimeError("no device returned a scan")
            ranking = cv.vote(voters, k=int(s["k"]),
                              allowed=cv.parse_allowed(s["allowed"]),
                              veto_dbm=int(s["veto_dbm"]), quiet_dbm=int(s["quiet_dbm"]))
            job["ranking"] = ranking
            job["decision"] = cv.decide(self.current_channel(), ranking,
                                        k=int(s["k"]), margin=float(s["margin"]),
                                        quiet_dbm=int(s["quiet_dbm"]))
            job["state"] = "done"
        except Exception as e:                               # noqa: BLE001
            job["state"] = "failed"
            job["error"] = str(e) or type(e).__name__
            log.warning("channel scan %s failed: %s", job["id"], job["error"])
        job["finished_at"] = int(time.time())
        st = self._state()
        st["last_job"] = {k: job[k] for k in ("id", "state", "source", "started_at",
                                              "finished_at", "decision", "error")}
        st["last_job"]["top"] = [{"channel": r["channel"], "points": r["points"],
                                  "vetoed": bool(r["vetoed_by"])}
                                 for r in (job["ranking"] or [])[:5]]
        st["last_job_full"] = job
        self._save_state(st)
        log.info("channel scan %s %s: %d voters, decision %s", job["id"], job["state"],
                 sum(1 for x in job["scans"] if x["dbm"]), job["decision"])
        return job

    # ── migration ───────────────────────────────────────────────────────
    def _pick_gateway(self):
        gws = [g for g in self._db.list_gateways()
               if self._router._server.is_gateway_online(g.id)]
        gws.sort(key=lambda g: {3: 0, 2: 1}.get(g.role, 2))   # leader, router, other
        return gws[0] if gws else None

    async def migrate(self, channel: int, source: str = "manual",
                      dry_run: bool = False, reason: str = "") -> dict:
        async with self._lock:
            s = self.settings()
            st = self._state()
            cur = self.current_channel()
            now = int(time.time())
            if channel not in cv.parse_allowed(s["allowed"]):
                raise ValueError(f"channel {channel} is not in the allowed list {s['allowed']}")
            if channel == cur:
                raise ValueError(f"the mesh is already on channel {channel}")
            pend = st.get("pending")
            if pend and now < pend.get("effective_at", 0) + 600:
                raise RuntimeError(f"a move to channel {pend['channel']} is still pending")
            last = st.get("last_migration_at") or 0
            if source == "auto" and now < last + s["cooldown_h"] * 3600:
                raise RuntimeError("automatic channel change is in its cooldown")
            gw = self._pick_gateway()
            if gw is None:
                raise RuntimeError("no gateway online")
            delay = int(s["delay_s"])
            plan = {"channel": channel, "from": cur, "via": gw.name or gw.id,
                    "delay_s": delay, "source": source, "reason": reason}
            if dry_run:
                return {"dry_run": True, **plan}
            status, _ = await self._router.gateway_request(
                gw.id, proto.Cmd.GW_CHANNEL_SET,
                bytes([channel]) + struct.pack("<H", delay), timeout=20)
            if status != 0:
                raise RuntimeError(f"gateway refused the channel change (status {status})")
            pend = {**plan, "requested_at": now, "effective_at": now + delay}
            st["pending"] = pend
            st["last_migration_at"] = now
            self._save_state(st)
            log.warning("channel: moving the mesh %s -> %s in %d s via %s (%s%s)",
                        cur, channel, delay, plan["via"], source,
                        f": {reason}" if reason else "")
            if self._verify_task is None or self._verify_task.done():
                self._verify_task = asyncio.ensure_future(self._verify(pend))
            return {"dry_run": False, **pend}

    async def _verify(self, pend: dict) -> None:
        """After the delay timer: read the active dataset back; store it when
        the mesh really moved."""
        await asyncio.sleep(max(0, pend["effective_at"] - time.time()) + 90)
        result = {"channel": pend["channel"], "from": pend["from"],
                  "source": pend["source"], "requested_at": pend["requested_at"],
                  "checked_at": int(time.time()), "ok": False, "detail": ""}
        seen = []
        for g in self._db.list_gateways():
            if not self._router._server.is_gateway_online(g.id):
                continue
            try:
                st, tlvs = await self._router.gateway_request(
                    g.id, proto.Cmd.GW_DATASET_GET, b"", timeout=10)
            except Exception as e:                           # noqa: BLE001
                seen.append(f"{g.name}: {e}")
                continue
            ch = dataset_channel(tlvs) if st == 0 else None
            seen.append(f"{g.name}: channel {ch}")
            if ch == pend["channel"] and not result["ok"]:
                self._db._conn.execute(
                    "UPDATE network SET channel = ?, dataset_tlvs_hex = ? WHERE id = 1",
                    (ch, tlvs.hex()))
                self._db._conn.commit()
                result["ok"] = True
        result["detail"] = "; ".join(seen) or "no gateway online"
        st = self._state()
        st["pending"] = None
        st.setdefault("history", []).insert(0, result)
        st["history"] = st["history"][:HISTORY_KEEP]
        self._save_state(st)
        (log.info if result["ok"] else log.error)(
            "channel: move to %s %s (%s)", pend["channel"],
            "confirmed" if result["ok"] else "NOT confirmed", result["detail"])

    def resume(self) -> None:
        """After a hub restart: re-arm the check of a pending migration."""
        pend = self._state().get("pending")
        if pend and (self._verify_task is None or self._verify_task.done()):
            self._verify_task = asyncio.ensure_future(self._verify(pend))

    # ── automatic trigger ───────────────────────────────────────────────
    def evaluate(self, now: Optional[int] = None) -> dict:
        """Is the channel bad enough, for long enough, on enough devices?"""
        s = self.settings()
        now = now or int(time.time())
        key = _METRIC_KEY.get(s["auto.metric"], "busy_pct")
        rows = self._db.radio_stats_since(None, now - int(s["auto.sustain_min"]) * 60)
        by_dev: dict[str, list] = {}
        for r in rows:
            by_dev.setdefault(r["device_id"], []).append(r)
        values = {}
        for did, reps in by_dev.items():
            summ = radio_health.summarize(reps)
            w = (summ or {}).get("window")
            if w and w.get(key) is not None and w.get("seconds", 0) >= 0.6 * s["auto.sustain_min"] * 60:
                d = self._db.get_device(did)
                values[d.name if d else did] = w[key]
        over = [n for n, v in values.items() if v >= s["auto.threshold_pct"]]
        need = max(int(s["auto.min_devices"]), math.ceil(len(values) / 2)) if values else 1
        return {"at": now, "metric": s["auto.metric"], "threshold_pct": s["auto.threshold_pct"],
                "values": values, "over": over, "need": need,
                "breach": bool(values) and len(over) >= need}

    async def run(self) -> None:
        self.resume()
        while True:
            s = self.settings()
            await asyncio.sleep(max(60, int(s["auto.check_s"])))
            if not int(s["auto.enabled"]):
                continue
            try:
                ev = self.evaluate()
                st = self._state()
                ev["action"] = "none"
                last = st.get("last_migration_at") or 0
                if ev["breach"]:
                    if time.time() < last + s["cooldown_h"] * 3600:
                        ev["action"] = "cooldown"
                    elif st.get("pending") or (self._job_task and not self._job_task.done()):
                        ev["action"] = "busy"
                    elif time.time() < (st.get("auto_scanned_at") or 0) + s["auto.rescan_min"] * 60:
                        ev["action"] = "waiting to rescan"
                    else:
                        job = self.start_scan(source="auto")
                        await self._job_task
                        st = self._state()
                        st["auto_scanned_at"] = int(time.time())
                        self._save_state(st)
                        dec = job.get("decision") or {}
                        ev["job"] = job["id"]
                        ev["decision"] = dec
                        if job["state"] == "done" and dec.get("move") and dec.get("winner"):
                            await self.migrate(dec["winner"], source="auto",
                                               reason=f"{ev['metric']} >= {ev['threshold_pct']:g} % on "
                                                      f"{', '.join(ev['over'])}; {dec['reason']}")
                            ev["action"] = f"moving to {dec['winner']}"
                        else:
                            ev["action"] = "stay"
                st = self._state()
                st["auto"] = ev
                self._save_state(st)
                if ev["breach"]:
                    log.warning("channel auto: %s over %g %% on %s -> %s", ev["metric"],
                                ev["threshold_pct"], ev["over"], ev["action"])
            except Exception as e:                           # noqa: BLE001
                log.warning("channel auto check failed: %s", e)
