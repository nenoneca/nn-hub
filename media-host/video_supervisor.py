#!/usr/bin/env python3
"""nn-video — ONE service for every camera slot on this host.

Until 2026-09-21 each camera slot was its own systemd unit (nn-video,
nn-video2, nn-video3, nn-video4), each a copy of the same command line with
different ports and directories.  Adding a camera meant writing a unit file
by hand, and a wedged pipeline could only be noticed by a person.

This supervisor is the single unit.  It reads the slot table
(/etc/nn-media/slots.yaml), runs every slot as a supervised WORKER
subprocess (the proven per-slot video_service.py — same ports, URLs, key
directories, so nothing the hub addresses moves), and:

  * restarts a worker that exits or crashes, with backoff (2 s … 60 s) that
    resets after ten healthy minutes — it never gives up on a slot;
  * detects a HUNG worker (control API unreachable for 20 s while the
    process lives, or video still arriving with no new HLS segment for
    hang_grace_s) and kills it (TERM, then KILL) so the restart path runs;
  * classifies exits: -11 = segfault (counted per slot, with the Python
    stack printed by faulthandler — PYTHONFAULTHANDLER=1 is set for every
    worker), 42 = the worker's own HLS watchdog, 43 = an uncaught exception
    in a worker thread, 44 = a GStreamer bus error, 45 = pipeline state change
    timeout;
  * prefixes every worker log line with the slot id into ONE journal source;
  * exposes GET /health, GET /slots, POST /slots (add a slot live),
    DELETE /slots/{id}, POST /slots/{id}/restart, POST /reload — slots can be
    added and removed without a restart, which is what makes this scale.

Why worker PROCESSES and not threads in one interpreter: the native code a
slot runs (GStreamer, PyAV, hardware decoders) has segfaulted before; a
process boundary is the only thing that turns "all cameras die" into "one
slot restarts in two seconds".  One unit, one API, one log — many workers.

Nothing here holds credentials; slots.yaml names ports, directories and the
per-slot key directory (the keys themselves stay under /var/lib/nn-media).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path

import yaml
from aiohttp import ClientSession, ClientTimeout, web

HERE = Path(__file__).resolve().parent
WORKER = str(HERE / "video_service.py")
EXIT_NAMES = {-11: "segfault", -6: "abort", -9: "killed", 42: "hls-watchdog",
              43: "thread-exception", 44: "gst-error", 45: "state-timeout"}
BACKOFF_S = (2, 4, 8, 16, 32, 60)


def log(slot: str, msg: str) -> None:
    print(f"[{slot}] {msg}", flush=True)


class Slot:
    def __init__(self, cfg: dict, sup: "Supervisor"):
        self.cfg = cfg
        self.sup = sup
        self.id = str(cfg["id"])
        self.proc: asyncio.subprocess.Process | None = None
        self.task: asyncio.Task | None = None
        self.state = "stopped"          # stopped | starting | running | hung | crashed | backoff
        self.started_at = 0.0
        self.restarts = 0
        self.crashes: dict[str, int] = {}
        self.last_exit: int | None = None
        self.last_exit_name = ""
        self.last_error = ""
        self.backoff_i = 0
        self.fed = -1
        self.fed_at = 0.0
        self.status_fail = 0
        self.hls_age_s: float | None = None
        self.stop_requested = False

    # ── command line ─────────────────────────────────────────────────────
    def command(self) -> list[str]:
        c = self.cfg
        cmd = [sys.executable, WORKER,
               "--stream-port", str(c["stream_port"]),
               "--control-port", str(c["control_port"]),
               "--keydir", str(c["keydir"]),
               "--hls-dir", str(c["hls_dir"])]
        if c.get("hls_port"):
            cmd += ["--hls-port", str(c["hls_port"])]
        if c.get("snapshot_dir"):
            cmd += ["--snapshot-dir", str(c["snapshot_dir"])]
        cmd += ["--hub-url", str(c.get("hub_url") or self.sup.hub_url)]
        cmd += [str(a) for a in (c.get("args") or [])]
        return cmd

    def environment(self) -> dict:
        env = dict(os.environ)
        env.update({k: str(v) for k, v in (self.sup.common_env or {}).items()})
        env.update({k: str(v) for k, v in (self.cfg.get("env") or {}).items()})
        env["NN_CAM_ID"] = self.id
        env.setdefault("NN_CAM_NAME", str(self.cfg.get("name") or self.id))
        env.setdefault("NN_CAM_URL", f"http://127.0.0.1:{self.cfg['control_port']}")
        env["PYTHONFAULTHANDLER"] = "1"      # Python stack on SIGSEGV, into our log
        env["PYTHONUNBUFFERED"] = "1"
        env["NN_SUPERVISED"] = "1"
        return env

    # ── lifecycle ────────────────────────────────────────────────────────
    async def run(self) -> None:
        """Start, pump logs, wait, classify, back off, repeat."""
        while not self.stop_requested:
            self.state = "starting"
            try:
                self.proc = await asyncio.create_subprocess_exec(
                    *self.command(), env=self.environment(),
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            except Exception as e:                               # noqa: BLE001
                self.last_error = f"spawn failed: {e}"
                log(self.id, self.last_error)
                await self._backoff()
                continue
            self.started_at = time.time()
            self.fed, self.fed_at, self.status_fail = -1, time.time(), 0
            self.state = "running"
            log(self.id, f"worker pid {self.proc.pid} started: {' '.join(self.command()[2:8])} …")
            pump = asyncio.create_task(self._pump(self.proc.stdout))
            rc = await self.proc.wait()
            await pump
            self.proc = None
            self.last_exit = rc
            self.last_exit_name = EXIT_NAMES.get(rc, f"exit {rc}")
            up = time.time() - self.started_at
            if self.stop_requested:
                self.state = "stopped"
                log(self.id, f"worker stopped ({self.last_exit_name}) after {up:.0f}s")
                return
            self.crashes[self.last_exit_name] = self.crashes.get(self.last_exit_name, 0) + 1
            self.restarts += 1
            if up > 600:
                self.backoff_i = 0                   # a long healthy run resets the backoff
            self.state = "crashed"
            log(self.id, f"worker exited: {self.last_exit_name} after {up:.0f}s "
                         f"(restart #{self.restarts})")
            await self._backoff()

    async def _backoff(self) -> None:
        delay = BACKOFF_S[min(self.backoff_i, len(BACKOFF_S) - 1)]
        self.backoff_i += 1
        self.state = "backoff"
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise

    async def _pump(self, stream) -> None:
        while True:
            line = await stream.readline()
            if not line:
                return
            text = line.decode("utf-8", errors="replace").rstrip()
            if text:
                log(self.id, text)

    async def kill(self, reason: str) -> None:
        p = self.proc
        if p is None or p.returncode is not None:
            return
        log(self.id, f"killing worker pid {p.pid}: {reason}")
        self.last_error = reason
        try:
            p.terminate()
            try:
                await asyncio.wait_for(p.wait(), timeout=8)
                return
            except asyncio.TimeoutError:
                pass
            p.kill()
        except ProcessLookupError:
            pass

    async def stop(self) -> None:
        self.stop_requested = True
        await self.kill("stop requested")
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except (asyncio.CancelledError, Exception):      # noqa: BLE001
                pass
        self.state = "stopped"

    # ── health ───────────────────────────────────────────────────────────
    async def check(self, http: ClientSession) -> None:
        if self.proc is None or self.state not in ("running", "hung"):
            return
        alive_s = time.time() - self.started_at
        # the worker needs a few seconds to bind its control port
        if alive_s < self.sup.startup_grace_s:
            return
        try:
            # /health carries fed + HLS age (added with the supervisor); a
            # worker that predates it still answers /status, which proves
            # liveness even though it cannot prove flow.
            async with http.get(f"http://127.0.0.1:{self.cfg['control_port']}/health",
                                timeout=ClientTimeout(total=3)) as r:
                if r.status == 404:
                    async with http.get(f"http://127.0.0.1:{self.cfg['control_port']}/status",
                                        timeout=ClientTimeout(total=3)) as r2:
                        doc = await r2.json()
                else:
                    doc = await r.json()
            self.status_fail = 0
        except Exception as e:                                   # noqa: BLE001
            self.status_fail += 1
            if self.status_fail >= self.sup.status_fail_limit:
                self.state = "hung"
                await self.kill(f"control API unreachable {self.status_fail}x ({type(e).__name__})")
            return
        fed = int(doc.get("fed", 0) or 0)
        now = time.time()
        if fed != self.fed:
            self.fed, self.fed_at = fed, now
        self.hls_age_s = self._hls_newest_age()
        # video is arriving (fed advances) but nothing comes out of the HLS
        # end for hang_grace_s: the pipeline is wedged.  A camera that is
        # simply offline (fed frozen) is not a fault.
        if (now - self.fed_at < 30 and self.hls_age_s is not None
                and self.hls_age_s > self.sup.hang_grace_s):
            self.state = "hung"
            await self.kill(f"video arriving but newest HLS segment is {self.hls_age_s:.0f}s old")

    def _hls_newest_age(self) -> float | None:
        d = Path(str(self.cfg["hls_dir"]))
        try:
            segs = [p.stat().st_mtime for p in d.glob("*.ts")]
        except OSError:
            return None
        if not segs:
            return None
        return time.time() - max(segs)

    def describe(self) -> dict:
        return {
            "id": self.id, "name": self.cfg.get("name"), "state": self.state,
            "pid": self.proc.pid if self.proc else None,
            "control_url": f"http://127.0.0.1:{self.cfg['control_port']}",
            "uptime_s": int(time.time() - self.started_at) if self.proc else 0,
            "restarts": self.restarts, "crashes": dict(self.crashes),
            "last_exit": self.last_exit_name or None, "last_error": self.last_error or None,
            "fed": self.fed if self.fed >= 0 else None,
            "hls_newest_age_s": round(self.hls_age_s, 1) if self.hls_age_s is not None else None,
            "stream_port": self.cfg["stream_port"], "control_port": self.cfg["control_port"],
        }


class Supervisor:
    def __init__(self, slots_path: Path, api_port: int, hub_url: str,
                 hang_grace_s: float = 90.0, startup_grace_s: float = 15.0,
                 status_fail_limit: int = 4, check_interval_s: float = 5.0):
        self.slots_path = slots_path
        self.api_port = api_port
        self.hub_url = hub_url
        self.hang_grace_s = hang_grace_s
        self.startup_grace_s = startup_grace_s
        self.status_fail_limit = status_fail_limit
        self.check_interval_s = check_interval_s
        self.common_env: dict = {}
        self.slots: dict[str, Slot] = {}
        self.started = time.time()

    # ── slot table ───────────────────────────────────────────────────────
    def load(self) -> list[dict]:
        raw = yaml.safe_load(self.slots_path.read_text()) if self.slots_path.exists() else {}
        raw = raw or {}
        self.common_env = dict(raw.get("env") or {})
        if raw.get("hub_url"):
            self.hub_url = str(raw["hub_url"])
        if raw.get("hang_grace_s"):
            self.hang_grace_s = float(raw["hang_grace_s"])
        out = []
        for s in raw.get("slots") or []:
            for k in ("id", "stream_port", "control_port", "keydir", "hls_dir"):
                if k not in s:
                    raise ValueError(f"slot {s.get('id')!r} lacks {k!r}")
            out.append(s)
        return out

    def save(self) -> str | None:
        """Persist the live slot set.  Returns an error text instead of
        raising when the file is not writable by the service user (a
        root-owned /etc/nn-media): the live change already happened, the
        caller reports that it will not survive a restart."""
        doc = {"hub_url": self.hub_url, "env": self.common_env,
               "hang_grace_s": self.hang_grace_s,
               "slots": [s.cfg for s in self.slots.values()]}
        tmp = self.slots_path.with_suffix(".tmp")
        try:
            tmp.write_text(yaml.safe_dump(doc, sort_keys=False))
            os.replace(tmp, self.slots_path)
        except OSError as e:
            msg = f"slots file not saved ({e}); the change is live but will not survive a restart"
            log("supervisor", msg)
            return msg
        return None

    def start_slot(self, cfg: dict) -> Slot:
        s = Slot(cfg, self)
        self.slots[s.id] = s
        s.task = asyncio.create_task(s.run(), name=f"slot-{s.id}")
        return s

    async def remove_slot(self, sid: str) -> None:
        s = self.slots.pop(sid, None)
        if s:
            await s.stop()

    async def reload(self) -> dict:
        wanted = {str(c["id"]): c for c in self.load()}
        removed = [sid for sid in self.slots if sid not in wanted]
        for sid in removed:
            await self.remove_slot(sid)
        added, changed = [], []
        for sid, cfg in wanted.items():
            cur = self.slots.get(sid)
            if cur is None:
                self.start_slot(cfg); added.append(sid)
            elif cur.cfg != cfg:
                await self.remove_slot(sid); self.start_slot(cfg); changed.append(sid)
        return {"added": added, "removed": removed, "restarted": changed}

    # ── loops ────────────────────────────────────────────────────────────
    async def health_loop(self) -> None:
        async with ClientSession() as http:
            while True:
                await asyncio.sleep(self.check_interval_s)
                for s in list(self.slots.values()):
                    try:
                        await s.check(http)
                    except Exception as e:                       # noqa: BLE001
                        log(s.id, f"health check error: {e}")

    def health(self) -> dict:
        slots = [s.describe() for s in self.slots.values()]
        return {"service": "nn-video", "uptime_s": int(time.time() - self.started),
                "slots": slots,
                "summary": {st: sum(1 for s in slots if s["state"] == st)
                            for st in ("running", "starting", "backoff", "crashed", "hung", "stopped")},
                "hang_grace_s": self.hang_grace_s}

    # ── API ──────────────────────────────────────────────────────────────
    def make_app(self) -> web.Application:
        app = web.Application()

        async def health(_r):
            return web.json_response(self.health())

        async def list_slots(_r):
            return web.json_response([s.describe() for s in self.slots.values()])

        async def add_slot(r):
            try:
                cfg = await r.json()
                for k in ("id", "stream_port", "control_port", "keydir", "hls_dir"):
                    if k not in cfg:
                        return web.json_response({"err": f"missing {k}"}, status=400)
            except Exception:
                return web.json_response({"err": "expected JSON slot object"}, status=400)
            sid = str(cfg["id"])
            if sid in self.slots:
                return web.json_response({"err": f"slot {sid} exists"}, status=409)
            ports = {(s.cfg["stream_port"], s.cfg["control_port"]) for s in self.slots.values()}
            for s in self.slots.values():
                if s.cfg["control_port"] == cfg["control_port"] or s.cfg["stream_port"] == cfg["stream_port"]:
                    return web.json_response({"err": f"port clash with slot {s.id}"}, status=409)
            self.start_slot(cfg)
            self.save()
            log(sid, "slot added")
            return web.json_response(self.slots[sid].describe(), status=201)

        async def del_slot(r):
            sid = r.match_info["id"]
            if sid not in self.slots:
                return web.json_response({"err": "no such slot"}, status=404)
            await self.remove_slot(sid)
            warn = self.save()
            log(sid, "slot removed")
            out = {"ok": True, "removed": sid, "saved": warn is None}
            if warn:
                out["warn"] = warn
            return web.json_response(out)

        async def restart_slot(r):
            s = self.slots.get(r.match_info["id"])
            if s is None:
                return web.json_response({"err": "no such slot"}, status=404)
            s.backoff_i = 0
            await s.kill("restart requested")
            return web.json_response({"ok": True})

        async def reload(_r):
            try:
                return web.json_response(await self.reload())
            except Exception as e:                               # noqa: BLE001
                return web.json_response({"err": str(e)}, status=400)

        app.router.add_get("/health", health)
        app.router.add_get("/slots", list_slots)
        app.router.add_post("/slots", add_slot)
        app.router.add_delete("/slots/{id}", del_slot)
        app.router.add_post("/slots/{id}/restart", restart_slot)
        app.router.add_post("/reload", reload)
        return app

    async def serve(self) -> None:
        for cfg in self.load():
            self.start_slot(cfg)
        print(f"[nn-video] supervising {len(self.slots)} slot(s) from {self.slots_path}; "
              f"API on :{self.api_port}", flush=True)
        runner = web.AppRunner(self.make_app())
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", self.api_port).start()
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        hl = asyncio.create_task(self.health_loop())
        await stop.wait()
        print("[nn-video] stopping all slots", flush=True)
        hl.cancel()
        await asyncio.gather(*(s.stop() for s in list(self.slots.values())), return_exceptions=True)
        await runner.cleanup()


def main() -> None:
    ap = argparse.ArgumentParser(description="nn-video: one service, every camera slot")
    ap.add_argument("--slots", default="/etc/nn-media/slots.yaml")
    ap.add_argument("--api-port", type=int, default=8898)
    ap.add_argument("--hub-url", default="http://127.0.0.1:8769")
    ap.add_argument("--hang-grace-s", type=float, default=90.0)
    a = ap.parse_args()
    sup = Supervisor(Path(a.slots), a.api_port, a.hub_url, hang_grace_s=a.hang_grace_s)
    asyncio.run(sup.serve())


if __name__ == "__main__":
    main()
