"""Pipeline base, the generic worker loop, and the Engine that owns them.

A pipeline is a class with `run(request, settings)`; everything else —
taking from the queue, reading settings at run start, the time budget, the
progress stamp, failure counting, the reset after repeated failures, the
stall watchdog, forwarding what a run emits — is done ONCE, here, for every
pipeline.

Robustness inside one process (decided 2026-09-21):
  * an exception in a run is caught, logged with camera/pipeline/request,
    counted; `failures_before_reset` failures within `failure_window_s` on
    one camera → `pipeline.reset(camera_id)` and that camera's queued
    requests are purged;
  * a run that overruns `budget_s`: async runs are cancelled and counted as
    a stall; thread runs cannot be cancelled — the camera is marked stalled
    and reset when the thread returns;
  * a thread that never returns: after `stall_exit_s` the watchdog dumps
    every thread's stack (faulthandler) and exits the whole process for
    systemd to restart — the only way out inside one process.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import faulthandler
import logging
import os
import sys
import threading
import time
import traceback
from typing import Callable, Iterable, Optional

from .queue import PipelineQueue, Policy
from .request import Request
from .settings import Settings, Store

log = logging.getLogger("nnvideo")
EXIT_STUCK = 46

# The camera the current run is for.  Set by the worker around every run
# (and copied into the thread pool for THREAD pipelines) so code that has
# no camera object in hand — the legacy graph's print() calls above all —
# can tag its output "[cam/…]" for the per-camera log filter.
current_camera: contextvars.ContextVar[str] = contextvars.ContextVar("nnvideo_camera", default="")


def malloc_trim() -> int:
    """Return freed heap pages to the OS.  glibc keeps memory freed in a
    thread's arena for reuse; with ~60 native threads and event recordings
    that allocate and free tens of MB in bursts, the process's resident size
    grew by hundreds of MB of *free* memory (phase-3 soak, 2026-09-21).
    malloc_trim(0) walks every arena.  Returns 1 if something was released."""
    try:
        import ctypes
        return int(ctypes.CDLL("libc.so.6").malloc_trim(0))
    except Exception:                                        # noqa: BLE001
        return -1


_cpu_last = [0.0, 0.0]        # (wall time, process cpu seconds) of the previous health call


def cpu_pct() -> float:
    """Process CPU since the previous call, in percent of one core (0 on
    the first call).  /proc/self/stat utime+stime; the caller is the
    health route, polled by the hub's collector."""
    try:
        with open("/proc/self/stat") as f:
            parts = f.read().rsplit(")", 1)[1].split()
        cpu = (int(parts[11]) + int(parts[12])) / os.sysconf("SC_CLK_TCK")
    except Exception:
        return 0.0
    now = time.time()
    t0, c0 = _cpu_last
    _cpu_last[0], _cpu_last[1] = now, cpu
    if not t0 or now - t0 < 1.0:
        return 0.0
    return round(100.0 * (cpu - c0) / (now - t0), 1)


def rss_kb() -> int:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return 0


class Mode:
    ASYNC = "async"      # I/O and bookkeeping on the event loop
    THREAD = "thread"    # CPU or blocking native work in the pool


class Pipeline:
    name: str = "base"
    mode: str = Mode.ASYNC
    policy: str = Policy.DROP_OLDEST
    per_camera_cap: int = 64
    budget_s: float = 1.0
    workers: int = 1
    failures_before_reset: int = 5
    failure_window_s: float = 30.0
    defaults: dict = {}

    def __init__(self):
        self.engine: Optional["Engine"] = None

    async def run(self, req: Request, cfg: dict) -> Iterable[Request] | None:   # pragma: no cover - abstract
        raise NotImplementedError

    def run_sync(self, req: Request, cfg: dict) -> Iterable[Request] | None:    # pragma: no cover - abstract
        """THREAD mode pipelines implement this instead of run()."""
        raise NotImplementedError

    def reset(self, camera_id: str) -> None:
        """Rebuild this camera's state (graph, decoder, ring) after repeated
        failures or a stall.  Default: nothing to rebuild."""

    def describe(self) -> dict:
        return {}


class _CamState:
    __slots__ = ("failures", "runs", "stalls", "resets", "last_run_at", "last_latency_ms",
                 "last_error", "stalled")

    def __init__(self):
        self.failures: list[float] = []
        self.runs = 0
        self.stalls = 0
        self.resets = 0
        self.last_run_at = 0.0
        self.last_latency_ms = 0.0
        self.last_error = ""
        self.stalled = False


class _Worker:
    """One worker of one pipeline: take → settings → run (budgeted) → emit."""

    def __init__(self, engine: "Engine", pipe: Pipeline, index: int):
        self.engine, self.pipe, self.index = engine, pipe, index
        self.busy_since: float | None = None
        self.current: Request | None = None
        self.task: asyncio.Task | None = None
        self.runs = 0
        self.busy_s = 0.0

    async def loop(self) -> None:
        q = self.engine.queues[self.pipe.name]
        while True:
            req = await q.take()
            if req is None:
                return
            self.current, self.busy_since = req, time.time()
            try:
                await self._one(req)
            finally:
                self.busy_s += time.time() - self.busy_since
                self.runs += 1
                self.current, self.busy_since = None, None
                await q.release(req.camera_id)

    async def _one(self, req: Request) -> None:
        eng, pipe = self.engine, self.pipe
        st = eng.state(pipe.name, req.camera_id)
        cfg = eng.settings.get(req.camera_id, pipe.name)     # at the start of EVERY run
        if not cfg.get("enabled", True):
            return
        t0 = time.time()
        token = current_camera.set(req.camera_id)
        try:
            if pipe.mode == Mode.THREAD:
                fut = eng.loop.run_in_executor(eng.pool, contextvars.copy_context().run, pipe.run_sync, req, cfg)
                try:
                    out = await asyncio.wait_for(asyncio.shield(fut), timeout=pipe.budget_s)
                except asyncio.TimeoutError:
                    # cannot cancel a thread: mark stalled, let the watchdog
                    # decide, reset when (if) it returns
                    st.stalls += 1; st.stalled = True
                    eng._log(pipe.name, req.camera_id,
                             f"run over budget {pipe.budget_s}s in a thread ({req}); marked stalled")
                    # the worker is free again, but the THREAD is not: the
                    # watchdog keeps an eye on it from here
                    eng._stalled_threads[(pipe.name, req.camera_id)] = (t0, fut, req)
                    fut.add_done_callback(lambda f, c=req.camera_id: eng._thread_returned(pipe, c, f))
                    return
            else:
                out = await asyncio.wait_for(pipe.run(req, cfg), timeout=pipe.budget_s)
        except asyncio.TimeoutError:
            st.stalls += 1
            st.failures.append(time.time())
            st.last_error = f"cancelled: over budget {pipe.budget_s}s"
            eng._log(pipe.name, req.camera_id, f"{st.last_error} ({req})")
            await eng._maybe_reset(pipe, req.camera_id, st)
            return
        except Exception as e:                                   # noqa: BLE001
            st.failures.append(time.time())
            st.last_error = f"{type(e).__name__}: {e}"
            eng._log(pipe.name, req.camera_id,
                     f"run failed on {req}: {st.last_error}\n" + traceback.format_exc().rstrip())
            await eng._maybe_reset(pipe, req.camera_id, st)
            return
        finally:
            current_camera.reset(token)
        st.runs += 1
        st.last_run_at = time.time()
        st.last_latency_ms = round(1000 * (st.last_run_at - t0), 2)
        st.stalled = False
        if out:
            for r in out:
                await eng.submit(r)


class Engine:
    def __init__(self, store: Store, *, pool_workers: int = 4,
                 stall_exit_s: float = 120.0, exit_fn: Callable[[int], None] | None = None,
                 malloc_trim_s: float = 60.0):
        self.store = store
        self.malloc_trim_s = malloc_trim_s
        self._last_trim = time.time()
        self.trims = 0
        self.last_trim_mb = 0
        self.settings = Settings(store)
        self.pipelines: dict[str, Pipeline] = {}
        self.queues: dict[str, PipelineQueue] = {}
        self.workers: dict[str, list[_Worker]] = {}
        self._states: dict[tuple[str, str], _CamState] = {}
        self._stalled_threads: dict[tuple[str, str], tuple] = {}   # (pipeline, camera) -> (t0, fut, req)
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=pool_workers, thread_name_prefix="nnv")
        self.loop: asyncio.AbstractEventLoop | None = None
        self.stall_exit_s = stall_exit_s
        self._exit = exit_fn or (lambda code: os._exit(code))
        self.started = time.time()
        self.events: list[str] = []                # last 50 notable lines, for /health
        self._watchdog_task: asyncio.Task | None = None

    # ── registration ─────────────────────────────────────────────────────
    def add(self, pipe: Pipeline) -> Pipeline:
        pipe.engine = self
        self.pipelines[pipe.name] = pipe
        self.queues[pipe.name] = PipelineQueue(pipe.name, policy=pipe.policy,
                                               per_camera_cap=pipe.per_camera_cap)
        self.settings.register_defaults(pipe.name, {"enabled": True, "workers": pipe.workers,
                                                    **pipe.defaults})
        return pipe

    def state(self, pipeline: str, camera_id: str) -> _CamState:
        st = self._states.get((pipeline, camera_id))
        if st is None:
            st = self._states[(pipeline, camera_id)] = _CamState()
        return st

    # ── lifecycle ────────────────────────────────────────────────────────
    async def start(self) -> None:
        self.loop = asyncio.get_running_loop()
        faulthandler.enable()
        for name, pipe in self.pipelines.items():
            n = int(self.settings.get("*", name).get("workers", pipe.workers) or 1)
            self.workers[name] = []
            for i in range(n):
                self._spawn(pipe, i)
        self._watchdog_task = asyncio.create_task(self._watchdog(), name="nnvideo-watchdog")

    def _spawn(self, pipe: Pipeline, i: int) -> None:
        w = _Worker(self, pipe, i)
        w.task = asyncio.create_task(w.loop(), name=f"{pipe.name}-w{i}")
        self.workers[pipe.name].append(w)

    async def set_workers(self, pipeline: str, n: int) -> int:
        """Change a pipeline's worker count live (persisted as a global
        setting).  Extra workers finish their current run and stop."""
        n = max(1, min(int(n), 32))
        ws = self.workers[pipeline]
        while len(ws) < n:
            self._spawn(self.pipelines[pipeline], len(ws))
        while len(ws) > n:
            w = ws.pop()
            w.task.cancel()
        self.store.set_setting("*", pipeline, "workers", n)
        return n

    async def stop(self) -> None:
        if self._watchdog_task:
            self._watchdog_task.cancel()
        for q in self.queues.values():
            await q.close()
        for ws in self.workers.values():
            for w in ws:
                w.task.cancel()
        await asyncio.gather(*(w.task for ws in self.workers.values() for w in ws), return_exceptions=True)
        self.pool.shutdown(wait=False, cancel_futures=True)

    # ── submit / route ───────────────────────────────────────────────────
    async def submit(self, req: Request) -> bool:
        q = self.queues.get(req.pipeline)
        if q is None:
            self._log(req.pipeline, req.camera_id, f"no such pipeline for {req}")
            return False
        return await q.put(req)

    # ── failure handling ─────────────────────────────────────────────────
    async def _maybe_reset(self, pipe: Pipeline, camera_id: str, st: _CamState) -> None:
        now = time.time()
        st.failures = [t for t in st.failures if now - t <= pipe.failure_window_s]
        if len(st.failures) >= pipe.failures_before_reset:
            await self.reset(pipe.name, camera_id, "repeated failures")

    async def reset(self, pipeline: str, camera_id: str, reason: str) -> None:
        pipe, st = self.pipelines[pipeline], self.state(pipeline, camera_id)
        purged = await self.queues[pipeline].purge(camera_id)
        try:
            pipe.reset(camera_id)
        except Exception as e:                                   # noqa: BLE001
            self._log(pipeline, camera_id, f"reset itself failed: {e}")
        st.resets += 1
        st.failures.clear()
        st.stalled = False
        self.store.incr_runtime(camera_id, pipeline, "resets")
        self._log(pipeline, camera_id, f"RESET ({reason}); purged {purged} queued request(s)")

    def _thread_returned(self, pipe: Pipeline, camera_id: str, fut) -> None:
        self._stalled_threads.pop((pipe.name, camera_id), None)
        st = self.state(pipe.name, camera_id)
        err = fut.exception() if not fut.cancelled() else None
        msg = f"stalled thread returned{' with ' + repr(err) if err else ''}"
        asyncio.ensure_future(self.reset(pipe.name, camera_id, msg))

    # ── watchdog ─────────────────────────────────────────────────────────
    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            now = time.time()
            if self.malloc_trim_s > 0 and now - self._last_trim >= self.malloc_trim_s:
                self._last_trim = now
                before = rss_kb()
                await self.loop.run_in_executor(self.pool, malloc_trim)
                self.trims += 1
                freed = before - rss_kb()
                self.last_trim_mb = freed // 1024
                if freed > 100 * 1024:              # routine churn is not an event
                    self._log("-", "-", f"malloc_trim returned {freed // 1024} MB to the OS")
            for (name, cam), (t0, fut, req) in list(self._stalled_threads.items()):
                if not fut.done() and now - t0 > self.stall_exit_s:
                    self._log(name, cam, f"thread stuck for {now - t0:.0f}s on {req}; "
                                         "dumping all stacks and exiting for restart")
                    faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
                    sys.stderr.flush()
                    self._exit(EXIT_STUCK)
                    return
            for name, ws in self.workers.items():
                for w in ws:
                    if w.busy_since and now - w.busy_since > self.stall_exit_s:
                        # a worker stuck this long is a thread that never
                        # returned: nothing inside the process can free it
                        self._log(name, w.current.camera_id if w.current else "-",
                                  f"worker {w.index} stuck for {now - w.busy_since:.0f}s on "
                                  f"{w.current}; dumping all stacks and exiting for restart")
                        faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
                        sys.stderr.flush()
                        self._exit(EXIT_STUCK)
                        return

    # ── observability ────────────────────────────────────────────────────
    def _log(self, pipeline: str, camera_id: str, msg: str) -> None:
        line = f"[{camera_id}/{pipeline}] {msg}"
        log.debug(line)
        print(line, flush=True)          # stdout is the journal; a warning-level log duplicated every line
        self.events.append(f"{time.strftime('%H:%M:%S')} {line.splitlines()[0]}")
        del self.events[:-50]

    def health(self) -> dict:
        now = time.time()
        pipes = {}
        for name, pipe in self.pipelines.items():
            ws = self.workers.get(name, [])
            busy = sum(1 for w in ws if w.busy_since)
            uptime = max(now - self.started, 1e-6)
            occupancy = sum(w.busy_s + ((now - w.busy_since) if w.busy_since else 0) for w in ws) / (uptime * max(len(ws), 1))
            cams = {}
            for (pn, cid), st in self._states.items():
                if pn != name:
                    continue
                cams[cid] = {"runs": st.runs, "failures_recent": len(st.failures), "stalls": st.stalls,
                             "resets": st.resets, "stalled": st.stalled,
                             "last_latency_ms": st.last_latency_ms, "last_error": st.last_error or None,
                             "last_run_age_s": round(now - st.last_run_at, 1) if st.last_run_at else None}
            pipes[name] = {"mode": pipe.mode, "workers": len(ws), "busy": busy,
                           "occupancy": round(occupancy, 3), "budget_s": pipe.budget_s,
                           "queue": self.queues[name].snapshot(), "cameras": cams, **pipe.describe()}
        return {"service": "nn-video", "uptime_s": int(now - self.started),
                "settings_version": self.store.version, "pipelines": pipes,
                "rss_kb": rss_kb(), "cpu_pct": cpu_pct(), "malloc_trims": self.trims, "last_trim_mb": self.last_trim_mb,
                "stalled_threads": [{"pipeline": k[0], "camera": k[1], "age_s": round(now - v[0], 1)}
                                    for k, v in self._stalled_threads.items() if not v[1].done()],
                "threads": sorted(t.name for t in threading.enumerate()),
                "events": list(reversed(self.events[-20:]))}
