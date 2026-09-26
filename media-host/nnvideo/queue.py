"""One flat FIFO per pipeline, N workers, per-camera order kept.

Decision (2026-09-21): a flat FIFO shared by all cameras, scaled by worker
count.  Several workers on one FIFO could run two requests of the same
camera concurrently and deliver, say, two access units to the graph out of
order — so the queue carries an IN-FLIGHT GUARD per camera: a worker that
takes a request whose camera already has a run in progress parks it on that
camera's hold list; the hold list is released, in order, when the run ends.
Cameras interleave freely; a camera's own requests never overtake each
other.

Policies, per (camera, kind), chosen by the pipeline:
    DROP_OLDEST  real-time media: when the per-(camera, kind) cap is reached
                 the oldest such item is dropped and counted
    BLOCK        durable work (events): the submitter waits for room
    COALESCE     control: only the latest item of that (camera, kind) is kept
"""
from __future__ import annotations

import asyncio
import collections
import time
from dataclasses import dataclass, field

from .request import Request


class Policy:
    DROP_OLDEST = "drop_oldest"
    BLOCK = "block"
    COALESCE = "coalesce"


class QueueFull(Exception):
    pass


@dataclass
class QueueStats:
    submitted: int = 0
    taken: int = 0
    dropped: int = 0
    coalesced: int = 0
    held: int = 0            # requests that waited on the in-flight guard
    max_depth: int = 0
    lat_sum: float = 0.0     # arrival -> taken
    lat_n: int = 0

    def snapshot(self) -> dict:
        return {"submitted": self.submitted, "taken": self.taken, "dropped": self.dropped,
                "coalesced": self.coalesced, "held": self.held, "max_depth": self.max_depth,
                "avg_wait_ms": round(1000 * self.lat_sum / self.lat_n, 1) if self.lat_n else 0.0}


class PipelineQueue:
    def __init__(self, name: str, *, policy: str = Policy.DROP_OLDEST,
                 per_camera_cap: int = 64, total_cap: int = 4096):
        self.name = name
        self.policy = policy
        self.per_camera_cap = per_camera_cap
        self.total_cap = total_cap
        self._q: collections.deque[Request] = collections.deque()
        self._counts: dict[tuple[str, str], int] = {}     # (camera, kind) -> queued
        self._inflight: set[str] = set()                   # cameras with a run in progress
        self._hold: dict[str, collections.deque[Request]] = {}
        self._cv = asyncio.Condition()
        self.stats = QueueStats()
        self.closed = False

    # ── submit ───────────────────────────────────────────────────────────
    async def put(self, req: Request) -> bool:
        """Enqueue under the policy.  Returns False if the request was dropped
        (DROP_OLDEST drops the OLDEST of its (camera, kind), so the newest is
        always kept; COALESCE replaces the older one)."""
        key = (req.camera_id, req.kind)
        async with self._cv:
            if self.closed:
                return False
            self.stats.submitted += 1
            if self.policy == Policy.COALESCE:
                self._remove_all(key)
            elif self._counts.get(key, 0) >= self.per_camera_cap or len(self._q) >= self.total_cap:
                if self.policy == Policy.BLOCK:
                    while (self._counts.get(key, 0) >= self.per_camera_cap
                           or len(self._q) >= self.total_cap) and not self.closed:
                        await self._cv.wait()
                    if self.closed:
                        return False
                else:
                    self._drop_oldest(key)
            self._q.append(req)
            self._counts[key] = self._counts.get(key, 0) + 1
            self.stats.max_depth = max(self.stats.max_depth, len(self._q))
            self._cv.notify()
            return True

    def _remove_all(self, key) -> None:
        keep = collections.deque()
        for r in self._q:
            if (r.camera_id, r.kind) == key:
                self.stats.coalesced += 1
            else:
                keep.append(r)
        self._q = keep
        self._counts[key] = 0

    def _drop_oldest(self, key) -> None:
        for i, r in enumerate(self._q):
            if (r.camera_id, r.kind) == key:
                del self._q[i]
                self._counts[key] -= 1
                self.stats.dropped += 1
                return
        # cap reached on the total: drop the oldest of anyone with the same kind
        for i, r in enumerate(self._q):
            if r.kind == key[1]:
                del self._q[i]
                self._counts[(r.camera_id, r.kind)] -= 1
                self.stats.dropped += 1
                return
        if self._q:
            r = self._q.popleft()
            self._counts[(r.camera_id, r.kind)] -= 1
            self.stats.dropped += 1

    # ── take / release (the in-flight guard) ─────────────────────────────
    async def take(self) -> Request | None:
        """Next request whose camera has no run in progress; marks the camera
        in flight.  Returns None when the queue is closed and drained."""
        async with self._cv:
            while True:
                if self.closed and not self._q and not any(self._hold.values()):
                    return None
                # released hold lists first (they are older than anything queued)
                for cam, h in self._hold.items():
                    if h and cam not in self._inflight:
                        req = h.popleft()
                        self._inflight.add(cam)
                        return self._taken(req)
                for i, req in enumerate(self._q):
                    if req.camera_id in self._inflight:
                        continue
                    del self._q[i]
                    self._counts[(req.camera_id, req.kind)] -= 1
                    self._inflight.add(req.camera_id)
                    self._cv.notify_all()          # room for a BLOCKed submitter
                    return self._taken(req)
                # everything queued belongs to cameras already in flight:
                # park those requests so they keep their order and wait
                moved = 0
                while self._q and self._q[0].camera_id in self._inflight:
                    r = self._q.popleft()
                    self._counts[(r.camera_id, r.kind)] -= 1
                    self._hold.setdefault(r.camera_id, collections.deque()).append(r)
                    self.stats.held += 1
                    moved += 1
                if moved:
                    self._cv.notify_all()
                await self._cv.wait()

    def _taken(self, req: Request) -> Request:
        self.stats.taken += 1
        self.stats.lat_sum += time.time() - req.ts_arrival
        self.stats.lat_n += 1
        return req

    async def release(self, camera_id: str) -> None:
        """The run for this camera finished: its next held request may go."""
        async with self._cv:
            self._inflight.discard(camera_id)
            self._cv.notify_all()

    async def purge(self, camera_id: str) -> int:
        """Drop everything queued or held for a camera (reset path)."""
        async with self._cv:
            before = len(self._q)
            self._q = collections.deque(r for r in self._q if r.camera_id != camera_id)
            n = before - len(self._q)
            h = self._hold.pop(camera_id, None)
            n += len(h) if h else 0
            for k in list(self._counts):
                if k[0] == camera_id:
                    self._counts[k] = 0
            self.stats.dropped += n
            self._cv.notify_all()
            return n

    async def close(self) -> None:
        async with self._cv:
            self.closed = True
            self._cv.notify_all()

    # ── introspection ────────────────────────────────────────────────────
    @property
    def depth(self) -> int:
        return len(self._q) + sum(len(h) for h in self._hold.values())

    def oldest_age_s(self) -> float | None:
        oldest = None
        for r in self._q:
            oldest = r.ts_arrival if oldest is None else min(oldest, r.ts_arrival)
        for h in self._hold.values():
            for r in h:
                oldest = r.ts_arrival if oldest is None else min(oldest, r.ts_arrival)
        return None if oldest is None else round(time.time() - oldest, 3)

    def snapshot(self) -> dict:
        per_cam: dict[str, int] = {}
        for r in self._q:
            per_cam[r.camera_id] = per_cam.get(r.camera_id, 0) + 1
        for cam, h in self._hold.items():
            per_cam[cam] = per_cam.get(cam, 0) + len(h)
        return {"name": self.name, "policy": self.policy, "depth": self.depth,
                "oldest_age_s": self.oldest_age_s(), "in_flight": sorted(self._inflight),
                "per_camera": per_cam, **self.stats.snapshot()}
