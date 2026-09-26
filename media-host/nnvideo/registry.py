"""Volatile connection registry: everything about a camera's CURRENT
connection that must never be persisted (decision 2026-09-21: no port is
fixed to a camera).

A camera connects to the one ingest port, is identified by its handshake
key, and from then on the accepted socket, its peer address, the session,
the loopback port its HLS muxer reads from and any live-view branch ports
are facts of THIS connection.  They live here, in memory, are shared with the
rest of the system through GET /cameras/{id}/runtime, and vanish with the
connection.
"""
from __future__ import annotations

import socket
import threading
import time
from typing import Any, Optional


class Registry:
    def __init__(self, ephemeral_range: tuple[int, int] = (40000, 49999)):
        self._lock = threading.Lock()
        self._conn: dict[str, dict] = {}
        self._lo, self._hi = ephemeral_range
        self._next = self._lo
        self._ports: dict[int, tuple[str, str]] = {}    # port -> (camera, purpose)

    # ── connections ──────────────────────────────────────────────────────
    def connect(self, camera_id: str, **facts: Any) -> dict:
        """Register a new connection for a camera (replacing any older one:
        a reconnecting camera is the same camera)."""
        with self._lock:
            self._conn.pop(camera_id, None)
            rec = {"camera_id": camera_id, "connected_at": time.time(),
                   "last_record_at": None, "records": 0, "bytes": 0}
            rec.update(facts)
            self._conn[camera_id] = rec
            return rec

    def disconnect(self, camera_id: str) -> Optional[dict]:
        """The connection is gone.  The camera's loopback ports are NOT
        freed here: they belong to its graph, which outlives a reconnect
        (cam3 reconnected after 11.5 h, 2026-09-22 10:00, and its ports
        would have been handed to the next graph built).  free_port() and
        forget() release them."""
        with self._lock:
            return self._conn.pop(camera_id, None)

    def forget(self, camera_id: str) -> None:
        """The camera is removed: connection and every port it held."""
        with self._lock:
            self._conn.pop(camera_id, None)
            self._free_ports_locked(camera_id)

    def touch(self, camera_id: str, nbytes: int = 0) -> None:
        rec = self._conn.get(camera_id)
        if rec is not None:
            rec["last_record_at"] = time.time()
            rec["records"] += 1
            rec["bytes"] += nbytes

    def update(self, camera_id: str, **facts: Any) -> None:
        with self._lock:
            rec = self._conn.get(camera_id)
            if rec is not None:
                rec.update(facts)

    def raw(self, camera_id: str) -> Optional[dict]:
        """The live record itself (socket, session, ctrl included) — for the
        pipelines and the handler, never for the API."""
        return self._conn.get(camera_id)

    def get(self, camera_id: str) -> Optional[dict]:
        rec = self._conn.get(camera_id)
        return None if rec is None else self._public(rec)

    def connected(self) -> list[str]:
        return sorted(self._conn)

    def all(self) -> dict[str, dict]:
        return {cid: self._public(r) for cid, r in self._conn.items()}

    # ── ephemeral ports (loopback helpers such as the HLS muxer feed) ────
    def allocate_port(self, camera_id: str, purpose: str) -> int:
        """A free port from the ephemeral range, bound-tested, recorded
        against the connection; freed with the connection."""
        with self._lock:
            for _ in range(self._hi - self._lo + 1):
                p = self._next
                self._next = self._lo if self._next >= self._hi else self._next + 1
                if p in self._ports:
                    continue
                with socket.socket() as s:
                    try:
                        s.bind(("127.0.0.1", p))
                    except OSError:
                        continue
                self._ports[p] = (camera_id, purpose)
                return p
        raise RuntimeError("no free ephemeral port")

    def free_port(self, port: int) -> None:
        """Give back one ephemeral port before its connection ends (a graph
        rebuilt by a reset allocates fresh ones)."""
        with self._lock:
            self._ports.pop(port, None)

    def _free_ports_locked(self, camera_id: str) -> None:
        for p in [p for p, (c, _) in self._ports.items() if c == camera_id]:
            del self._ports[p]

    def ports(self) -> dict[int, tuple[str, str]]:
        return dict(self._ports)

    def camera_ports(self, camera_id: str) -> dict[str, int]:
        return {purpose: p for p, (c, purpose) in self._ports.items() if c == camera_id}

    def _public(self, rec: dict) -> dict:
        out = {k: v for k, v in rec.items() if k not in ("socket", "session", "ctrl")}
        out["ports"] = self.camera_ports(rec["camera_id"])
        out["age_s"] = round(time.time() - rec["connected_at"], 1)
        out["idle_s"] = (round(time.time() - rec["last_record_at"], 1)
                         if rec.get("last_record_at") else None)
        return out
