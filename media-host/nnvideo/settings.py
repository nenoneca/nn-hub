"""Persistent settings: the media host's store, written only by the hub.

    cameras   one row per camera (identity + enabled flag; no ports)
    settings  (camera_id, pipeline, key) -> value, versioned
    runtime   counters and last-seen facts the pipelines record (not config)
    keys      device public keys and the host key pair(s) for the handshake

`Settings.get(camera_id, pipeline)` is called at the START OF EVERY RUN.  It
costs one integer compare: the store bumps a single global version on every
write; a cached dict is reused until that integer moves.  So a change made
through the API is live on the very next request, for every camera, and
survives a restart.  Defaults come from the pipeline classes, so an empty
store is a valid store.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS cameras (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL DEFAULT '',
    enabled     INTEGER NOT NULL DEFAULT 1,
    transport   TEXT NOT NULL DEFAULT 'sectun',
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS settings (
    camera_id   TEXT NOT NULL,
    pipeline    TEXT NOT NULL,
    key         TEXT NOT NULL,
    value       TEXT NOT NULL,          -- JSON
    version     INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (camera_id, pipeline, key)
);
CREATE TABLE IF NOT EXISTS runtime (
    camera_id   TEXT NOT NULL,
    pipeline    TEXT NOT NULL,
    key         TEXT NOT NULL,
    value       TEXT NOT NULL,          -- JSON
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (camera_id, pipeline, key)
);
CREATE TABLE IF NOT EXISTS keys (
    kind        TEXT NOT NULL,          -- 'device' | 'host'
    id          TEXT NOT NULL,          -- camera id | host key id
    pub         BLOB NOT NULL,
    priv        BLOB,                   -- host keys only
    host_key_id TEXT NOT NULL DEFAULT '',   -- device: which host key it was provisioned with
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (kind, id)
);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
"""
GLOBAL = "*"        # settings row that applies to every camera of a pipeline


class Store:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.execute("INSERT OR IGNORE INTO meta VALUES ('version', '1')")
            self._conn.commit()
        self._version = int(self._get_meta("version"))

    # ── version ──────────────────────────────────────────────────────────
    @property
    def version(self) -> int:
        return self._version

    def _bump(self) -> None:
        self._version += 1
        self._conn.execute("INSERT OR REPLACE INTO meta VALUES ('version', ?)", (str(self._version),))

    def _get_meta(self, k: str) -> str:
        row = self._conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return row["v"] if row else ""

    # ── cameras ──────────────────────────────────────────────────────────
    def add_camera(self, camera_id: str, name: str = "", enabled: bool = True,
                   transport: str = "sectun") -> dict:
        now = int(time.time())
        with self._lock:
            self._conn.execute(
                "INSERT INTO cameras (id, name, enabled, transport, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name, "
                "enabled=excluded.enabled, transport=excluded.transport, updated_at=excluded.updated_at",
                (camera_id, name or camera_id, int(enabled), transport, now, now))
            self._bump(); self._conn.commit()
        return self.camera(camera_id)

    def remove_camera(self, camera_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM cameras WHERE id=?", (camera_id,))
            self._conn.execute("DELETE FROM settings WHERE camera_id=?", (camera_id,))
            self._conn.execute("DELETE FROM runtime WHERE camera_id=?", (camera_id,))
            self._conn.execute("DELETE FROM keys WHERE kind='device' AND id=?", (camera_id,))
            self._bump(); self._conn.commit()
            return cur.rowcount > 0

    def camera(self, camera_id: str) -> Optional[dict]:
        row = self._conn.execute("SELECT * FROM cameras WHERE id=?", (camera_id,)).fetchone()
        return dict(row) if row else None

    def cameras(self, enabled_only: bool = False) -> list[dict]:
        q = "SELECT * FROM cameras" + (" WHERE enabled=1" if enabled_only else "") + " ORDER BY id"
        return [dict(r) for r in self._conn.execute(q).fetchall()]

    # ── settings ─────────────────────────────────────────────────────────
    def set_setting(self, camera_id: str, pipeline: str, key: str, value: Any) -> int:
        now = int(time.time())
        with self._lock:
            self._bump()
            self._conn.execute(
                "INSERT OR REPLACE INTO settings VALUES (?,?,?,?,?,?)",
                (camera_id, pipeline, key, json.dumps(value), self._version, now))
            self._conn.commit()
            return self._version

    def set_settings(self, camera_id: str, pipeline: str, values: dict) -> int:
        now = int(time.time())
        with self._lock:
            self._bump()
            for k, v in values.items():
                self._conn.execute(
                    "INSERT OR REPLACE INTO settings VALUES (?,?,?,?,?,?)",
                    (camera_id, pipeline, k, json.dumps(v), self._version, now))
            self._conn.commit()
            return self._version

    def delete_setting(self, camera_id: str, pipeline: str, key: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM settings WHERE camera_id=? AND pipeline=? AND key=?",
                               (camera_id, pipeline, key))
            self._bump(); self._conn.commit()

    def settings(self, camera_id: str, pipeline: str) -> dict:
        """Stored values only (global row then camera row, camera wins)."""
        out: dict = {}
        for cid in (GLOBAL, camera_id):
            for r in self._conn.execute(
                    "SELECT key, value FROM settings WHERE camera_id=? AND pipeline=?",
                    (cid, pipeline)).fetchall():
                out[r["key"]] = json.loads(r["value"])
        return out

    def settings_versioned(self, camera_id: str, pipeline: str) -> dict:
        rows = {}
        for cid in (GLOBAL, camera_id):
            for r in self._conn.execute(
                    "SELECT key, value, version, updated_at FROM settings WHERE camera_id=? AND pipeline=?",
                    (cid, pipeline)).fetchall():
                rows[r["key"]] = {"value": json.loads(r["value"]), "version": r["version"],
                                  "updated_at": r["updated_at"], "scope": "camera" if cid != GLOBAL else "global"}
        return rows

    # ── runtime facts ────────────────────────────────────────────────────
    def set_runtime(self, camera_id: str, pipeline: str, key: str, value: Any) -> None:
        with self._lock:
            self._conn.execute("INSERT OR REPLACE INTO runtime VALUES (?,?,?,?,?)",
                               (camera_id, pipeline, key, json.dumps(value), int(time.time())))
            self._conn.commit()

    def incr_runtime(self, camera_id: str, pipeline: str, key: str, by: int = 1) -> int:
        with self._lock:
            row = self._conn.execute("SELECT value FROM runtime WHERE camera_id=? AND pipeline=? AND key=?",
                                     (camera_id, pipeline, key)).fetchone()
            v = (json.loads(row["value"]) if row else 0) + by
            self._conn.execute("INSERT OR REPLACE INTO runtime VALUES (?,?,?,?,?)",
                               (camera_id, pipeline, key, json.dumps(v), int(time.time())))
            self._conn.commit()
            return v

    def runtime(self, camera_id: str, pipeline: str | None = None) -> dict:
        q = "SELECT pipeline, key, value FROM runtime WHERE camera_id=?"
        args: tuple = (camera_id,)
        if pipeline:
            q += " AND pipeline=?"; args = (camera_id, pipeline)
        out: dict = {}
        for r in self._conn.execute(q, args).fetchall():
            out.setdefault(r["pipeline"], {})[r["key"]] = json.loads(r["value"])
        return out

    # ── keys ─────────────────────────────────────────────────────────────
    def set_device_key(self, camera_id: str, pub: bytes, host_key_id: str = "default") -> None:
        with self._lock:
            self._conn.execute("INSERT OR REPLACE INTO keys VALUES ('device', ?, ?, NULL, ?, ?)",
                               (camera_id, bytes(pub), host_key_id, int(time.time())))
            self._bump(); self._conn.commit()

    def camera_for_device_pub(self, pub: bytes) -> Optional[tuple[str, str]]:
        """(camera_id, host_key_id) for a device public key seen in a HELLO."""
        row = self._conn.execute("SELECT id, host_key_id FROM keys WHERE kind='device' AND pub=?",
                                 (bytes(pub),)).fetchone()
        return (row["id"], row["host_key_id"]) if row else None

    def camera_for_host_key(self, key_id: str) -> Optional[str]:
        """The camera a legacy per-slot host key belongs to (settings row
        stream.host_key_id), or the key id itself when a camera carries it."""
        row = self._conn.execute(
            "SELECT camera_id FROM settings WHERE pipeline='stream' AND key='host_key_id' AND value=?",
            (json.dumps(key_id),)).fetchone()
        if row:
            return row["camera_id"]
        return key_id if self.camera(key_id) else None

    def set_host_key(self, key_id: str, pub: bytes, priv: bytes) -> None:
        with self._lock:
            self._conn.execute("INSERT OR REPLACE INTO keys VALUES ('host', ?, ?, ?, '', ?)",
                               (key_id, bytes(pub), bytes(priv), int(time.time())))
            self._conn.commit()

    def host_key(self, key_id: str = "default") -> Optional[tuple[bytes, bytes]]:
        row = self._conn.execute("SELECT pub, priv FROM keys WHERE kind='host' AND id=?", (key_id,)).fetchone()
        return (bytes(row["pub"]), bytes(row["priv"])) if row else None

    def host_key_ids(self) -> list[str]:
        return [r["id"] for r in self._conn.execute("SELECT id FROM keys WHERE kind='host' ORDER BY id")]

    def close(self) -> None:
        self._conn.close()


class Settings:
    """Per-run view: defaults from the pipeline class + stored values, cached
    by store version.  `get` is what a pipeline calls at the start of a run."""

    def __init__(self, store: Store):
        self._store = store
        self._defaults: dict[str, dict] = {}
        self._cache: dict[tuple[str, str], tuple[int, dict]] = {}
        self.reloads = 0

    def register_defaults(self, pipeline: str, defaults: dict) -> None:
        self._defaults[pipeline] = dict(defaults)
        self._cache = {k: v for k, v in self._cache.items() if k[1] != pipeline}

    def get(self, camera_id: str, pipeline: str) -> dict:
        v = self._store.version
        hit = self._cache.get((camera_id, pipeline))
        if hit is not None and hit[0] == v:
            return hit[1]
        merged = dict(self._defaults.get(pipeline, {}))
        merged.update(self._store.settings(camera_id, pipeline))
        self._cache[(camera_id, pipeline)] = (v, merged)
        self.reloads += 1
        return merged

    @property
    def version(self) -> int:
        return self._store.version
