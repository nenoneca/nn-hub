"""Phase 4 — rotating SQLite store for device logs.

Layout::

    <data_dir>/logs/
        logs_<unix_ts>.db          ← oldest rotation file
        ...
        logs_<unix_ts>.db          ← current (newest)

Each file holds the same schema (``events`` table).  When the active file
hits the configured rotation interval, ``LogStore.maybe_rotate()``
closes the current connection and opens a fresh one with a new name —
no rename gymnastics, no symlink.  The "current" file is just whichever
is newest.

Append path is intentionally synchronous (sqlite3 is ~10 µs per insert
at this volume) and called from the asyncio event loop.  If you ever
need to scale: wrap append() in ``loop.run_in_executor``.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

log = logging.getLogger("hub.log_store")

# ── schema ────────────────────────────────────────────────────────────────────

_SCHEMA = """
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id   TEXT    NOT NULL DEFAULT '',
    upload_ts   INTEGER NOT NULL,             -- hub epoch (sec)
    log_ts      TEXT    NOT NULL DEFAULT '',  -- device-side timestamp string
    level       TEXT    NOT NULL DEFAULT '',  -- inf | wrn | err | dbg
    tag         TEXT    NOT NULL DEFAULT '',  -- module name
    content     TEXT    NOT NULL,             -- the message body
    fw_version  TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS events_dev_ts ON events(device_id, upload_ts DESC);
CREATE INDEX IF NOT EXISTS events_level   ON events(level, upload_ts DESC);
"""


# ── parsing ───────────────────────────────────────────────────────────────────

# Zephyr text format with FLAG_LEVEL + FLAG_TIMESTAMP looks like:
#   [00:01:23.456,000] <inf> ncp_host: auto: wifi connected
# The trailing content can include any printable chars and may itself
# contain ":" / brackets, so we use a non-greedy module match.
_ZEPHYR_RE = re.compile(
    r"^\s*\[(?P<ts>[0-9:.,]+)\]\s+"
    r"<(?P<lvl>[a-z]+)>\s+"
    r"(?P<tag>[A-Za-z0-9_]+)\s*:\s*"
    r"(?P<content>.*)$"
)


@dataclass(frozen=True)
class ParsedLine:
    log_ts:  str
    level:   str
    tag:     str
    content: str


def parse_zephyr_line(text: str) -> ParsedLine:
    """Parse a single Zephyr-format log line.  Falls back to (empty
    metadata + raw text in content) for lines that don't match — so
    nothing is ever silently dropped."""
    m = _ZEPHYR_RE.match(text.rstrip())
    if not m:
        return ParsedLine(log_ts="", level="", tag="", content=text.rstrip())
    return ParsedLine(
        log_ts=m.group("ts"),
        level=m.group("lvl"),
        tag=m.group("tag"),
        content=m.group("content"),
    )


# ── store ─────────────────────────────────────────────────────────────────────


def _file_order(p: Path):
    """Sort key: the start timestamp in logs_<ts>.db; a malformed name falls
    back to its mtime so it still sorts somewhere sensible."""
    try:
        return (int(p.stem.split("_", 1)[1]), 0.0)
    except (IndexError, ValueError):
        try:
            return (int(p.stat().st_mtime), 1.0)
        except OSError:
            return (0, 2.0)


class LogStore:
    """Persistent rolling store for device logs.

    Parameters
    ----------
    log_dir
        Directory holding ``logs_<ts>.db`` files (created if missing).
    rotate_interval_sec
        Seconds between rotations.  Default 1 hour (3600).  A new file
        is opened whenever the current active file is older than this.
    """

    def __init__(self, log_dir: Path, rotate_interval_sec: int = 3600,
                 max_mb: int = 256):
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._interval = int(rotate_interval_sec)
        # Total-size cap across rotation files; oldest files are deleted
        # first.  Adjustable at runtime (Settings → General) via set_cap_mb.
        self._max_bytes = max(16, int(max_mb)) * 1024 * 1024
        self._conn: Optional[sqlite3.Connection] = None
        self._current_path: Optional[Path] = None
        self._current_started_at: int = 0
        self._open_or_resume()

    # — internal —

    def _open_new_file(self, started_at: Optional[int] = None) -> None:
        ts = int(started_at if started_at is not None else time.time())
        path = self._log_dir / f"logs_{ts}.db"
        # If a file with this exact name already exists (rotation hit
        # within the same second the previous file opened), bump by 1
        # until we find a free slot.  Keeps filenames monotonic.
        while path.exists():
            ts += 1
            path = self._log_dir / f"logs_{ts}.db"
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._current_path = path
        self._current_started_at = ts
        log.info("log_store: opened %s", path.name)

    def _open_or_resume(self) -> None:
        """If there's a recent file we can keep appending to, reuse it;
        otherwise start a new one."""
        candidates = self.list_files()
        now = int(time.time())
        if candidates:
            newest = candidates[-1]
            try:
                started = int(newest.stem.split("_", 1)[1])
            except (IndexError, ValueError):
                started = int(newest.stat().st_mtime)
            if now - started < self._interval:
                # Still inside the rotation window — resume.
                self._conn = sqlite3.connect(str(newest), check_same_thread=False)
                self._conn.executescript(_SCHEMA)
                self._conn.commit()
                self._current_path = newest
                self._current_started_at = started
                log.info("log_store: resumed %s", newest.name)
                return
        self._open_new_file(started_at=now)

    def maybe_rotate(self) -> None:
        if not self._conn:
            self._open_or_resume()
            return
        if int(time.time()) - self._current_started_at >= self._interval:
            log.info("log_store: rotating (interval %ds elapsed)", self._interval)
            self._open_new_file()
            self.prune()

    def set_cap_mb(self, mb: int) -> None:
        self._max_bytes = max(16, int(mb)) * 1024 * 1024
        self.prune()

    @property
    def cap_mb(self) -> int:
        return self._max_bytes // (1024 * 1024)

    def total_bytes(self) -> int:
        total = 0
        for f in self._log_dir.glob("logs_*.db*"):     # includes -wal/-shm
            try:
                total += f.stat().st_size
            except OSError:
                pass
        return total

    def prune(self) -> None:
        """Delete oldest rotation files until the store fits the cap.
        The current (open) file is never deleted, so a single oversized
        hour can transiently exceed the cap rather than lose live data."""
        files = self.list_files()
        total = self.total_bytes()
        removed = 0
        for path in files:                              # oldest first
            if total <= self._max_bytes:
                break
            if path == self._current_path:
                continue                                # never the live file; older ones may follow
            freed = 0
            for f in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
                try:
                    freed += f.stat().st_size
                    f.unlink()
                except OSError:
                    pass
            total -= freed
            removed += 1
        if removed:
            log.info("log_store: pruned %d file(s), now %.1f MB (cap %d MB)",
                     removed, total / 1048576, self.cap_mb)

    # — public —

    @property
    def current_path(self) -> Optional[Path]:
        return self._current_path

    def list_files(self) -> list[Path]:
        """Oldest first, by the start time in the file NAME (logs_<ts>.db,
        monotonic by construction).  Ordering by mtime was flaky: the kernel's
        coarse file clock let the live file tie with or sort ahead of older
        ones, and prune() stopped at it with old files still over the cap."""
        return sorted(self._log_dir.glob("logs_*.db"), key=_file_order)

    def append(self, *,
               device_id: str,
               content: str,
               log_ts: str = "",
               level: str = "",
               tag: str = "",
               fw_version: str = "",
               upload_ts: Optional[int] = None) -> None:
        self.maybe_rotate()
        assert self._conn is not None
        ts = int(upload_ts if upload_ts is not None else time.time())
        self._conn.execute(
            "INSERT INTO events (device_id, upload_ts, log_ts, level, tag, "
            " content, fw_version) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (device_id, ts, log_ts, level, tag, content, fw_version),
        )
        self._conn.commit()

    def append_text(self, *, device_id: str, text: str,
                    fw_version: str = "",
                    upload_ts: Optional[int] = None) -> None:
        """Convenience: parses Zephyr text format then appends."""
        p = parse_zephyr_line(text)
        self.append(device_id=device_id, content=p.content,
                    log_ts=p.log_ts, level=p.level, tag=p.tag,
                    fw_version=fw_version, upload_ts=upload_ts)

    def query(self, *,
              device_id: Optional[str] = None,
              level: Optional[str] = None,
              tag: Optional[str] = None,
              since_ts: Optional[int] = None,
              limit: int = 50) -> list[dict]:
        """Query across all rotation files.  Newest entries first."""
        rows: list[dict] = []
        for path in reversed(self.list_files()):
            # Skip files entirely older than the cutoff.
            if since_ts is not None and path.stat().st_mtime < since_ts:
                continue
            need = limit - len(rows)
            if need <= 0:
                break
            try:
                conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
                conn.row_factory = sqlite3.Row
            except sqlite3.Error as e:
                log.warning("log_store: skip %s: %s", path.name, e)
                continue
            try:
                clauses, params = [], []
                if device_id:
                    clauses.append("device_id = ?"); params.append(device_id)
                if level:
                    clauses.append("level = ?");    params.append(level)
                if tag:
                    clauses.append("tag = ?");      params.append(tag)
                if since_ts is not None:
                    clauses.append("upload_ts >= ?"); params.append(since_ts)
                where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
                params.append(need)
                cur = conn.execute(
                    f"SELECT * FROM events {where} "
                    f"ORDER BY upload_ts DESC, id DESC LIMIT ?",
                    tuple(params),
                )
                for r in cur.fetchall():
                    rows.append(dict(r))
            finally:
                conn.close()
        return rows[:limit]

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
