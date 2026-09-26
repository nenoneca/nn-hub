"""Tests for hub.log_store.LogStore (Phase 4)."""

import time
from pathlib import Path

import pytest

from hub.log_store import LogStore, parse_zephyr_line


def test_parse_zephyr_typical():
    p = parse_zephyr_line(
        "[00:01:23.456,000] <inf> ncp_host: auto: wifi connected")
    assert p.log_ts == "00:01:23.456,000"
    assert p.level  == "inf"
    assert p.tag    == "ncp_host"
    assert p.content == "auto: wifi connected"


def test_parse_zephyr_warning_with_colon_in_content():
    p = parse_zephyr_line(
        "[00:00:01.123,000] <wrn> proto_tcp: connect(192.0.2.10:8767): 22")
    assert p.level == "wrn"
    assert p.tag   == "proto_tcp"
    assert "192.0.2.10:8767" in p.content


def test_parse_zephyr_unparseable_falls_back_to_raw():
    p = parse_zephyr_line("garbage line that doesn't match")
    assert p.level == ""
    assert p.tag   == ""
    assert p.content == "garbage line that doesn't match"


def test_append_and_query_roundtrip(tmp_path: Path):
    store = LogStore(tmp_path, rotate_interval_sec=3600)
    store.append_text(
        device_id="dev-A",
        text="[00:00:01.000,000] <inf> ncp_host: hello",
    )
    store.append_text(
        device_id="dev-A",
        text="[00:00:02.000,000] <wrn> wifi: lost ap",
    )
    store.append_text(
        device_id="dev-B",
        text="[00:00:03.000,000] <err> proto_tcp: connect failed",
    )
    rows = store.query(limit=10)
    assert len(rows) == 3
    # Newest first
    assert rows[0]["device_id"] == "dev-B"
    assert rows[0]["level"]     == "err"

    only_a = store.query(device_id="dev-A", limit=10)
    assert {r["device_id"] for r in only_a} == {"dev-A"}

    only_warn = store.query(level="wrn", limit=10)
    assert len(only_warn) == 1 and only_warn[0]["tag"] == "wifi"

    by_tag = store.query(tag="ncp_host", limit=10)
    assert len(by_tag) == 1 and by_tag[0]["content"] == "hello"


def test_rotation_creates_new_file(tmp_path: Path):
    # An hour-long window, force-elapsed by backdating: a 1 s window let the
    # clock tick between construction and the first append, which rotated
    # an extra time (3 files, flaky in full runs).
    store = LogStore(tmp_path, rotate_interval_sec=3600)
    store.append_text(device_id="d", text="line 1")
    file1 = store.current_path

    # Backdate the start time so the next append rotates.
    store._current_started_at = int(time.time()) - 7200
    store.append_text(device_id="d", text="line 2")
    file2 = store.current_path
    assert file1 != file2

    # Both files exist
    files = store.list_files()
    assert len(files) == 2
    assert file1 in files and file2 in files

    # Query traverses both rotation files
    rows = store.query(limit=10)
    contents = {r["content"] for r in rows}
    assert contents == {"line 1", "line 2"}


def test_resume_existing_file(tmp_path: Path):
    """A second LogStore() pointed at the same dir within the rotation
    window should resume the most-recent file (not start a new one)."""
    s1 = LogStore(tmp_path, rotate_interval_sec=3600)
    s1.append_text(device_id="d", text="first run")
    file1 = s1.current_path
    s1.close()

    s2 = LogStore(tmp_path, rotate_interval_sec=3600)
    assert s2.current_path == file1
    s2.append_text(device_id="d", text="second run")

    rows = s2.query(limit=10)
    assert len(rows) == 2


# ── size cap / prune (added with the Settings > General cap) ─────────────────

def _make_old_files(d: Path, n: int, mb: int = 1):
    for i in range(n):
        (d / f"logs_{1000000 + i}.db").write_bytes(b"x" * mb * 1024 * 1024)


def test_prune_deletes_oldest_first(tmp_path: Path):
    d = tmp_path / "logs"
    d.mkdir()
    _make_old_files(d, 5)
    st = LogStore(d, rotate_interval_sec=3600, max_mb=16)
    st.append(device_id="t", content="live")
    n_before = len(st.list_files())

    st._max_bytes = 2 * 1024 * 1024
    st.prune()
    files = st.list_files()
    assert len(files) < n_before
    # survivors are the NEWEST ones — the oldest names must be gone
    assert not (d / "logs_1000000.db").exists()
    assert st.total_bytes() <= 2 * 1024 * 1024 + st.current_path.stat().st_size


def test_prune_never_deletes_current_file(tmp_path: Path):
    d = tmp_path / "logs"
    d.mkdir()
    _make_old_files(d, 3)
    st = LogStore(d, rotate_interval_sec=3600, max_mb=16)
    st.append(device_id="t", content="live")
    st._max_bytes = 1                       # absurd cap: below any file size
    st.prune()
    assert st.current_path.exists()
    st.append(device_id="t", content="still writable")   # store still works


def test_set_cap_clamps_and_applies(tmp_path: Path):
    st = LogStore(tmp_path / "logs", rotate_interval_sec=3600, max_mb=256)
    st.set_cap_mb(4)                        # below the 16 MB floor
    assert st.cap_mb == 16
    st.set_cap_mb(512)
    assert st.cap_mb == 512
