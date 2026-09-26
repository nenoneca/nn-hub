"""Tests for hub.firmware_catalog orchestrator + sources.yaml round-trip."""

import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from hub.db import DB
from hub.firmware_catalog import FirmwareCatalog
from hub.firmware_sources.base import MCUBOOT_IMAGE_MAGIC


pytestmark = pytest.mark.asyncio


def _seed_local(src_root: Path, device_type: str, version: str) -> tuple[str, int]:
    """Write a valid image+manifest pair under <src_root>/<dt>/<ver>/."""
    payload = MCUBOOT_IMAGE_MAGIC + b"body-" * 256
    d = src_root / device_type / version
    d.mkdir(parents=True)
    (d / "image.signed.bin").write_bytes(payload)
    sha = hashlib.sha256(payload).hexdigest()
    manifest = {
        "schema": 1,
        "device_type": device_type, "version": version,
        "sha256": sha, "size_bytes": len(payload),
        "mcuboot": {"image_magic": "0x96f3b83d"},
    }
    (d / "manifest.json").write_text(json.dumps(manifest))
    return sha, len(payload)


def _write_sources_yaml(path: Path, src_root: Path, name: str = "lab") -> None:
    path.write_text(
        f"sources:\n"
        f"  - name: {name}\n"
        f"    kind: local\n"
        f"    root: {src_root}\n"
        f"    poll_seconds: 0\n"
    )


async def test_sync_then_promote_writes_firmware_target(tmp_path: Path):
    src_root = tmp_path / "fw"
    cache    = tmp_path / "cache"
    db_path  = tmp_path / "h.db"
    sy       = tmp_path / "sources.yaml"

    sha, size = _seed_local(src_root, "sample_c6", "2.13.0")
    _write_sources_yaml(sy, src_root)

    db = DB(db_path)
    cat = FirmwareCatalog(db, cache, sy)
    assert cat.source_names() == ["lab"]
    assert (await cat.sync_all()) == {"lab": 1}

    [entry] = db.list_catalog()
    assert entry.is_cached is False
    assert entry.sha256 == sha

    result = await cat.promote("sample_c6", "2.13.0")
    assert result["sha256"] == sha
    assert result["size_bytes"] == size
    assert Path(result["path"]).is_file()
    target = db.get_firmware_target("sample_c6")
    assert target is not None
    assert target.target_version == "2.13.0"


async def test_sync_is_idempotent_and_preserves_cache(tmp_path: Path):
    src_root = tmp_path / "fw"
    cache    = tmp_path / "cache"
    sy       = tmp_path / "sources.yaml"
    _seed_local(src_root, "sample_c6", "2.13.0")
    _write_sources_yaml(sy, src_root)

    db = DB(tmp_path / "h.db")
    cat = FirmwareCatalog(db, cache, sy)
    await cat.sync_all()
    await cat.promote("sample_c6", "2.13.0")
    first_path = db.get_catalog_entry("sample_c6", "2.13.0").local_path
    first_downloaded_at = db.get_catalog_entry("sample_c6", "2.13.0").downloaded_at
    assert first_path is not None

    # Re-sync should NOT clear the cached state.
    await cat.sync_all()
    after = db.get_catalog_entry("sample_c6", "2.13.0")
    assert after.local_path == first_path
    assert after.downloaded_at == first_downloaded_at


async def test_multi_source_same_image(tmp_path: Path):
    """Two sources offering the same (device_type, version) co-exist as
    distinct catalog rows; promote picks one."""
    src_root_a = tmp_path / "fw-a"
    src_root_b = tmp_path / "fw-b"
    _seed_local(src_root_a, "sample_c6", "2.13.0")
    _seed_local(src_root_b, "sample_c6", "2.13.0")

    sy = tmp_path / "sources.yaml"
    sy.write_text(
        f"sources:\n"
        f"  - name: a\n    kind: local\n    root: {src_root_a}\n    poll_seconds: 0\n"
        f"  - name: b\n    kind: local\n    root: {src_root_b}\n    poll_seconds: 0\n"
    )
    db = DB(tmp_path / "h.db")
    cat = FirmwareCatalog(db, tmp_path / "cache", sy)
    counts = await cat.sync_all()
    assert counts == {"a": 1, "b": 1}
    rows = db.list_catalog("sample_c6")
    assert {r.source_name for r in rows} == {"a", "b"}

    # Default promote picks whichever the DB prefers (cached if any —
    # but neither is cached yet, so just whichever last_seen is more
    # recent).  Explicit source pin is robust:
    res = await cat.promote("sample_c6", "2.13.0", source_name="b")
    assert res["source"] == "b"


async def test_promote_with_no_catalog_entry_raises(tmp_path: Path):
    sy = tmp_path / "sources.yaml"
    sy.write_text("sources: []\n")
    db = DB(tmp_path / "h.db")
    cat = FirmwareCatalog(db, tmp_path / "cache", sy)
    with pytest.raises(ValueError, match="no catalog entry"):
        await cat.promote("sample_c6", "9.9.9")


async def test_reload_picks_up_yaml_changes(tmp_path: Path):
    src_root = tmp_path / "fw"
    _seed_local(src_root, "sample_c6", "2.13.0")
    sy = tmp_path / "sources.yaml"

    sy.write_text("sources: []\n")
    db = DB(tmp_path / "h.db")
    cat = FirmwareCatalog(db, tmp_path / "cache", sy)
    assert cat.source_names() == []

    _write_sources_yaml(sy, src_root)
    cat.reload()
    assert cat.source_names() == ["lab"]


async def test_missing_sources_yaml_is_not_fatal(tmp_path: Path):
    db = DB(tmp_path / "h.db")
    # Path doesn't exist — should be a quiet no-op.
    cat = FirmwareCatalog(db, tmp_path / "cache", tmp_path / "absent.yaml")
    assert cat.source_names() == []
    assert (await cat.sync_all()) == {}
