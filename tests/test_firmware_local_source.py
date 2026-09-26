"""Tests for hub.firmware_sources.LocalDirSource."""

import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from hub.firmware_sources import LocalDirSource
from hub.firmware_sources.base import (
    MCUBOOT_IMAGE_MAGIC,
    Manifest,
    ManifestError,
)


pytestmark = pytest.mark.asyncio


def _make_image(version_dir: Path, device_type: str, version: str,
                extra_payload: bytes = b"x" * 256,
                manifest_overrides: dict | None = None,
                magic: bytes = MCUBOOT_IMAGE_MAGIC) -> tuple[str, int]:
    """Synthesize an image.signed.bin + manifest.json pair.  Returns sha256."""
    version_dir.mkdir(parents=True, exist_ok=True)
    data = magic + extra_payload
    (version_dir / "image.signed.bin").write_bytes(data)
    sha = hashlib.sha256(data).hexdigest()
    manifest = {
        "schema": 1,
        "device_type": device_type,
        "version": version,
        "sha256": sha,
        "size_bytes": len(data),
        "mcuboot": {"image_magic": "0x96f3b83d"},
        "build_meta": {"git_sha": "cafef00d"},
    }
    if manifest_overrides:
        manifest.update(manifest_overrides)
    (version_dir / "manifest.json").write_text(json.dumps(manifest))
    return sha, len(data)


async def test_list_available_picks_up_well_formed_images(tmp_path: Path):
    src_root = tmp_path / "fw"
    _make_image(src_root / "sample_c6" / "2.13.0", "sample_c6", "2.13.0")
    _make_image(src_root / "sample_c6" / "2.14.0", "sample_c6", "2.14.0")
    _make_image(src_root / "esp_tbr"   / "1.0.0",  "esp_tbr",   "1.0.0")

    src = LocalDirSource("lab", src_root)
    images = await src.list_available()
    assert len(images) == 3
    triples = {(i.device_type, i.version, i.source_name) for i in images}
    assert ("sample_c6", "2.13.0", "lab") in triples
    assert ("esp_tbr",   "1.0.0",  "lab") in triples


async def test_list_available_skips_incomplete_dirs(tmp_path: Path):
    src_root = tmp_path / "fw"
    # only image, no manifest
    (src_root / "sample_c6" / "2.13.0").mkdir(parents=True)
    (src_root / "sample_c6" / "2.13.0" / "image.signed.bin").write_bytes(b"x")
    # only manifest, no image
    (src_root / "sample_c6" / "2.14.0").mkdir(parents=True)
    (src_root / "sample_c6" / "2.14.0" / "manifest.json").write_text("{}")
    # well-formed control
    _make_image(src_root / "sample_c6" / "2.15.0", "sample_c6", "2.15.0")

    images = await LocalDirSource("lab", src_root).list_available()
    assert len(images) == 1
    assert images[0].version == "2.15.0"


async def test_list_available_skips_when_manifest_disagrees_with_dir(tmp_path: Path):
    src_root = tmp_path / "fw"
    # manifest says version 9.9.9 but dir says 2.13.0 — skip
    _make_image(src_root / "sample_c6" / "2.13.0", "sample_c6", "9.9.9")
    images = await LocalDirSource("lab", src_root).list_available()
    assert images == []


async def test_list_available_skips_manifest_with_bad_sha(tmp_path: Path):
    src_root = tmp_path / "fw"
    _make_image(
        src_root / "sample_c6" / "2.13.0", "sample_c6", "2.13.0",
        manifest_overrides={"sha256": "z" * 64},   # invalid hex
    )
    images = await LocalDirSource("lab", src_root).list_available()
    assert images == []


async def test_fetch_verifies_sha_and_writes_atomically(tmp_path: Path):
    src_root = tmp_path / "fw"
    cache = tmp_path / "cache" / "image.bin"
    _make_image(src_root / "sample_c6" / "2.13.0", "sample_c6", "2.13.0")

    src = LocalDirSource("lab", src_root)
    [img] = await src.list_available()
    path = await src.fetch(img, cache)
    assert path == cache
    assert cache.is_file()
    # No leftover .tmp
    assert not cache.with_suffix(".bin.tmp").exists()
    # Re-verify by sha
    actual = hashlib.sha256(cache.read_bytes()).hexdigest()
    assert actual == img.sha256


async def test_fetch_rejects_when_binary_does_not_match_manifest_sha(tmp_path: Path):
    """Tamper with image bytes (same size) after manifest → fetch must reject."""
    src_root = tmp_path / "fw"
    _make_image(src_root / "sample_c6" / "2.13.0", "sample_c6", "2.13.0")
    src = LocalDirSource("lab", src_root)
    [img] = await src.list_available()
    # Replace last byte without changing total length.
    image_path = src_root / "sample_c6" / "2.13.0" / "image.signed.bin"
    data = bytearray(image_path.read_bytes())
    data[-1] ^= 0xFF
    image_path.write_bytes(bytes(data))
    with pytest.raises(ManifestError, match="sha256 mismatch"):
        await src.fetch(img, tmp_path / "cache" / "x.bin")


async def test_fetch_rejects_image_without_mcuboot_magic(tmp_path: Path):
    src_root = tmp_path / "fw"
    _make_image(src_root / "sample_c6" / "2.13.0", "sample_c6", "2.13.0",
                magic=b"NOTM")  # not the MCUboot magic
    src = LocalDirSource("lab", src_root)
    [img] = await src.list_available()
    with pytest.raises(ManifestError, match="MCUboot magic"):
        await src.fetch(img, tmp_path / "cache" / "x.bin")


async def test_manifest_parse_accepts_well_formed():
    m = Manifest.parse({
        "device_type": "x", "version": "1.0", "sha256": "0" * 64,
        "size_bytes": 1,
    })
    assert m.device_type == "x"
    assert m.signature_uri is None  # reserved field default


async def test_manifest_parse_rejects_missing_required():
    with pytest.raises(ManifestError, match="missing required"):
        Manifest.parse({"version": "1.0"})


async def test_manifest_parse_rejects_bad_sha():
    with pytest.raises(ManifestError, match="sha256"):
        Manifest.parse({
            "device_type": "x", "version": "1", "sha256": "abc",
            "size_bytes": 1,
        })


async def test_manifest_parse_rejects_negative_size():
    with pytest.raises(ManifestError, match="size_bytes"):
        Manifest.parse({
            "device_type": "x", "version": "1", "sha256": "0" * 64,
            "size_bytes": 0,
        })
