"""
Source ABC + image descriptor + manifest validator.

Manifest schema v1 (image.signed.bin sibling JSON):
    {
      "schema":          1,
      "device_type":     "sample_c6",
      "version":         "2.13.0",
      "sha256":          "<64 hex>",
      "size_bytes":      1028188,
      "mcuboot": {
        "image_magic":   "0x96f3b83d",
        "slot_size_bytes": 524288
      },
      "build_meta": {
        "git_sha":       "<short>",
        "built_at":      "<ISO8601 UTC>",
        "tool_chain":    "<freeform>"
      },
      "signature_uri":   null,    # reserved for detached-sig (Option C)
      "signing_key_id":  null     # reserved for detached-sig (Option C)
    }

Validation is permissive on unknown fields (forward-compat) and strict on
the four fields a consumer always needs: device_type, version, sha256,
size_bytes.  MCUboot magic header is verified at fetch time on the binary
itself, not from the manifest.
"""

from __future__ import annotations

import abc
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


# MCUboot v1 image magic at offset 0 (little-endian).  Same value the
# device-side bootloader checks.  Stored as the raw 4 bytes here.
MCUBOOT_IMAGE_MAGIC = b"\x3d\xb8\xf3\x96"


class ManifestError(ValueError):
    """Raised when a manifest is missing, malformed, or fails validation."""


@dataclass(frozen=True)
class Manifest:
    """Parsed image manifest.  Use Manifest.parse() to validate raw JSON."""

    schema: int
    device_type: str
    version: str
    sha256: str
    size_bytes: int
    mcuboot: dict
    build_meta: dict
    signature_uri: Optional[str]
    signing_key_id: Optional[str]
    raw: dict = field(repr=False)

    @classmethod
    def parse(cls, payload: bytes | str | dict) -> "Manifest":
        """Validate a manifest payload (bytes, str, or already-parsed dict)
        and return a Manifest.  Raises ManifestError on any problem."""
        if isinstance(payload, (bytes, str)):
            try:
                raw = json.loads(payload)
            except json.JSONDecodeError as e:
                raise ManifestError(f"manifest is not valid JSON: {e}") from e
        elif isinstance(payload, dict):
            raw = payload
        else:
            raise ManifestError(
                f"manifest must be bytes/str/dict, got {type(payload).__name__}"
            )

        if not isinstance(raw, dict):
            raise ManifestError(f"manifest root must be an object, got {type(raw).__name__}")

        required = ("device_type", "version", "sha256", "size_bytes")
        missing = [k for k in required if k not in raw]
        if missing:
            raise ManifestError(f"manifest missing required field(s): {missing}")

        sha = raw["sha256"]
        if not isinstance(sha, str) or len(sha) != 64 or not all(c in "0123456789abcdef" for c in sha.lower()):
            raise ManifestError(f"manifest.sha256 must be a 64-char hex string, got {sha!r}")

        size = raw["size_bytes"]
        if not isinstance(size, int) or size <= 0:
            raise ManifestError(f"manifest.size_bytes must be a positive int, got {size!r}")

        return cls(
            schema=int(raw.get("schema", 1)),
            device_type=str(raw["device_type"]),
            version=str(raw["version"]),
            sha256=sha.lower(),
            size_bytes=size,
            mcuboot=dict(raw.get("mcuboot") or {}),
            build_meta=dict(raw.get("build_meta") or {}),
            signature_uri=raw.get("signature_uri"),
            signing_key_id=raw.get("signing_key_id"),
            raw=raw,
        )

    def to_json(self) -> str:
        return json.dumps(self.raw, sort_keys=True)


@dataclass(frozen=True)
class ImageDescriptor:
    """One discoverable image.  Returned by FirmwareSource.list_available()."""

    source_name: str
    device_type: str
    version: str
    asset_uri: str
    manifest: Manifest

    @property
    def sha256(self) -> str:
        return self.manifest.sha256

    @property
    def size_bytes(self) -> int:
        return self.manifest.size_bytes


class FirmwareSource(abc.ABC):
    """Abstract base for a firmware image source.

    Implementations: LocalDirSource, GitHubReleasesSource.  Each instance
    is bound to one source entry in sources.yaml.
    """

    def __init__(self, name: str):
        self.name = name

    @abc.abstractmethod
    async def list_available(self) -> list[ImageDescriptor]:
        """Discover all (device_type, version) images this source offers.

        May make network calls.  Should NOT download binaries — fetch()
        does that.  Should be idempotent + safe to retry.
        """

    @abc.abstractmethod
    async def fetch(self, descriptor: ImageDescriptor, dest: Path) -> Path:
        """Download (if necessary) the binary for `descriptor` and place it
        at `dest`.  Verify sha256 + MCUboot header magic.  Return the path
        that was written.

        Implementations should be safe against partial writes (write to a
        .tmp first, fsync, rename).
        """


# ── shared helpers used by both backends ────────────────────────────────────


XZ_MAGIC = b"\xfd7zXZ\x00"

# manifest "format" → (magic bytes, human name).  Anything not listed here is
# an MCU image and must carry the MCUboot header.
_FORMAT_MAGIC = {
    "tar.gz":        (b"\x1f\x8b", "gzip"),          # Linux app bundle (byai_camera)
    "tar.xz":        (XZ_MAGIC,    "xz"),            # trimmed TI container (byai_platform)
    "sdcard.img.xz": (XZ_MAGIC,    "xz"),            # whole-disk card image (byai_sdcard)
    "rootfs.ext4.xz": (XZ_MAGIC,   "xz"),            # A/B system slot image (byai_system)
}


def check_image_magic(head: bytes, image_format: str = "mcuboot") -> None:
    """Raise ManifestError unless *head* starts with the magic for *image_format*."""
    if image_format in _FORMAT_MAGIC:
        magic, name = _FORMAT_MAGIC[image_format]
        if len(head) < len(magic) or head[:len(magic)] != magic:
            raise ManifestError(
                f"{image_format} image is not {name}; got {head[:len(magic)].hex()}")
    elif len(head) < 4 or head[:4] != MCUBOOT_IMAGE_MAGIC:
        raise ManifestError(
            f"image does not start with MCUboot magic; got {head[:4].hex()}"
        )


def verify_image_bytes(data: bytes, expected_sha256: str,
                        expected_size: int,
                        image_format: str = "mcuboot") -> None:
    """Verify a downloaded blob.  Raises ManifestError on any mismatch.

    *image_format* comes from the manifest's optional "format" field
    (default "mcuboot").  MCU images must carry the MCUboot magic;
    "tar.gz" app bundles must be gzip; "sdcard.img.xz" card images must
    be xz."""
    import hashlib
    if len(data) != expected_size:
        raise ManifestError(
            f"image size mismatch: got {len(data)}, manifest says {expected_size}"
        )
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected_sha256.lower():
        raise ManifestError(
            f"image sha256 mismatch: got {actual}, manifest says {expected_sha256}"
        )
    check_image_magic(data, image_format)


def verify_image_file(path: Path, expected_sha256: str, expected_size: int,
                      image_format: str = "mcuboot") -> None:
    """Streaming twin of verify_image_bytes for files too big to hold in RAM
    (a card image is gigabytes).  Same checks, same errors."""
    import hashlib
    size = path.stat().st_size
    if size != expected_size:
        raise ManifestError(
            f"image size mismatch: got {size}, manifest says {expected_size}"
        )
    h = hashlib.sha256()
    head = b""
    with open(path, "rb") as f:
        while True:
            chunk = f.read(4 << 20)
            if not chunk:
                break
            if not head:
                head = chunk[:16]
            h.update(chunk)
    actual = h.hexdigest()
    if actual != expected_sha256.lower():
        raise ManifestError(
            f"image sha256 mismatch: got {actual}, manifest says {expected_sha256}"
        )
    check_image_magic(head, image_format)


def atomic_write(dest: Path, data: bytes) -> None:
    """Write `data` to `dest` atomically (tmp + fsync + rename)."""
    import os
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(dest)
