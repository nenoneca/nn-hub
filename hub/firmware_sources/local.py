"""
LocalDirSource — discovers images on a local filesystem.

Convention:
    <root>/
      <device_type>/
        <version>/
          image.signed.bin
          manifest.json
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from .base import (
    FirmwareSource,
    ImageDescriptor,
    Manifest,
    ManifestError,
    verify_image_bytes,
    atomic_write,
    verify_image_file,
)

log = logging.getLogger(__name__)

IMAGE_NAME = "image.signed.bin"
MANIFEST_NAME = "manifest.json"


class LocalDirSource(FirmwareSource):
    def __init__(self, name: str, root: str | Path):
        super().__init__(name)
        self.root = Path(root).expanduser().resolve()

    async def list_available(self) -> list[ImageDescriptor]:
        return await asyncio.to_thread(self._scan)

    def _scan(self) -> list[ImageDescriptor]:
        out: list[ImageDescriptor] = []
        if not self.root.is_dir():
            log.warning("[%s] root %s does not exist or is not a directory",
                        self.name, self.root)
            return out

        for type_dir in sorted(self.root.iterdir()):
            if not type_dir.is_dir():
                continue
            device_type = type_dir.name
            for version_dir in sorted(type_dir.iterdir()):
                if not version_dir.is_dir():
                    continue
                version = version_dir.name
                manifest_path = version_dir / MANIFEST_NAME
                image_path = version_dir / IMAGE_NAME
                if not (manifest_path.is_file() and image_path.is_file()):
                    log.debug("[%s] %s/%s incomplete (missing image or manifest)",
                              self.name, device_type, version)
                    continue
                try:
                    raw = manifest_path.read_bytes()
                    manifest = Manifest.parse(raw)
                except ManifestError as e:
                    log.warning("[%s] %s/%s manifest invalid: %s",
                                self.name, device_type, version, e)
                    continue
                if manifest.device_type != device_type:
                    log.warning("[%s] %s/%s manifest device_type=%r doesn't match dir name",
                                self.name, device_type, version, manifest.device_type)
                    continue
                if manifest.version != version:
                    log.warning("[%s] %s/%s manifest version=%r doesn't match dir name",
                                self.name, device_type, version, manifest.version)
                    continue
                out.append(ImageDescriptor(
                    source_name=self.name,
                    device_type=device_type,
                    version=version,
                    asset_uri=str(image_path),
                    manifest=manifest,
                ))
        return out

    async def fetch(self, descriptor: ImageDescriptor, dest: Path) -> Path:
        return await asyncio.to_thread(self._fetch_sync, descriptor, dest)

    def _fetch_sync(self, descriptor: ImageDescriptor, dest: Path) -> Path:
        src = Path(descriptor.asset_uri)
        if not src.is_file():
            raise ManifestError(f"local image missing: {src}")
        # Streamed, not read_bytes(): a card image is gigabytes.
        verify_image_file(src, descriptor.sha256, descriptor.size_bytes,
                          descriptor.manifest.raw.get("format", "mcuboot"))
        # For LocalDirSource, "fetching" is just copying into the cache.
        # We copy rather than symlink so the cached file is independent of
        # whatever the source mount does next.  tmp + fsync + rename keeps
        # the cache atomic like atomic_write.
        import os
        import shutil
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        with open(src, "rb") as fi, open(tmp, "wb") as fo:
            shutil.copyfileobj(fi, fo, 4 << 20)
            fo.flush()
            os.fsync(fo.fileno())
        tmp.replace(dest)
        log.info("[%s] cached %s/%s → %s",
                 self.name, descriptor.device_type, descriptor.version, dest)
        return dest
