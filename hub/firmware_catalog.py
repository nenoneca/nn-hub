"""
firmware_catalog — orchestrates source discovery, image caching, and
promotion of catalog entries into the OTA-serving firmware_targets table.

Architecture (see docs/factory.md):

    sources.yaml ──► FirmwareCatalog ──► firmware_catalog table
                          │
                          └──► fetch(image) ──► local cache dir
                                                       │
                          promote() ─────────► firmware_targets row (OTA)

Key invariants:
- Discovery never blocks promotion (image stays in catalog even if a sync
  fails later).
- fetch() is idempotent — if a matching sha256 file already exists in the
  cache dir we skip the download.
- The active firmware_targets row always points at a verified local file;
  promotion fetches first if needed.

sources.yaml schema:
    sources:
      - name: lab-local
        kind: local
        root: /srv/firmware
        poll_seconds: 0           # 0 = no periodic poll; sync on boot + on demand
      - name: prod-github
        kind: github
        repo: chalos/nn-fw-sample-c6
        pat_env: NN_HUB_GH_PAT
        poll_seconds: 300
        channels: [stable]
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

from .db import DB
from .firmware_sources import FirmwareSource, ImageDescriptor, build_source

log = logging.getLogger(__name__)


@dataclass
class SourceState:
    source: FirmwareSource
    poll_seconds: int        # 0 disables the periodic loop
    last_sync_at: Optional[int] = None
    last_error: Optional[str] = None


class FirmwareCatalog:
    """In-process service: loads sources.yaml, runs sync + promote ops."""

    def __init__(self, db: DB, cache_dir: Path,
                 sources_yaml_path: Optional[Path] = None):
        self._db = db
        self._cache_dir = Path(cache_dir).expanduser().resolve()
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._sources_yaml_path = (
            Path(sources_yaml_path).expanduser().resolve()
            if sources_yaml_path else None
        )
        self._sources: dict[str, SourceState] = {}
        self._poll_tasks: list[asyncio.Task] = []
        self.reload()

    # ── config ──────────────────────────────────────────────────────────────

    def reload(self) -> None:
        """Re-read sources.yaml from disk and reinstantiate backends.

        Safe to call at any time; cancels any in-flight poll loops and
        restarts them.  Returns silently with no sources if the YAML is
        missing — the operator can populate it later via REST/CLI.
        """
        self._cancel_polls()
        self._sources.clear()

        if not self._sources_yaml_path or not self._sources_yaml_path.is_file():
            log.info("firmware_catalog: no sources.yaml at %s — running with zero sources",
                     self._sources_yaml_path)
            return

        try:
            raw = yaml.safe_load(self._sources_yaml_path.read_text()) or {}
        except yaml.YAMLError as e:
            log.error("firmware_catalog: sources.yaml parse error: %s", e)
            return
        entries = raw.get("sources") or []
        if not isinstance(entries, list):
            log.error("firmware_catalog: sources.yaml `sources` must be a list")
            return

        seen = set()
        for spec in entries:
            try:
                if not isinstance(spec, dict) or "name" not in spec:
                    raise ValueError("each source must be an object with a `name`")
                if spec["name"] in seen:
                    raise ValueError(f"duplicate source name {spec['name']!r}")
                seen.add(spec["name"])
                src = build_source(spec)
                self._sources[src.name] = SourceState(
                    source=src,
                    poll_seconds=int(spec.get("poll_seconds", 0)),
                )
                log.info("firmware_catalog: loaded source %r (%s)",
                         src.name, type(src).__name__)
            except (KeyError, ValueError) as e:
                log.error("firmware_catalog: bad source entry %r: %s", spec, e)

    def source_names(self) -> list[str]:
        return list(self._sources.keys())

    def source_state(self, name: str) -> Optional[SourceState]:
        return self._sources.get(name)

    # ── periodic polling ────────────────────────────────────────────────────

    async def start_polling(self) -> None:
        """Kick off per-source poll loops.  Idempotent."""
        self._cancel_polls()
        for state in self._sources.values():
            if state.poll_seconds > 0:
                self._poll_tasks.append(
                    asyncio.create_task(self._poll_loop(state))
                )

    def _cancel_polls(self) -> None:
        for t in self._poll_tasks:
            t.cancel()
        self._poll_tasks.clear()

    async def stop(self) -> None:
        self._cancel_polls()
        if self._poll_tasks:
            await asyncio.gather(*self._poll_tasks, return_exceptions=True)

    async def _poll_loop(self, state: SourceState) -> None:
        # Stagger the first poll a few seconds so multiple sources don't all
        # fire at boot.
        await asyncio.sleep(2.0)
        while True:
            try:
                await self.sync_one(state.source.name)
            except asyncio.CancelledError:
                return
            except Exception as e:  # pragma: no cover — defensive
                log.exception("firmware_catalog: poll loop %s crashed: %s",
                              state.source.name, e)
            try:
                await asyncio.sleep(state.poll_seconds)
            except asyncio.CancelledError:
                return

    # ── discovery ───────────────────────────────────────────────────────────

    async def sync_all(self) -> dict[str, int]:
        """Discover from every source.  Returns {source_name: count} on success
        and {source_name: -1} for sources that errored."""
        out = {}
        for name in self._sources:
            try:
                out[name] = await self.sync_one(name)
            except Exception as e:  # pragma: no cover
                log.exception("sync_one %s failed: %s", name, e)
                out[name] = -1
        return out

    async def sync_one(self, source_name: str) -> int:
        """Discover from one source.  Persists ImageDescriptors into the
        firmware_catalog table.  Returns the number of entries seen."""
        import time
        state = self._sources.get(source_name)
        if not state:
            raise KeyError(f"no source named {source_name!r}")
        try:
            descriptors = await state.source.list_available()
        except Exception as e:
            state.last_error = f"{type(e).__name__}: {e}"
            state.last_sync_at = int(time.time())
            raise
        state.last_error = None
        state.last_sync_at = int(time.time())

        for desc in descriptors:
            self._db.upsert_catalog_entry(
                device_type=desc.device_type,
                version=desc.version,
                source_name=source_name,
                asset_uri=desc.asset_uri,
                manifest_json=desc.manifest.to_json(),
                sha256=desc.sha256,
                size_bytes=desc.size_bytes,
            )
        log.info("firmware_catalog: sync %s found %d images",
                 source_name, len(descriptors))
        return len(descriptors)

    # ── fetch + promote ─────────────────────────────────────────────────────

    def _cache_path(self, device_type: str, version: str, sha256: str) -> Path:
        """Stable per-image cache path so re-promotion is a no-op."""
        # Keep the sha8 in the filename for forensics if the manifest sha
        # ever disagrees with the file.
        return (self._cache_dir / device_type / version /
                f"image.{sha256[:8]}.signed.bin")

    async def ensure_cached(self, device_type: str, version: str,
                              source_name: Optional[str] = None) -> Path:
        """Make sure the binary is present locally.  Returns the local path.

        Picks the preferred catalog row via DB.get_catalog_entry (cached
        rows preferred over uncached).
        """
        entry = self._db.get_catalog_entry(device_type, version, source_name)
        if not entry:
            raise ValueError(
                f"no catalog entry for {device_type}/{version}"
                + (f" (source={source_name})" if source_name else "")
            )
        cache = self._cache_path(device_type, version, entry.sha256)
        if entry.is_cached and entry.local_path == str(cache) and cache.is_file():
            return cache
        state = self._sources.get(entry.source_name)
        if not state:
            raise RuntimeError(
                f"catalog entry references source {entry.source_name!r} which is no longer configured"
            )
        from .firmware_sources.base import Manifest
        manifest = Manifest.parse(entry.manifest_json)
        descriptor = ImageDescriptor(
            source_name=entry.source_name,
            device_type=entry.device_type,
            version=entry.version,
            asset_uri=entry.asset_uri,
            manifest=manifest,
        )
        await state.source.fetch(descriptor, cache)
        self._db.mark_catalog_downloaded(
            device_type, version, entry.source_name, str(cache)
        )
        return cache

    async def promote(self, device_type: str, version: str,
                       source_name: Optional[str] = None) -> dict:
        """Ensure the image is cached locally + verified, then make it the
        active OTA target for `device_type`.

        Returns a summary dict.  Raises on missing-entry / verify-fail.
        """
        path = await self.ensure_cached(device_type, version, source_name)
        # Read it back to get authoritative size; sha256 we already have.
        size = path.stat().st_size
        entry = self._db.get_catalog_entry(device_type, version, source_name)
        assert entry is not None  # ensure_cached would have raised
        target = self._db.set_firmware_target(
            device_type=device_type,
            version=version,
            path=str(path),
            size=size,
            sha256=entry.sha256,
        )
        log.info("firmware_catalog: promoted %s/%s (source=%s) → %s",
                 device_type, version, entry.source_name, path)
        return {
            "device_type": target.device_type,
            "version": target.target_version,
            "path": target.firmware_path,
            "size_bytes": target.size_bytes,
            "sha256": target.sha256,
            "source": entry.source_name,
        }
