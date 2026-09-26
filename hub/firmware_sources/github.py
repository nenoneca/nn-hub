"""
GitHubReleasesSource — discovers images from a GitHub repo's Releases.

Convention (one repo per device_type):
    repo:  chalos/nn-fw-<device_type>
    tag:   v<version>   (e.g. v2.13.0)
    assets per release:
      - image.signed.bin
      - manifest.json

Optional channels filter: a sources.yaml `channels: [stable, beta]` only
keeps releases whose tag prerelease suffix matches.  Tags like
`v2.13.0` are stable, `v2.13.0-beta.1` is beta, etc.

Auth: optional PAT via env var (pat_env in sources.yaml).  Anonymous calls
work fine for public repos at the GitHub-default 60 req/hour rate limit.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Optional

import aiohttp

from .base import (
    FirmwareSource,
    ImageDescriptor,
    Manifest,
    ManifestError,
    verify_image_bytes,
    atomic_write,
)

log = logging.getLogger(__name__)

IMAGE_NAME = "image.signed.bin"
MANIFEST_NAME = "manifest.json"

# Tag must be v<semver> where the version part can be M.m.p, M.m, etc.
# Optional `-<channel>[.N]` suffix.  We don't enforce strict semver — just
# pull whatever's after the leading `v`.
TAG_RX = re.compile(r"^v(?P<version>\d+(?:\.\d+){0,3}(?:-[A-Za-z0-9.-]+)?)$")


class GitHubReleasesSource(FirmwareSource):
    def __init__(self, name: str, repo: str,
                 pat_env: Optional[str] = None,
                 channels: Optional[list[str]] = None,
                 api_base: str = "https://api.github.com"):
        super().__init__(name)
        if "/" not in repo:
            raise ValueError(f"github repo must be owner/name, got {repo!r}")
        self.repo = repo
        self.pat_env = pat_env
        # `None` means "no filtering"; explicit `[]` would mean "nothing
        # matches" which is rarely desired so we normalise it to None.
        self.channels = channels or None
        self.api_base = api_base.rstrip("/")

    # ── auth ────────────────────────────────────────────────────────────────

    def _headers(self) -> dict:
        h = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "nn-hub-firmware-catalog",
        }
        if self.pat_env:
            tok = os.environ.get(self.pat_env)
            if tok:
                h["Authorization"] = f"Bearer {tok}"
            else:
                log.warning("[%s] pat_env=%s set but env var is empty",
                            self.name, self.pat_env)
        return h

    # ── tag → version + channel parsing ─────────────────────────────────────

    @staticmethod
    def _parse_tag(tag: str) -> Optional[tuple[str, str]]:
        """Return (version, channel) or None if the tag isn't shaped right.

        channel is `stable` if there's no `-suffix`, else the suffix's
        first component (`v2.13.0-beta.1` → channel `beta`).
        """
        m = TAG_RX.match(tag)
        if not m:
            return None
        version = m.group("version")
        if "-" in version:
            version_core, suffix = version.split("-", 1)
            channel = suffix.split(".", 1)[0]
            return version, channel
        return version, "stable"

    # ── discovery ───────────────────────────────────────────────────────────

    async def list_available(self) -> list[ImageDescriptor]:
        out: list[ImageDescriptor] = []
        url = f"{self.api_base}/repos/{self.repo}/releases?per_page=100"
        async with aiohttp.ClientSession(headers=self._headers()) as session:
            async with session.get(url) as resp:
                if resp.status == 404:
                    log.warning("[%s] repo %s not found (404)", self.name, self.repo)
                    return out
                if resp.status == 401:
                    log.warning("[%s] unauthenticated (401) — PAT missing or invalid",
                                self.name)
                    return out
                if resp.status == 403:
                    # Likely rate-limited.  Log and bail; next sync tick will retry.
                    body = await resp.text()
                    log.warning("[%s] github 403: %s", self.name, body[:200])
                    return out
                resp.raise_for_status()
                releases = await resp.json()

            # Infer device_type from repo name: `chalos/nn-fw-<device_type>`.
            # If the repo isn't named that way, the manifest's own device_type
            # field is authoritative; we just use the repo to set a default.
            repo_name = self.repo.split("/", 1)[1]
            default_device_type = (
                repo_name[len("nn-fw-"):] if repo_name.startswith("nn-fw-") else repo_name
            )

            for rel in releases:
                tag = rel.get("tag_name", "")
                parsed = self._parse_tag(tag)
                if not parsed:
                    log.debug("[%s] skip tag %r (not v<version>)", self.name, tag)
                    continue
                version, channel = parsed
                if self.channels is not None and channel not in self.channels:
                    log.debug("[%s] skip %s (channel %s not in %s)",
                              self.name, tag, channel, self.channels)
                    continue
                if rel.get("draft"):
                    continue

                assets = {a["name"]: a for a in rel.get("assets", [])}
                manifest_asset = assets.get(MANIFEST_NAME)
                image_asset = assets.get(IMAGE_NAME)
                if not manifest_asset or not image_asset:
                    log.debug("[%s] %s missing %s or %s asset",
                              self.name, tag, MANIFEST_NAME, IMAGE_NAME)
                    continue

                # Fetch the manifest JSON (small).  We do this at discovery
                # time so we can populate sha256 + size_bytes in the catalog
                # without downloading the binary.
                manifest_url = manifest_asset["url"]
                try:
                    raw = await self._download_asset(session, manifest_url)
                    manifest = Manifest.parse(raw)
                except (ManifestError, aiohttp.ClientError) as e:
                    log.warning("[%s] %s manifest fetch/parse failed: %s",
                                self.name, tag, e)
                    continue

                device_type = manifest.device_type or default_device_type
                if manifest.version != version:
                    log.warning("[%s] %s tag version %r doesn't match manifest version %r",
                                self.name, tag, version, manifest.version)
                    continue

                out.append(ImageDescriptor(
                    source_name=self.name,
                    device_type=device_type,
                    version=version,
                    asset_uri=image_asset["url"],
                    manifest=manifest,
                ))
        return out

    # ── fetch ───────────────────────────────────────────────────────────────

    async def fetch(self, descriptor: ImageDescriptor, dest: Path) -> Path:
        async with aiohttp.ClientSession(headers=self._headers()) as session:
            data = await self._download_asset(session, descriptor.asset_uri)
        verify_image_bytes(data, descriptor.sha256, descriptor.size_bytes,
                           descriptor.manifest.raw.get("format", "mcuboot"))
        atomic_write(dest, data)
        log.info("[%s] cached %s/%s → %s (%d bytes)",
                 self.name, descriptor.device_type, descriptor.version,
                 dest, descriptor.size_bytes)
        return dest

    # ── http plumbing ───────────────────────────────────────────────────────

    async def _download_asset(self, session: aiohttp.ClientSession,
                                url: str) -> bytes:
        """Download a GitHub release asset.  Accepts asset .url (API form)
        which 302s to a signed download URL when given the right Accept header.
        """
        # `Accept: application/octet-stream` on the asset endpoint returns
        # the binary directly (or 302 to it for large files).  We let
        # aiohttp follow redirects.
        h = {"Accept": "application/octet-stream"}
        async with session.get(url, headers=h, allow_redirects=True) as resp:
            resp.raise_for_status()
            return await resp.read()
