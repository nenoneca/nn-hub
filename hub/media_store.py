"""
Media event storage — pluggable upload-link backends for camera event media.

The media server (beagle video_service) records motion/AI events fully in
memory (MP4 + event log) and must hand them off without touching its own
disk.  Flow:

  1. media server: POST /api/v1/media/uploads
       {"event_id": "...", "files": [{"name","size","content_type"}, ...]}
  2. hub replies with one GENERIC upload descriptor per file:
       {"uploads": [{"name":..., "url":..., "method":"PUT"|"POST",
                     "headers": {...}, "expires_s": N}, ...]}
     The descriptor is deliberately backend-agnostic: a Google Drive /
     Dropbox / MinIO backend returns its presigned URL + whatever auth
     headers that service needs; the media server just replays them.
  3. media server uploads each file to its descriptor and frees memory.

Backends implement prepare_upload(); the local-FS backend additionally
receives the PUT bodies itself (token-addressed), streaming to disk under
NN_MEDIA_ROOT (default /var/lib/nn-hub/media/<YYYY-MM-DD>/<event_id>/).

Select the backend with NN_MEDIA_STORAGE (default "local").
"""

from __future__ import annotations

import logging
import os
import re
import time
import uuid
from pathlib import Path
from typing import Optional

log = logging.getLogger("hub.media_store")

_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]")


class StorageBackend:
    """One upload target.  prepare_upload() returns a generic descriptor the
    client can execute without knowing which service is behind it."""

    name = "abstract"

    def prepare_upload(self, event_id: str, filename: str, size: int,
                       content_type: str, base_url: str) -> dict:
        raise NotImplementedError


class LocalFSBackend(StorageBackend):
    """Store on the hub's own filesystem.  The hub itself is the upload
    endpoint: descriptors point at PUT /api/v1/media/upload/{token}."""

    name = "local"

    def __init__(self, root: str, ttl_s: int = 600):
        self.root = Path(root)
        self.ttl_s = ttl_s
        self._pending: dict[str, dict] = {}   # token -> {path, expires, max}

    def prepare_upload(self, event_id: str, filename: str, size: int,
                       content_type: str, base_url: str) -> dict:
        safe_ev = _SAFE_NAME.sub("_", event_id)[:64] or "event"
        safe_fn = _SAFE_NAME.sub("_", filename)[:128] or "file.bin"
        day = time.strftime("%Y-%m-%d")
        path = self.root / day / safe_ev / safe_fn
        token = uuid.uuid4().hex
        self._pending[token] = {
            "path": path,
            "expires": time.time() + self.ttl_s,
            # allow some slack over the declared size, reject runaways
            "max": max(size * 2, size + (1 << 20)),
            "content_type": content_type,
        }
        self._gc()
        return {
            "name": filename,
            "url": f"{base_url}/api/v1/media/upload/{token}",
            "method": "PUT",
            "headers": {"Content-Type": content_type or "application/octet-stream"},
            "expires_s": self.ttl_s,
            "backend": self.name,
        }

    def claim(self, token: str) -> Optional[dict]:
        ent = self._pending.get(token)
        if not ent or ent["expires"] < time.time():
            self._pending.pop(token, None)
            return None
        return ent

    def finish(self, token: str) -> None:
        self._pending.pop(token, None)

    def _gc(self) -> None:
        now = time.time()
        for t in [t for t, e in self._pending.items() if e["expires"] < now]:
            del self._pending[t]


# Future backends implement prepare_upload() returning their service's
# presigned URL + auth headers, e.g.:
#   class MinioBackend(StorageBackend):   descriptor url = presigned PUT URL
#   class GDriveBackend(StorageBackend):  method POST + OAuth bearer header
#   class DropboxBackend(StorageBackend): POST + Dropbox-API-Arg headers


class MediaStore:
    """Backend registry + selection (NN_MEDIA_STORAGE, default local)."""

    def __init__(self):
        root = os.environ.get("NN_MEDIA_ROOT", "/var/lib/nn-hub/media")
        self.backends: dict[str, StorageBackend] = {
            "local": LocalFSBackend(root),
        }
        want = os.environ.get("NN_MEDIA_STORAGE", "local")
        self.active = self.backends.get(want) or self.backends["local"]
        log.info("media store backend=%s root=%s", self.active.name, root)

    def prepare_uploads(self, event_id: str, files: list[dict],
                        base_url: str) -> list[dict]:
        out = []
        for f in files:
            out.append(self.active.prepare_upload(
                event_id,
                str(f.get("name", "file.bin")),
                int(f.get("size", 0)),
                str(f.get("content_type", "application/octet-stream")),
                base_url))
        return out
