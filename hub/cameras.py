"""Camera registry for the ``/devices`` webapp.

A *camera* is a live-video source — an nn media camera served by a
``video_service`` instance (the beagle media server, default port 8899).
Each entry carries the base URL of that service so the hub can PROXY its
snapshot / live (jmuxer WebSocket + HLS) / detection-overlay endpoints under
a single origin: the browser only ever talks to the hub, never directly to
the video service (which may not even be reachable from the client network).

Sources, in priority order:

  * ``NN_CAMERAS`` — JSON array ``[{"id","name","url"}, ...]`` (multi-camera).
  * ``NN_VIDEO_SERVICE_URL`` — a single camera (id ``cam0``); this is the same
    env the video-gateway (WebRTC) already uses, so a normal deployment needs
    no extra config.
  * default single camera at ``http://127.0.0.1:8899``.

The camera ``id`` is also the event namespace: an event whose id starts with
``"<id>-"`` belongs to that camera (see ``event_engine`` NN_CAM_ID prefix).
Legacy unprefixed events (``ev-...``) are attributed to the first camera.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass
class Camera:
    id: str
    name: str
    url: str          # video_service base URL, e.g. http://beagle:8899
    caps: dict | None = None      # self-registered capabilities ({"infer": ...})
    online: bool = True           # False = registered but heartbeat is stale
    implicit_default: bool = False  # the built-in cam0 fallback, not configured or registered


# A self-registered camera is dropped from the list once its heartbeat has
# been silent this long (services re-register every 30 s).
STALE_AFTER_S = 180


def _name_from_url(url: str) -> str:
    host = urlparse(url).hostname or ""
    # bare IP / loopback → a friendly default rather than "127" or "192"
    if not host or host in ("localhost",) or host.replace(".", "").isdigit():
        return "Camera"
    return host.split(".")[0].capitalize()


def _default_url() -> str:
    return (os.environ.get("NN_VIDEO_SERVICE_URL") or "http://127.0.0.1:8899").rstrip("/")


def load_cameras(db=None, include_blocked: bool = False) -> list[Camera]:
    """Configured cameras (env) merged with self-registered ones (DB).

    Env entries are authoritative — a static NN_CAMERAS id always wins over
    a registration with the same id, so a deployment can pin a camera's name
    or URL.  Self-registered cameras that stopped heartbeating are marked
    offline rather than vanishing mid-session."""
    out = _env_cameras()
    if db is not None:
        import time as _t
        # The implicit default camera exists so a fresh box shows something
        # before any camera has registered; once the media service registers
        # cameras (self-registration, phase 4 of nn-video pipelines) it must
        # not shadow their rows — cam0 pinned to a dead :8899 was the symptom.
        if any(getattr(c, "implicit_default", False) for c in out):
            try:
                if db.list_cameras():
                    out = [c for c in out if not getattr(c, "implicit_default", False)]
            except Exception:
                pass
        # Operator intent outranks deployment config: a name typed in the
        # add-device wizard is what the operator expects to see, but the
        # display name used to come from NN_CAMERAS (pinned at deploy) or
        # the service's env — so the typed name silently vanished.
        def _override(cam_id, current):
            try:
                v = db.get_setting("camera_name#" + cam_id)
            except Exception:
                v = None
            return v or current
        # Unregistered ids outrank static config: an unregistered camera
        # must not resurrect from NN_CAMERAS on every list (found live
        # 2026-08-20 — the registration guard held while the env merge
        # quietly put the camera straight back).  The block is the
        # cameras_blocked setting — archive history has no fleet effect.
        try:
            blocked = set(json.loads(db.get_setting("cameras_blocked")
                                     or "[]"))
        except Exception:
            blocked = set()
        if blocked and not include_blocked:
            out = [c for c in out if c.id not in blocked]
        by_id = {c.id: c for c in out}
        for row in db.list_cameras():
            # Blocked = awaiting a device (after unregister, or a slot marked
            # /free).  The service behind the slot keeps heart-beating, so the
            # DB row outlives the block — filter it here too, not only the env
            # list, or the slot shows up "live" with nothing behind it.
            if blocked and not include_blocked and row["id"] in blocked:
                continue
            if row["id"] in by_id:
                # Env pins name/url, but it cannot know CAPABILITIES — those
                # only exist because the service registered them.  Without
                # this a statically-configured camera showed no class list
                # and its detection policy could not be edited.
                try:
                    by_id[row["id"]].caps = json.loads(row["caps_json"] or "{}")
                except Exception:
                    pass
                continue
            try:
                caps = json.loads(row["caps_json"] or "{}")
            except Exception:
                caps = {}
            age = _t.time() - (row["last_seen"] or 0)
            out.append(Camera(row["id"],
                              _override(row["id"], row["name"] or row["id"]),
                              str(row["url"]).rstrip("/"), caps,
                              age <= STALE_AFTER_S))
        for c in out:
            c.name = _override(c.id, c.name)
    return out


def _env_cameras() -> list[Camera]:
    raw = os.environ.get("NN_CAMERAS")
    if raw:
        try:
            arr = json.loads(raw)
            out: list[Camera] = []
            for i, c in enumerate(arr):
                url = str(c["url"]).rstrip("/")
                cid = str(c.get("id") or f"cam{i}")
                name = str(c.get("name") or _name_from_url(url))
                out.append(Camera(cid, name, url))
            if out:
                return out
        except Exception:
            pass  # fall through to the single-camera default
    url = _default_url()
    name = os.environ.get("NN_CAMERA_NAME") or _name_from_url(url)
    cam = Camera("cam0", name, url)
    # implicit: neither NN_CAMERAS nor NN_VIDEO_SERVICE_URL was set — this is
    # the single-camera fallback for a box with nothing registered yet
    cam.implicit_default = not os.environ.get("NN_VIDEO_SERVICE_URL")
    return [cam]


def get_camera(cam_id: str, db=None) -> Camera | None:
    for c in load_cameras(db):
        if c.id == cam_id:
            return c
    return None
