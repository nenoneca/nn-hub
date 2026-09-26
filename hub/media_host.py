"""The hub's side of the media host API (nn-video pipelines, one process
for every camera).  Decided 2026-09-21: the media host's store is the
runtime authority and the HUB is its only writer — provisioning creates the
camera row and installs the device key here, the webapp edits pipeline
settings here, unregister/adopt switch the camera's hub registration here.

Where the media host is: NN_MEDIA_HOST_URL, else the base of any registered
camera URL of the form http://host:port/cam/<id>, else http://127.0.0.1:8880.
"""
from __future__ import annotations

import base64
import os
import re
from typing import Any, Optional

from aiohttp import ClientSession, ClientTimeout

DEFAULT_URL = "http://127.0.0.1:8880"
PIPELINES = ("ingest", "stream", "detect", "event", "control", "hub")
_CAM_URL = re.compile(r"^(https?://[^/]+)/cam/[^/]+/?$")


def media_host_url(db=None) -> str:
    env = (os.environ.get("NN_MEDIA_HOST_URL") or "").strip().rstrip("/")
    if env:
        return env
    if db is not None:
        try:
            for row in db.list_cameras() or []:
                m = _CAM_URL.match(str(row["url"]).strip())
                if m:
                    return m.group(1)
        except Exception:
            pass
    return DEFAULT_URL


async def _req(method: str, url: str, body: Any = None, timeout: float = 8.0) -> tuple[int, Any]:
    async with ClientSession(timeout=ClientTimeout(total=timeout)) as cs:
        async with cs.request(method, url, json=body) as r:
            try:
                data = await r.json()
            except Exception:
                data = {"text": (await r.text())[:400]}
            return r.status, data


async def get(db, path: str, timeout: float = 8.0) -> tuple[int, Any]:
    return await _req("GET", media_host_url(db) + path, timeout=timeout)


async def put(db, path: str, body: Any, timeout: float = 8.0) -> tuple[int, Any]:
    return await _req("PUT", media_host_url(db) + path, body, timeout)


async def post(db, path: str, body: Any = None, timeout: float = 8.0) -> tuple[int, Any]:
    return await _req("POST", media_host_url(db) + path, body, timeout)


# ── camera lifecycle ─────────────────────────────────────────────────────

def next_camera_id(taken: set[str]) -> str:
    """The lowest camN not in use anywhere the hub knows about."""
    n = 0
    while f"cam{n}" in taken:
        n += 1
    return f"cam{n}"


async def ensure_camera(db, cam: str, name: str) -> dict:
    """The camera row on the media host (create, or accept 'exists')."""
    st, data = await post(db, "/cameras", {"id": cam, "name": name or cam, "enabled": True})
    if st == 409:
        st, data = await get(db, f"/cameras/{cam}")
    if st >= 300:
        raise RuntimeError(f"media host refused camera {cam}: {data}")
    return data


async def camera_host_key_id(db, cam: str) -> str:
    """Which host key this camera's slot was provisioned with: the imported
    per-slot key (its own id) or the default."""
    st, data = await get(db, f"/cameras/{cam}")
    if st == 200:
        hk = ((data.get("settings") or {}).get("stream") or {}).get("host_key_id")
        if hk:
            return str(hk)
    return "default"


async def provinfo(db) -> dict:
    st, data = await get(db, "/provinfo")
    if st != 200:
        raise RuntimeError(f"media host /provinfo: {data}")
    return data


async def install_device_key(db, cam: str, device_pub: bytes, host_key_id: str) -> None:
    """After provisioning: the camera's own public key, so the next connect
    is a lookup rather than a trial decrypt."""
    st, data = await put(db, f"/cameras/{cam}/key",
                         {"device_pub_b64": base64.b64encode(bytes(device_pub)).decode(),
                          "host_key_id": host_key_id})
    if st >= 300:
        raise RuntimeError(f"media host refused the device key for {cam}: {data}")


async def set_register(db, cam: str, on: bool) -> bool:
    """Whether the media host keeps registering this camera with the hub.
    Best effort: an unreachable media host must not fail unregister/adopt."""
    try:
        st, _ = await put(db, f"/cameras/{cam}/settings/hub", {"register": bool(on)}, timeout=4)
        return st < 300
    except Exception:
        return False


# ── pipelines (webapp) ───────────────────────────────────────────────────

async def camera_pipelines(db, cam: str) -> dict:
    """One document for the camera's Pipelines tab: per-pipeline health for
    this camera, its effective settings per pipeline, the connection."""
    st, health = await get(db, "/health")
    if st != 200:
        raise RuntimeError(f"media host /health: {health}")
    st, row = await get(db, f"/cameras/{cam}")
    if st != 200:
        raise RuntimeError(f"media host has no camera {cam}")
    out = {"camera": cam, "media_host": media_host_url(db), "uptime_s": health.get("uptime_s"),
           "rss_kb": health.get("rss_kb"), "connection": row.get("connection"), "pipelines": {}}
    for name, p in (health.get("pipelines") or {}).items():
        cams = p.get("cameras") or {}
        out["pipelines"][name] = {
            "mode": p.get("mode"), "workers": p.get("workers"), "budget_s": p.get("budget_s"),
            "queue": {k: (p.get("queue") or {}).get(k) for k in ("depth", "dropped", "avg_wait_ms", "max_depth")},
            "state": cams.get(cam),
            "settings": (row.get("settings") or {}).get(name) or {},
        }
    return out


async def get_settings(db, cam: str, pipeline: str) -> tuple[int, Any]:
    return await get(db, f"/cameras/{cam}/settings/{pipeline}")


async def put_settings(db, cam: str, pipeline: str, values: dict) -> tuple[int, Any]:
    return await put(db, f"/cameras/{cam}/settings/{pipeline}", values)


async def reset_pipeline(db, cam: str, pipeline: str) -> tuple[int, Any]:
    return await post(db, f"/cameras/{cam}/reset/{pipeline}")
