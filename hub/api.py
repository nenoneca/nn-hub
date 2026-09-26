"""Phase 5 — REST API wrapping the CLI.

Mounted alongside the existing services by ``nn-hub serve``.  Read-only
endpoints by default; the field-write endpoint is the only mutator.
Auth is optional bearer-token; default bind is localhost so it's safe
to run without one.

Routes (all under ``/api/v1``)::

    GET    /healthz                          {"ok": true, "ts": <unix>}
    GET    /devices                          [{id, name, type, last_seen}, ...]
    GET    /devices/{id_or_name}             {device + provision_info + capabilities}
    GET    /devices/{id_or_name}/field/{f}   {name, value}    (CoAP /field op=get)
    PUT    /devices/{id_or_name}/field/{f}   body: {"value": <num>}  →  set
    GET    /gateways                         [{id, name, role, rloc16, ...}, ...]
    GET    /gateways/{id}                    {gateway + serviced devices}
    GET    /network                          {dataset summary + counts}
    GET    /logs                             query: device, level, tag, since, limit
    GET    /log_files                        list of rotation files

The handlers translate to the same DB / coap_client paths the CLI uses.
"""

from __future__ import annotations

import asyncio
import struct
import base64
import contextlib
import json
import re
import logging
import os
import sys
import secrets
import time
from pathlib import Path
from typing import Optional

from aiohttp import web, ClientSession, ClientTimeout, WSMsgType

from .db import DB, role_name
from . import coap_client as cc
from . import network as net_mod
from . import log_store as log_store_mod
from . import proto
from .crypto import load_or_generate_enc_key

log = logging.getLogger("hub.api")


# ── helpers ────────────────────────────────────────────────────────────────────


def _json_err(code: int, msg: str, extra: Optional[dict] = None) -> web.Response:
    body = {"err": msg}
    if extra:
        body.update(extra)
    return web.json_response(body, status=code)


def _resolve_device(db: DB, name_or_id: str):
    d = db.get_device_by_name(name_or_id)
    if not d:
        d = db.get_device(name_or_id)
    return d


def _device_dict(db: DB, d) -> dict:
    pi = db.get_provision_info(d.id)
    out = {
        "id":            d.id,
        "name":          d.name,
        "type":          d.type,
        "registered_at": d.registered_at,
        "last_seen":     d.last_seen,
    }
    if pi:
        import json as _json
        try:
            caps = _json.loads(pi.capabilities or "[]")
        except Exception:
            caps = []
        out.update({
            "device_type":    pi.device_type,
            "mdns_addr":      pi.mdns_addr,
            "ml_eid":         pi.ml_eid,
            "gateway_id":     pi.gateway_id,
            "eui64":          pi.eui64,
            "capabilities":   caps,
            "provisioned_at": pi.provisioned_at,
            "last_info_at":   pi.last_info_at,
        })
    return out


def _gateway_dict(db: DB, g) -> dict:
    return {
        "id":                   g.id,
        "name":                 g.name,
        "mdns_addr":            g.mdns_addr,
        "registered_at":        g.registered_at,
        "last_seen":            g.last_seen,
        "role":                 g.role,
        "role_name":            role_name(g.role),
        "rloc16":               g.rloc16,
        "mleid_hex":            g.mleid_hex,
        "last_thread_state_at": g.last_thread_state_at,
    }


# ── auth middleware ────────────────────────────────────────────────────────────


@web.middleware
async def _bearer_auth(request: web.Request, handler):
    expected = request.app.get("auth_token")
    if expected:
        got = request.headers.get("Authorization", "")
        if not got.startswith("Bearer ") or got[7:] != expected:
            return _json_err(401, "invalid bearer token")
    return await handler(request)


# ── handlers ───────────────────────────────────────────────────────────────────


async def post_media_uploads(request: web.Request) -> web.Response:
    """Camera event media: return one generic upload descriptor per file.
    Body: {"event_id": str, "files": [{"name","size","content_type"}...]}"""
    store = request.app["media_store"]
    try:
        body = await request.json()
        files = body["files"]
        event_id = str(body.get("event_id") or f"ev-{int(time.time())}")
        assert isinstance(files, list) and files
    except Exception:
        return _json_err(400, "body must be {event_id, files:[{name,size,content_type}]}")
    base = f"{request.scheme}://{request.host}"
    ups = store.prepare_uploads(event_id, files, base)
    return web.json_response({"event_id": event_id, "uploads": ups})


async def put_media_upload(request: web.Request) -> web.Response:
    """Local-FS backend receiver: stream the body to disk (token-addressed)."""
    store = request.app["media_store"]
    backend = store.backends["local"]
    ent = backend.claim(request.match_info["token"])
    if ent is None:
        return _json_err(404, "unknown or expired upload token")
    path = ent["path"]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")
    written = 0
    try:
        with open(tmp, "wb") as f:
            async for chunk in request.content.iter_chunked(256 * 1024):
                written += len(chunk)
                if written > ent["max"]:
                    raise ValueError("size exceeds declared maximum")
                f.write(chunk)
        tmp.rename(path)
    except Exception as e:
        tmp.unlink(missing_ok=True)
        return _json_err(400, f"upload failed: {e}")
    backend.finish(request.match_info["token"])
    log.info("media upload stored: %s (%d B)", path, written)
    return web.json_response({"stored": str(path), "bytes": written})


async def list_media_events(request: web.Request) -> web.Response:
    """Browse stored events (local backend)."""
    import os as _os
    root = request.app["media_store"].backends["local"].root
    out = []
    if root.exists():
        for day in sorted(_os.listdir(root), reverse=True)[:14]:
            dd = root / day
            if not dd.is_dir():
                continue
            for ev in sorted(_os.listdir(dd), reverse=True):
                files = [{"name": f.name, "bytes": f.stat().st_size}
                         for f in sorted((dd / ev).iterdir()) if f.is_file()]
                out.append({"day": day, "event": ev, "files": files})
    return web.json_response(out[:200])


async def delete_media_events(request: web.Request) -> web.Response:
    """Delete whole events: {"events": [{"day": "...", "event": "..."}, ...]}.

    Bulk by design — the UI deletes a multi-selection, and one request keeps
    that atomic from the user's point of view.  Each id is validated the same
    way file serving is (no traversal), and a miss is reported rather than
    silently counted as success."""
    import shutil as _sh
    root = request.app["media_store"].backends["local"].root.resolve()
    try:
        body = await request.json()
    except Exception:
        return _json_err(400, "bad json")
    items = body.get("events")
    if not isinstance(items, list) or not items:
        return _json_err(400, "events[] required")
    deleted, missing = [], []
    for it in items[:500]:
        day = str((it or {}).get("day", ""))
        ev = str((it or {}).get("event", ""))
        if not day or not ev or _MEDIA_SAFE.search(day) or _MEDIA_SAFE.search(ev):
            missing.append(f"{day}/{ev}")
            continue
        path = (root / day / ev).resolve()
        if root not in path.parents or not path.is_dir():
            missing.append(f"{day}/{ev}")
            continue
        try:
            _sh.rmtree(path)
            deleted.append(f"{day}/{ev}")
            # tidy an emptied day folder so the list doesn't grow husks
            dd = root / day
            if dd.is_dir() and not any(dd.iterdir()):
                dd.rmdir()
        except Exception as e:
            missing.append(f"{day}/{ev}: {e}")
    log.info("media: deleted %d event(s)%s", len(deleted),
             f", {len(missing)} failed" if missing else "")
    return web.json_response({"deleted": deleted, "failed": missing})


_MEDIA_SAFE = __import__("re").compile(r"[^A-Za-z0-9._-]")


def _media_resolve(request: web.Request):
    """Map {day}/{event}/{name} to a file under the media root, refusing any
    path that escapes it (traversal-safe)."""
    root = request.app["media_store"].backends["local"].root.resolve()
    parts = [request.match_info["day"], request.match_info["event"],
             request.match_info["name"]]
    if any(_MEDIA_SAFE.search(p) for p in parts):
        return None, root
    path = (root / parts[0] / parts[1] / parts[2]).resolve()
    if root not in path.parents or not path.is_file():
        return None, root
    return path, root


async def get_media_file(request: web.Request) -> web.Response:
    """Serve a stored event file.  web.FileResponse honours Range requests, so
    the browser can seek within the MP4."""
    path, _ = _media_resolve(request)
    if path is None:
        return _json_err(404, "file not found")
    ct = ("video/mp4" if path.suffix == ".mp4"
          else "application/json" if path.suffix in (".json", ".jsonl")
          else "application/octet-stream")
    return web.FileResponse(path, headers={"Content-Type": ct})


_STATIC_DIR = __import__("pathlib").Path(__file__).resolve().parent / "static"
_html_cache: dict[str, str] = {}


def _load_html(name: str) -> str:
    """Read a page from hub/static/, cached after first read."""
    if name not in _html_cache:
        _html_cache[name] = (_STATIC_DIR / name).read_text(encoding="utf-8")
    return _html_cache[name]


async def redirect_to_devices(request: web.Request) -> web.Response:
    """The bare root and the old standalone /media event browser now fold into
    the unified /devices webapp (per-device live view + events browser), so
    there is a single web UI entry point."""
    raise web.HTTPFound("/devices")


# ── /devices webapp + camera proxies ────────────────────────────────────────
#
# The webapp itself is static (hub/hub/static/devices.html + app.js/app.css);
# no HTML/JS lives in this file.  These handlers give the page a single-origin
# API: the hub PROXIES each camera's live/snapshot endpoints from its
# video_service so the browser never has to reach the video service directly.


async def devices_page(request: web.Request) -> web.Response:
    return web.Response(text=_load_html("devices.html"), content_type="text/html")


# Video-flow tracker.  "online" only proves the camera's video_service
# answers HTTP — its pipeline reports state "playing" with no input, and
# snapshot.jpg serves a cached frame, so a camera whose sensor died looks
# perfectly healthy (cam0/cam1 did, for 15 h, 2026-08-16).  The HLS media
# sequence only advances when new video is actually muxed, so we sample it
# in the background and expose the result.
#   cam_id -> {"seq": int, "changed_at": float, "checked_at": float}
_VIDEO_FLOW: dict = {}
VIDEO_STALE_AFTER = 20.0     # seconds without a new segment = not streaming

async def _sample_video_flow(cam_id: str, url: str) -> None:
    import re as _re, time as _time
    try:
        async with ClientSession() as cs:
            async with cs.get(f"{url}/hls/live.m3u8",
                              timeout=ClientTimeout(total=5)) as r:
                if r.status != 200:
                    return
                text = await r.text()
    except Exception:
        return
    m = _re.search(r"#EXT-X-MEDIA-SEQUENCE:(\d+)", text)
    if not m:
        return
    seq = int(m.group(1))
    now = _time.time()
    st = _VIDEO_FLOW.get(cam_id)
    if st is None or seq != st["seq"]:
        _VIDEO_FLOW[cam_id] = {"seq": seq, "changed_at": now, "checked_at": now}
        st = _VIDEO_FLOW[cam_id]
    else:
        st["checked_at"] = now
    st["pdt_skew"] = _playlist_pdt_skew(text)
    if st["pdt_skew"] is not None:
        _note_drift(cam_id, st["pdt_skew"])


def _parse_pdt(ts: str):
    """EXT-X-PROGRAM-DATE-TIME → aware datetime.  ffmpeg writes the zone as
    '+0800' (no colon), which datetime.fromisoformat rejects before 3.11."""
    import datetime as _dt, re as _re
    ts = ts.strip()
    m = _re.match(r"^(.*[+-]\d{2})(\d{2})$", ts)
    if m:
        ts = m.group(1) + ":" + m.group(2)
    return _dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _playlist_pdt_skew(text: str):
    """Seconds the playlist's LIVE EDGE lags our wall clock, or None.

    Why we watch this at all: ffmpeg anchors PROGRAM-DATE-TIME once and then
    advances it by MEDIA time, so every stall pushes the date permanently
    behind — and on the nn-transcoded path media time runs slower than wall
    clock, so it accumulates (~78 s/hour, cam1).  The webapp prints that date
    as the frame's capture time, so a drifting playlist makes a perfectly
    live stream look stale.  Sampling here means the hub notices with nobody
    watching.  Live edge = first segment's date + the whole window's EXTINF.
    """
    import datetime as _dt, re as _re
    pdts = _re.findall(r"#EXT-X-PROGRAM-DATE-TIME:(\S+)", text)
    if not pdts:
        return None
    try:
        stamp = _parse_pdt(pdts[0])
        span = sum(float(x) for x in _re.findall(r"#EXTINF:([\d.]+)", text))
        return (_dt.datetime.now(stamp.tzinfo) - stamp).total_seconds() - span
    except Exception:
        return None

def _video_flow_state(cam_id: str):
    """→ (streaming: bool|None, stale_seconds: float|None).  None means we
    have not sampled this camera yet (don't claim either way)."""
    import time as _time
    st = _VIDEO_FLOW.get(cam_id)
    if not st:
        return None, None
    age = _time.time() - st["changed_at"]
    return (age <= VIDEO_STALE_AFTER), age

# ── stream time-drift reports ─────────────────────────────────────────────
# Two independent observers, because they fail differently:
#   hub    — samples every playlist a few times a minute, so it notices with
#            nobody watching; but it only sees what the PLAYLIST claims.
#   client — reports the capture time the viewer is ACTUALLY looking at,
#            which is the thing that is wrong from the user's point of view,
#            and covers the jmuxer path, which has no playlist at all.
_STREAM_REPORTS: list = []
_STREAM_REPORTS_MAX = 100
_STREAM_REPORT_RATE: dict = {}      # ip -> (window_start, count)
DRIFT_KEY = "stream_drift_max_s"
DRIFT_DEFAULT = 20.0
_drift_max_s = DRIFT_DEFAULT
_DRIFT_LAST: dict = {}              # cam -> time of last stored hub report
DRIFT_REPORT_GAP = 600.0            # at most one hub report per cam / 10 min


def _add_stream_report(rec: dict) -> None:
    _STREAM_REPORTS.append(rec)
    del _STREAM_REPORTS[:-_STREAM_REPORTS_MAX]


def _note_drift(cam_id: str, skew: float) -> None:
    """Record a hub-observed playlist drift, throttled per camera."""
    import time as _t
    if abs(skew) <= _drift_max_s:
        return
    now = _t.time()
    if now - _DRIFT_LAST.get(cam_id, 0.0) < DRIFT_REPORT_GAP:
        return
    _DRIFT_LAST[cam_id] = now
    log.warning("camera %s: HLS playlist date %.1fs from wall clock "
                "(limit %.0fs)", cam_id, skew, _drift_max_s)
    _add_stream_report({"at": now, "cam": cam_id, "source": "hub",
                        "drift_s": round(skew, 1), "limit_s": _drift_max_s})


def _num(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


async def post_stream_report(request: web.Request) -> web.Response:
    """A viewer reports that the capture time on screen is far from now.

    Deliberately NOT gated on the debug flag: this is an error report, not
    telemetry, and it has to work on the setup where nobody thought to turn
    debugging on first.  Browser clocks are unreliable, so the report is
    never taken at face value — we store the hub's own independent
    measurement of the same camera beside it, and that is the number an
    operator should believe when the two disagree."""
    import time as _t
    cam = request.match_info["cam"]
    ip = request.remote or "?"
    now = _t.time()
    win, n = _STREAM_REPORT_RATE.get(ip, (now, 0))
    if now - win > 300:
        win, n = now, 0
    if n >= 6:
        return web.json_response({"ok": False, "dropped": "rate"}, status=429)
    _STREAM_REPORT_RATE[ip] = (win, n + 1)

    raw = await request.content.read(2048)
    try:
        body = json.loads(raw.decode() or "{}")
        if not isinstance(body, dict):
            raise ValueError("not an object")
    except Exception:
        return web.json_response({"ok": False, "error": "bad json"}, status=400)

    st = _VIDEO_FLOW.get(cam) or {}
    hub_drift = st.get("pdt_skew")
    rec = {"at": now, "cam": cam, "source": "client",
           "drift_s": round(_num(body.get("drift_s")), 1),
           "player": str(body.get("player") or "")[:16],
           "clock_skew_s": round(_num(body.get("clock_skew_s")), 1),
           "ua": str(body.get("ua") or "")[:120],
           "hub_drift_s": (round(hub_drift, 1) if hub_drift is not None
                           else None),
           "limit_s": _drift_max_s}
    _add_stream_report(rec)
    log.warning("camera %s: viewer sees capture time %.1fs from now "
                "(hub sees %s, player=%s, client clock off %.1fs)",
                cam, rec["drift_s"], rec["hub_drift_s"], rec["player"],
                rec["clock_skew_s"])
    return web.json_response({"ok": True, "hub_drift_s": rec["hub_drift_s"],
                              "limit_s": _drift_max_s})


async def get_stream_reports(request: web.Request) -> web.Response:
    """Recent drift reports, newest first.  ?cam=<id> filters."""
    cam = request.query.get("cam")
    items = [r for r in _STREAM_REPORTS if not cam or r.get("cam") == cam]
    return web.json_response({"limit_s": _drift_max_s,
                              "reports": list(reversed(items))})


async def video_flow_poller(db) -> None:
    """Sample every camera's playlist a few times a minute.  One small GET
    per camera on the LAN — cheap enough to run always, and it means the UI
    can distinguish 'server up' from 'video actually arriving'."""
    from . import cameras as cams_mod
    global _drift_max_s
    while True:
        try:
            # Operator-tunable, re-read each cycle so a settings change takes
            # effect without a restart.
            try:
                _drift_max_s = max(2.0, _num(db.get_setting(DRIFT_KEY, ""),
                                             DRIFT_DEFAULT))
            except Exception:
                _drift_max_s = DRIFT_DEFAULT
            for c in cams_mod.load_cameras(db):
                await _sample_video_flow(c.id, c.url)
        except Exception as e:
            log.debug("video flow poll: %s", e)
        await asyncio.sleep(7)

async def _camera_online(url: str) -> bool:
    """Online = the camera's video_service answers /status within a short window
    (i.e. the media server is up and serving).  Unreachable → offline."""
    try:
        async with ClientSession() as cs:
            async with cs.get(f"{url}/status", timeout=ClientTimeout(total=2)) as r:
                return r.status == 200
    except Exception:
        return False


async def _camera_device_age(url: str):
    """Seconds since the CAMERA itself last spoke to its media-host (the 'H'
    heartbeat record, every 5 s), or None if no device has ever connected --
    from the media-host's /api/device/status.  Returns "n/a" when the service
    predates that route, so callers can fall back instead of misreading."""
    try:
        async with ClientSession() as cs:
            async with cs.get(f"{url}/api/device/status",
                              timeout=ClientTimeout(total=2)) as r:
                if r.status != 200:
                    return "n/a"
                d = await r.json()
                return d.get("heartbeat_age_s")
    except Exception:
        return "n/a"


DEVICE_GONE_AFTER_S = 30.0       # camera heartbeats every 5 s


def _camera_status(online: bool, streaming, dev_age, hub_age_s: float) -> str:
    """One word the UI can render honestly.

    `online` has always meant "the media-host SERVICE answers", not "a camera
    is attached" -- so a slot whose service ran for weeks with no camera ever
    connecting showed a green "online" pill while its player reconnect-looped
    on a playlist that never appeared (cam0, 2026-09-04 .. 09-20: unpowered
    the whole time).  Distinguish the four states people actually need:

        offline    the service itself is not answering
        no_camera  service up, but no camera is connected to it
        no_video   a camera is connected, but video is not flowing
        streaming  video is flowing
        checking   hub just started; its flow sampler has no reading yet
    """
    if not online:
        return "offline"
    # Video actually flowing is the strongest evidence a camera is attached,
    # and it must win: the ESP32-P4 cameras never send heartbeat records, so
    # for them the device age is None while they stream perfectly well.  The
    # first cut of this rule read "no heartbeat" as "no camera" and marked a
    # streaming cam1 as no_camera.  Heartbeat absence only means something for
    # firmware that sends one.
    numeric = isinstance(dev_age, (int, float))
    if streaming is True:
        return "streaming"
    if streaming is False:
        # video was flowing and stopped: gone if the device stopped talking too
        return "no_camera" if (numeric and dev_age > DEVICE_GONE_AFTER_S) else "no_video"
    # No flow reading at all.  A device that is talking but not sending video
    # is "no video"; otherwise nothing has ever connected -- unless the hub
    # only just started and has had no chance to sample.
    if numeric and dev_age <= DEVICE_GONE_AFTER_S:
        return "no_video"
    return "checking" if hub_age_s < 60 else "no_camera"


async def list_cameras(request: web.Request) -> web.Response:
    from . import cameras as cams_mod
    cams = cams_mod.load_cameras(request.app.get("db"))
    # probe all cameras concurrently → total wait is one timeout, not the sum
    online = await asyncio.gather(*[_camera_online(c.url) for c in cams])
    dev_ages = await asyncio.gather(*[_camera_device_age(c.url) for c in cams])
    hub_age = time.time() - request.app.get("started_at", 0)
    out = []
    for c, on, dev_age in zip(cams, online, dev_ages):
        streaming, stale = _video_flow_state(c.id)
        status = _camera_status(bool(on), streaming, dev_age, hub_age)
        out.append({
            "id":           c.id,
            "name":         c.name,
            "online":       bool(on),
            "status":       status,
            "device_age_s": (round(dev_age, 1) if isinstance(dev_age, (int, float)) else None),
            "streaming":    streaming,
            "video_stale_s": (round(stale, 1) if stale is not None else None),
            # Which physical board owns this slot; "" for cameras that were
            # configured by hand rather than provisioned through the hub.
            "addr":         _slot_prev_addr(request.app["db"], c.id),
            "snapshot_url": f"/api/v1/cameras/{c.id}/snapshot.jpg",
            "live_ws":      f"/api/v1/cameras/{c.id}/ws",
            "hls_url":      f"/api/v1/cameras/{c.id}/hls/live.m3u8",
            "detections":   f"/api/v1/cameras/{c.id}/detections",
            "events_url":   f"/api/v1/cameras/{c.id}/events",
        })
    return web.json_response(out)


def _get_camera_or_404(request: web.Request):
    from . import cameras as cams_mod
    c = cams_mod.get_camera(request.match_info["cam"], request.app.get("db"))
    return c


async def camera_snapshot(request: web.Request) -> web.Response:
    """Proxy the camera's latest JPEG snapshot (used as the grid thumbnail)."""
    c = _get_camera_or_404(request)
    if c is None:
        return _json_err(404, "no such camera")
    try:
        async with ClientSession() as cs:
            async with cs.get(f"{c.url}/snapshot.jpg",
                              timeout=ClientTimeout(total=8)) as r:
                body = await r.read()
                return web.Response(
                    body=body, status=r.status,
                    content_type=r.headers.get("Content-Type", "image/jpeg"),
                    headers={"Cache-Control": "no-store"})
    except Exception as e:
        return _json_err(502, f"camera unreachable: {e}")


async def camera_detections(request: web.Request) -> web.Response:
    """Proxy the live detection list (NPU boxes) for the overlay."""
    c = _get_camera_or_404(request)
    if c is None:
        return _json_err(404, "no such camera")
    try:
        async with ClientSession() as cs:
            async with cs.get(f"{c.url}/api/event/live",
                              timeout=ClientTimeout(total=5)) as r:
                return web.json_response(await r.json(), status=r.status)
    except Exception as e:
        return web.json_response({"detections": [], "err": str(e)}, status=200)


async def camera_device_status(request: web.Request) -> web.Response:
    """What the camera says about ITSELF: advertised capabilities, detector
    self-test result, heartbeat.

    Distinct from /detections, which is the MEDIA-HOST's view: that endpoint
    reports the host-side event engine, so for a camera that runs its own edge
    inference it shows an advancing timestamp and an empty detection list
    whether the device's detector is working or dead.  This is the endpoint
    that can tell the difference."""
    c = _get_camera_or_404(request)
    if c is None:
        return _json_err(404, "no such camera")
    try:
        async with ClientSession() as cs:
            async with cs.get(f"{c.url}/api/device/status",
                              timeout=ClientTimeout(total=5)) as r:
                return web.json_response(await r.json(), status=r.status)
    except Exception as e:
        return _json_err(502, f"camera unreachable: {e}")


_HLS_CT = {".m3u8": "application/vnd.apple.mpegurl", ".ts": "video/mp2t",
           ".m4s": "video/iso.segment", ".mp4": "video/mp4"}


async def camera_hls(request: web.Request) -> web.Response:
    """Proxy an HLS manifest or segment from the camera's video_service.

    Segments are referenced relative to the manifest, so a native <video>
    pointed at .../hls/live.m3u8 fetches .../hls/segNNNNN.ts through here too."""
    c = _get_camera_or_404(request)
    if c is None:
        return _json_err(404, "no such camera")
    name = request.match_info["name"]
    if _MEDIA_SAFE.search(name):
        return _json_err(400, "bad segment name")
    ext = os.path.splitext(name)[1].lower()
    ct = _HLS_CT.get(ext, "application/octet-stream")
    try:
        async with ClientSession() as cs:
            async with cs.get(f"{c.url}/hls/{name}",
                              timeout=ClientTimeout(total=12)) as r:
                body = await r.read()
                return web.Response(body=body, status=r.status, content_type=ct,
                                    headers={"Cache-Control": "no-store"})
    except Exception as e:
        return _json_err(502, f"camera unreachable: {e}")


# ── WebSocket tickets ─────────────────────────────────────────────────────
# Browsers do NOT attach saved HTTP credentials to a WebSocket handshake, so
# behind basic auth every stream popped a login box.  A page therefore asks
# for a ticket over ordinary HTTP (where the browser DOES authenticate) and
# spends it on the socket.  Tickets are random, single-use and short-lived,
# so a URL that leaks into a log or a referrer is worthless seconds later.
_WS_TICKETS: dict = {}
_WS_TICKET_TTL = 300.0
_WS_TICKETS_MAX = 256


def _mint_ws_ticket() -> str:
    now = time.time()
    for k, exp in list(_WS_TICKETS.items()):        # prune while we are here
        if exp < now:
            _WS_TICKETS.pop(k, None)
    if len(_WS_TICKETS) >= _WS_TICKETS_MAX:
        return ""
    t = secrets.token_urlsafe(24)
    _WS_TICKETS[t] = now + _WS_TICKET_TTL
    return t


def _check_ws_ticket(request: web.Request) -> bool:
    """Validate (NOT consume) a ws ticket: minted behind auth once, a
    ticket stays good for any number of sockets until its TTL runs out
    or the hub restarts (the store is in-memory by design).  Expired
    entries are dropped on sight."""
    t = request.rel_url.query.get("ticket", "")
    exp = _WS_TICKETS.get(t) if t else None
    if exp is None:
        return False
    if exp < time.time():
        _WS_TICKETS.pop(t, None)
        return False
    return True


async def get_ws_ticket(request: web.Request) -> web.Response:
    t = _mint_ws_ticket()
    if not t:
        return web.json_response({"error": "too many tickets"}, status=429)
    return web.json_response({"ticket": t, "ttl": int(_WS_TICKET_TTL)})


async def camera_ws(request: web.Request) -> web.StreamResponse:
    """Bidirectional WebSocket proxy for the jmuxer live view: bridges the
    browser's WS to the video_service ``/ws`` (raw H.264 frames)."""
    if not _check_ws_ticket(request):
        return web.Response(status=401, text="ws ticket required")
    c = _get_camera_or_404(request)
    if c is None:
        return web.Response(status=404, text="no such camera")
    ws_cli = web.WebSocketResponse(max_msg_size=0)
    await ws_cli.prepare(request)
    up = c.url.replace("https://", "wss://").replace("http://", "ws://") + "/ws"
    try:
        async with ClientSession() as cs:
            async with cs.ws_connect(up, max_msg_size=0,
                                     heartbeat=30) as ws_up:
                async def up2cli():
                    async for m in ws_up:
                        if m.type == WSMsgType.BINARY:
                            await ws_cli.send_bytes(m.data)
                        elif m.type == WSMsgType.TEXT:
                            await ws_cli.send_str(m.data)
                        else:
                            break

                async def cli2up():
                    async for m in ws_cli:
                        if m.type == WSMsgType.BINARY:
                            await ws_up.send_bytes(m.data)
                        elif m.type == WSMsgType.TEXT:
                            await ws_up.send_str(m.data)
                        else:
                            break

                await asyncio.gather(up2cli(), cli2up(), return_exceptions=True)
    except Exception as e:
        log.info("camera ws proxy closed: %s", e)
    finally:
        with contextlib.suppress(Exception):
            await ws_cli.close()
    return ws_cli


def _camera_life_start(db, cam_id: str) -> float:
    """Epoch when the CURRENT device life on this camera slot began.

    Set only by Adopt.  Absent (0) = the slot has never been unregistered,
    so the whole history belongs to the current life — which keeps
    existing deployments' history visible unchanged."""
    try:
        return float(db.get_setting("camera_life_start:" + cam_id) or 0)
    except Exception:
        return 0.0


def _walk_camera_events(store_root, cam_id: str, all_ids, t_min: float,
                        t_max: float) -> list:
    """Event dirs for *cam_id* whose mtime falls in [t_min, t_max)."""
    import os as _os
    def owner_of(ev: str) -> str:
        for cid in all_ids:
            if ev.startswith(cid + "-"):
                return cid
        return all_ids[0] if all_ids else ""
    out = []
    if not store_root.exists():
        return out
    for day in sorted(_os.listdir(store_root), reverse=True):
        dd = store_root / day
        if not dd.is_dir():
            continue
        for ev in sorted(_os.listdir(dd), reverse=True):
            if owner_of(ev) != cam_id:
                continue
            mt = (dd / ev).stat().st_mtime
            if not (t_min <= mt < t_max):
                continue
            files = [{"name": f.name, "bytes": f.stat().st_size}
                     for f in sorted((dd / ev).iterdir()) if f.is_file()]
            out.append({"day": day, "event": ev, "at": int(mt),
                        "files": files})
    return out


async def camera_events(request: web.Request) -> web.Response:
    """List stored events belonging to *cam*, newest first.

    Attribution: an event id starting with ``"<cam>-"`` belongs to that camera;
    legacy unprefixed events (``ev-...``) are attributed to the FIRST camera."""
    import os as _os
    from . import cameras as cams_mod
    cams = cams_mod.load_cameras(request.app.get("db"))
    ids = [c.id for c in cams]
    cam_id = request.match_info["cam"]
    if cam_id not in ids:
        return _json_err(404, "no such camera")
    default_id = ids[0]

    def owner_of(ev: str) -> str:
        for cid in ids:
            if ev.startswith(cid + "-"):
                return cid
        return default_id            # unprefixed → first camera

    root = request.app["media_store"].backends["local"].root
    # Per-life event ownership (operator decision 2026-08-20): a camera
    # lists only events recorded during its CURRENT device life; a
    # predecessor's footage belongs to that life's archive entry.
    life0 = _camera_life_start(request.app["db"], cam_id)
    out = _walk_camera_events(root, cam_id, ids, life0, float("inf"))
    return web.json_response(out[:300])


async def healthz(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "ts": int(time.time())})


@web.middleware
async def _static_cache_mw(request, handler):
    """Let the browser cache webapp assets for a short window.

    A refresh previously re-downloaded app.js + jmuxer.min.js every time; on
    a lossy link a dropped script fetch left the page without jMuxer, which
    used to fall through to a transport the browser can't play.  ETag still
    revalidates, so a deploy is picked up within max-age."""
    resp = await handler(request)
    try:
        if request.path.startswith("/app/") and resp.status == 200:
            resp.headers.setdefault("Cache-Control", "public, max-age=300")
    except Exception:
        pass
    return resp


_CLIENT_LOG: list = []          # newest-last ring of recent client reports
_CLIENT_LOG_MAX = 200
_CLIENT_LOG_RATE: dict = {}     # ip -> (window_start, count)


async def post_client_log(request: web.Request) -> web.Response:
    """Browser debug reports (opt-in ?debug=1 in the webapp).

    Deliberately cheap and bounded: 8 KB body cap, 6 POSTs/min per client
    IP, a 200-entry in-memory ring (no DB, no disk).  The webapp already
    batches + de-duplicates, so this only has to refuse abuse."""
    import time as _t
    ip = request.remote or "?"
    now = _t.time()
    win, n = _CLIENT_LOG_RATE.get(ip, (now, 0))
    if now - win > 60:
        win, n = now, 0
    if n >= 6:
        return web.json_response({"ok": False, "dropped": "rate"}, status=429)
    _CLIENT_LOG_RATE[ip] = (win, n + 1)

    db: DB = request.app["db"]
    if not _debug_enabled(db):
        return web.json_response({"ok": False, "debug": 0}, status=409)
    raw = await request.content.read(8192)
    try:
        body = json.loads(raw.decode("utf-8", "replace"))
    except Exception:
        return _json_err(400, "bad json")
    items = body.get("items")
    if not isinstance(items, list):
        return _json_err(400, "items required")
    entry = {"at": int(now), "ip": ip,
             "ua": str(body.get("ua", ""))[:120],
             "page": str(body.get("page", ""))[:60],
             "items": items[:10]}
    _CLIENT_LOG.append(entry)
    del _CLIENT_LOG[:-_CLIENT_LOG_MAX]
    for it in entry["items"]:
        log.warning("client %s %s: %s", ip, it.get("kind"), it.get("d"))
    return web.json_response({"ok": True})


DEBUG_KEY = "client_debug"


def _debug_enabled(db) -> bool:
    try:
        return db.get_setting(DEBUG_KEY, "0") == "1"
    except Exception:
        return False


# ── device / service journals ─────────────────────────────────────────────
# Sources are DISCOVERED from systemd, never hardcoded: each camera service
# advertises which camera it serves via NN_CAM_ID, and a request may only
# name a source from that list — a unit name from the query string is never
# passed to journalctl.
# nn-video is now ONE unit hosting every camera slot (media-host/
# video_supervisor.py); its journal carries every worker line prefixed
# "[camN]".  The per-slot units nn-video2/3/4 no longer exist.
_LOG_UNITS = ("nn-video-pipelines", "nn-video", "nn-hub", "nn-inferd", "nn-transcoded",
              "nn-media-ctrl", "nn-gw")
# The camera host unit: nn-video-pipelines (one process, every camera) since
# phase 3; the retired supervisor unit nn-video stays only as history.
_VIDEO_UNITS = ("nn-video-pipelines", "nn-video")
_LOG_SOURCES: dict = {}
_LOG_SOURCES_AT = 0.0
_LOG_SOURCES_TTL = 60.0


_LOG_PIPELINES = ("ingest", "stream", "detect", "event", "control", "hub", "svc", "handler")


def _log_line_filter(cam: str, pipeline: str = ""):
    """Match the journal lines of one camera (and one pipeline) of the
    single nn-video process: every line it writes for a camera is tagged
    '[cam3/stream] …', '[cam3/svc] …', '[cam3/handler] …' (or '[cam3] …'
    from the supervisor era)."""
    if pipeline:
        return re.compile(r"\[" + re.escape(cam) + "/" + re.escape(pipeline) + r"\]").search
    return re.compile(r"\[" + re.escape(cam) + r"(/[a-z]+)?\]").search


def _discover_log_sources(db=None) -> dict:
    """{source_id: (unit, label, camera_filter)} — services, plus one view
    per camera of the nn-video journal (the media host is one process for
    every camera; its lines carry the camera id)."""
    global _LOG_SOURCES, _LOG_SOURCES_AT
    now = time.time()
    if _LOG_SOURCES and (now - _LOG_SOURCES_AT) < _LOG_SOURCES_TTL:
        return _LOG_SOURCES
    import subprocess
    found = {}
    for unit in _LOG_UNITS:
        try:
            r = subprocess.run(["systemctl", "show", unit,
                                "-p", "Environment", "-p", "LoadState"],
                               capture_output=True, text=True, timeout=5)
        except (OSError, subprocess.SubprocessError):
            continue
        out = r.stdout or ""
        if "LoadState=loaded" not in out:
            continue
        cam = ""
        for tok in out.split():
            if tok.startswith("NN_CAM_ID="):
                cam = tok.split("=", 1)[1]
                break
        if cam:
            found[cam] = (unit, f"{cam} (camera service)", "")
        elif unit in _VIDEO_UNITS:
            found[unit] = (unit, f"{unit} (all cameras)", "")
        else:
            found[unit] = (unit, f"{unit} (service)", "")
    video_unit = next((u for u in _VIDEO_UNITS if u in found), "")
    if video_unit:
        # one id for "the camera host, every camera" whichever unit serves it,
        # so a remembered choice in the webapp keeps working across the move
        found["nn-video"] = (video_unit, f"{video_unit} (all cameras)", "")
        if video_unit != "nn-video":
            found.pop(video_unit, None)
        if db is not None:
            try:
                from . import cameras as _cm
                for c in _cm.load_cameras(db, include_blocked=True):
                    found[f"nn-video:{c.id}"] = (video_unit, f"{c.id} — {c.name} (camera pipelines)", c.id)
            except Exception:
                pass
    if found:
        _LOG_SOURCES, _LOG_SOURCES_AT = found, now
    return _LOG_SOURCES


async def get_log_sources(request: web.Request) -> web.Response:
    src = _discover_log_sources(request.app.get("db"))
    return web.json_response({"sources": [
        {"id": k, "label": v[1], "camera": v[2] or None} for k, v in sorted(src.items())],
        "pipelines": list(_LOG_PIPELINES)})


async def get_log_tail(request: web.Request) -> web.Response:
    """Last N lines of one source's journal; a camera source is a filter
    over the shared nn-video journal, optionally narrowed to one pipeline."""
    import subprocess
    q = request.rel_url.query
    src = _discover_log_sources(request.app.get("db"))
    sid = q.get("source", "")
    if sid not in src:
        return web.json_response({"error": f"unknown source {sid!r}"},
                                 status=404)
    try:
        lines = max(10, min(int(q.get("lines", "200")), 2000))
    except ValueError:
        lines = 200
    unit, cam = src[sid][0], src[sid][2]
    pipeline = q.get("pipeline", "")
    if pipeline and pipeline not in _LOG_PIPELINES:
        return web.json_response({"error": f"unknown pipeline {pipeline!r}"}, status=400)
    match = _log_line_filter(cam, pipeline) if cam else None
    # a camera view searches a deep fixed window: a pipeline that logs a
    # few lines an hour is nowhere near the tail of a three-camera journal
    want = 20000 if match else lines
    try:
        r = subprocess.run(
            ["journalctl", "-u", unit, "-n", str(want), "--no-pager",
             "-o", "short-iso"],
            capture_output=True, text=True, timeout=15)
    except (OSError, subprocess.SubprocessError) as e:
        return web.json_response({"error": f"journalctl: {e}"}, status=500)
    if r.returncode != 0:
        return web.json_response(
            {"error": (r.stderr or "journalctl failed").strip()[:200]},
            status=500)
    text = r.stdout
    if match:
        kept = [ln for ln in text.splitlines() if match(ln)]
        text = "\n".join(kept[-lines:]) + ("\n" if kept else "")
    return web.json_response({"source": sid, "unit": unit, "lines": lines,
                              "camera": cam or None, "pipeline": pipeline or None,
                              "text": text})


# A live journal stream costs a subprocess each, so the number of viewers is
# bounded — a log page left open in a few tabs must not be able to exhaust
# the hub.
_LOG_STREAMS = 0
_LOG_STREAMS_MAX = 8


async def ws_log_tail(request: web.Request) -> web.WebSocketResponse:
    """Stream one source's journal: the tail first, then new lines live."""
    global _LOG_STREAMS
    if not _check_ws_ticket(request):
        return web.Response(status=401, text="ws ticket required")
    ws = web.WebSocketResponse(heartbeat=30.0)
    await ws.prepare(request)

    src = _discover_log_sources(request.app.get("db"))
    sid = request.rel_url.query.get("source", "")
    if sid not in src:
        await ws.send_json({"error": f"unknown source {sid!r}"})
        await ws.close()
        return ws
    cam = src[sid][2]
    pipeline = request.rel_url.query.get("pipeline", "")
    if pipeline and pipeline not in _LOG_PIPELINES:
        await ws.send_json({"error": f"unknown pipeline {pipeline!r}"})
        await ws.close()
        return ws
    match = _log_line_filter(cam, pipeline) if cam else None
    try:
        lines = max(0, min(int(request.rel_url.query.get("lines", "200")), 2000))
    except ValueError:
        lines = 200
    if _LOG_STREAMS >= _LOG_STREAMS_MAX:
        await ws.send_json({"error": "too many log viewers open"})
        await ws.close()
        return ws

    unit = src[sid][0]
    _LOG_STREAMS += 1
    proc = None
    try:
        tail_n = 20000 if match else lines
        proc = await asyncio.create_subprocess_exec(
            "journalctl", "-u", unit, "-n", str(tail_n), "-f", "-o", "short-iso",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        await ws.send_json({"source": sid, "unit": unit, "started": True,
                            "camera": cam or None, "pipeline": pipeline or None})

        async def pump():
            # a camera view reads a wider tail and keeps the last `lines`
            # matching ones; journalctl -f writes that tail in one burst,
            # so a short quiet moment on the pipe marks the switch to live
            backlog: list = []
            live = not match
            while True:
                if live:
                    raw = await proc.stdout.readline()
                    if not raw:
                        return
                    line = raw.decode("utf-8", "replace").rstrip("\n")
                    if match and not match(line):
                        continue
                    await ws.send_str(line)
                    continue
                try:
                    raw = await asyncio.wait_for(proc.stdout.readline(), timeout=0.2)
                except asyncio.TimeoutError:
                    live = True
                    for b in backlog:
                        await ws.send_str(b)
                    backlog = []
                    continue
                if not raw:
                    return
                line = raw.decode("utf-8", "replace").rstrip("\n")
                if match(line):
                    backlog.append(line)
                    if len(backlog) > max(lines, 1):
                        del backlog[0]

        async def watch_client():
            # Reading is the only way to notice the browser going away while
            # the log is silent; anything it sends is ignored.
            async for _msg in ws:
                pass

        done, pending = await asyncio.wait(
            {asyncio.create_task(pump()), asyncio.create_task(watch_client())},
            return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        for t in done:
            exc = t.exception()
            if exc and not isinstance(exc, (ConnectionResetError, asyncio.CancelledError)):
                log.info("log stream %s ended: %s", sid, exc)
    except (OSError, asyncio.CancelledError) as e:
        log.info("log stream %s failed: %s", sid, e)
    finally:
        _LOG_STREAMS -= 1
        if proc is not None and proc.returncode is None:
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=3)
            except (OSError, asyncio.TimeoutError):
                try:
                    proc.kill()
                except OSError:
                    pass
        if not ws.closed:
            await ws.close()
    return ws


# Daemon stats posted by hosts that do NOT share a filesystem with the hub
# (the BeagleY runs its own nn-inferd for the C7x).  Kept in memory: it is
# a live gauge, worthless after a restart, and must never grow unbounded.
_INFERD_REMOTE: dict = {}
_INFERD_STALE_S = 120


async def post_inferd_stats(request: web.Request) -> web.Response:
    """A remote nn-inferd host reporting in.  {host, ...statsdoc}"""
    try:
        doc = await request.json()
    except Exception:
        return web.json_response({"error": "bad json"}, status=400)
    host = str(doc.get("host") or "")[:64]
    if not host:
        return web.json_response({"error": "host required"}, status=400)
    if len(_INFERD_REMOTE) >= 32 and host not in _INFERD_REMOTE:
        return web.json_response({"error": "too many hosts"}, status=429)
    doc["received_at"] = int(time.time())
    _INFERD_REMOTE[host] = doc
    return web.json_response({"ok": True})


async def get_inferd_stats(request: web.Request) -> web.Response:
    """Every nn-inferd the hub knows about, local file + remote reporters.

    Returns a list because inference is per-host: the OrangePi serves the
    two ESP32 cameras from its NPU while the BeagleY serves its own camera
    from the C7x, and a single merged total would hide which one is busy."""
    import json as _json
    import socket as _socket
    hosts = []
    path = os.environ.get("NN_INFERD_STATS", "/run/nn-inferd/stats.json")
    try:
        with open(path) as f:
            doc = _json.load(f)
        doc["host"] = _socket.gethostname()
        doc["local"] = True
        hosts.append(doc)
    except (OSError, ValueError):
        pass
    now = int(time.time())
    for host, doc in sorted(_INFERD_REMOTE.items()):
        d = dict(doc)
        d["stale"] = (now - int(d.get("received_at", 0))) > _INFERD_STALE_S
        hosts.append(d)
    if not hosts:
        return web.json_response({"hosts": [],
                                  "error": "no nn-inferd reporting"},
                                 status=404)
    return web.json_response({"hosts": hosts})


async def get_client_debug(request: web.Request) -> web.Response:
    """Just the flag — every page load fetches this, so keep it tiny.

    Entries live on GET /api/v1/clientlog (Settings page only)."""
    db: DB = request.app["db"]
    return web.json_response({"debug": {"enabled": 1 if _debug_enabled(db) else 0}})


async def get_client_log(request: web.Request) -> web.Response:
    """Recent client reports (Settings viewer).  ?limit=N, default 50."""
    db: DB = request.app["db"]
    try:
        lim = max(1, min(200, int(request.query.get("limit", 50))))
    except Exception:
        lim = 50
    return web.json_response({
        "debug": {"enabled": 1 if _debug_enabled(db) else 0},
        "entries": list(reversed(_CLIENT_LOG))[:lim],
    })


async def set_client_debug(request: web.Request) -> web.Response:
    """Turn client debug reporting on/off hub-wide (persists in the DB)."""
    db: DB = request.app["db"]
    try:
        body = await request.json()
    except Exception:
        body = {}
    val = body.get("enabled", request.query.get("enabled"))
    on = str(val).lower() in ("1", "true", "yes", "on")
    db.set_setting(DEBUG_KEY, "1" if on else "0")
    if not on:
        _CLIENT_LOG.clear()
    log.info("client debug reporting %s", "ENABLED" if on else "disabled")
    return web.json_response({"debug": {"enabled": 1 if on else 0}})


async def get_camera_inference(request: web.Request) -> web.Response:
    """Per-camera inference policy + the capability it must fit inside.

    Returns caps (what the device's model can detect, read-only) alongside
    the stored policy so a UI needs exactly one request to render the page."""
    db: DB = request.app["db"]
    from . import cameras as cams_mod
    from . import infer_policy_schema as sch
    cam_id = request.match_info["cam"]
    cam = cams_mod.get_camera(cam_id, db)
    if not cam:
        return _json_err(404, "no such camera")
    caps = getattr(cam, "caps", None) or {}
    try:
        policy = json.loads(db.get_camera_policy(cam_id) or "{}")
    except Exception:
        policy = {}
    if not policy:
        policy = sch.defaults(caps)
    return web.json_response({"id": cam_id, "caps": caps,
                              "classes": sch.class_names(caps),
                              "policy": policy})


async def put_camera_inference(request: web.Request) -> web.Response:
    """Store a policy (validated + clamped) and bump its version.

    The media service picks the new version up on its next poll and pushes
    it to the device over the encrypted control channel."""
    db: DB = request.app["db"]
    from . import cameras as cams_mod
    from . import infer_policy_schema as sch
    cam_id = request.match_info["cam"]
    cam = cams_mod.get_camera(cam_id, db)
    if not cam:
        return _json_err(404, "no such camera")
    try:
        body = await request.json()
    except Exception:
        return _json_err(400, "bad json")
    try:
        prev = json.loads(db.get_camera_policy(cam_id) or "{}")
    except Exception:
        prev = {}
    try:
        doc = sch.normalise(body.get("policy", body),
                            getattr(cam, "caps", None) or {}, prev)
    except sch.PolicyError as e:
        return _json_err(400, str(e))
    db.set_camera_policy(cam_id, json.dumps(doc))
    log.info("camera %s inference policy -> v%s", cam_id, doc["version"])
    return web.json_response({"id": cam_id, "policy": doc})


async def register_camera(request: web.Request) -> web.Response:
    """Camera self-registration / heartbeat.

    A video_service POSTs {id, name, url, caps} at startup and every ~30 s;
    the hub proxies and lists it without a restart.  Idempotent — the same
    id just refreshes name/url/caps and the liveness stamp."""
    db: DB = request.app["db"]
    try:
        body = await request.json()
    except Exception:
        return _json_err(400, "bad json")
    cid = str(body.get("id") or "").strip()
    url = str(body.get("url") or "").strip()
    if not cid or not url:
        return _json_err(400, "id and url are required")
    # Unregistered camera ids must NOT silently resurrect: the host
    # service re-registers every ~30 s, which would undo an unregister
    # within a heartbeat.  The block is cleared ONLY by the explicit
    # Adopt provisioning act (add-device wizard) — never by deleting
    # archive history.
    if cid in _blocked_cameras(db):
        return _json_err(409, "camera id is unregistered — adopt it as a "
                              "new device from the Add-device page")
    import json as _json
    caps = body.get("caps") or {}
    db.upsert_camera(cid, str(body.get("name") or cid), url.rstrip("/"),
                     _json.dumps(caps))
    return web.json_response({"ok": True, "id": cid})


async def unregister_camera(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    db.delete_camera(request.match_info["cam"])
    return web.json_response({"ok": True})


async def list_devices(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    return web.json_response([_device_dict(db, d) for d in db.list_devices()])


async def get_device(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    doc = _device_dict(db, d)
    # Session state is the difference between sealed ops + group-key
    # rotations reaching the device, and a device that silently stops
    # following cascades: show it where an operator looks first.
    router = request.app.get("proto_router")
    if router is not None and hasattr(router, "session_status"):
        doc["session"] = router.session_status(d.id)
    doc["image"] = _ota_image_key(db, d)
    return web.json_response(doc)


async def get_gateway_service(request: web.Request) -> web.Response:
    """Local gateway SERVICE health — the systemd unit + its NCP link.

    Distinct from /gateways (which lists gateways the hub has heard from
    over the mesh): this answers "is the box next to me actually able to
    reach the mesh right now", which is what an operator needs after
    replugging the NCP's USB."""
    import subprocess as _sp
    unit = os.environ.get("NN_GW_UNIT", "nn-gw")
    def _run(*a):
        try:
            return _sp.run(a, capture_output=True, text=True,
                           timeout=5).stdout.strip()
        except Exception:
            return ""
    active = _run("systemctl", "is-active", unit)
    since = _run("systemctl", "show", unit, "-p", "ActiveEnterTimestamp",
                 "--value")
    # NCP candidates: the by-id links are stable across renumbering, which
    # is exactly what a replug breaks.
    ports = []
    try:
        import glob as _g
        ports = sorted(_g.glob("/dev/serial/by-id/*"))
    except Exception:
        pass
    log = _run("journalctl", "-u", unit, "--no-pager", "-n", "40",
               "--output=cat")
    tail = [l for l in log.splitlines()
            if l.strip() and "revents" not in l
            # A second gateway's HELLO multicasts hit this gateway's UDP
            # listener every 30s and its C code warns each time — normal
            # operation, not worth a wall of warnings here.  Real fix:
            # teach gw_linux to ignore G2D 0x0003 silently.
            and "unexpected type 0x0003" not in l][-12:]
    # link health: HUP spam means the service is holding a dead tty
    hup = sum(1 for l in log.splitlines() if "revents" in l)
    role = ""
    for l in reversed(log.splitlines()):
        if "NET_ROLE=" in l:
            role = l.split("NET_ROLE=")[-1].split()[0]
            break
    return web.json_response({
        "unit": unit, "active": active, "since": since,
        "ncp_ports": ports, "role": role, "link_errors": hup,
        "log": tail,
    })


async def post_gateway_service_restart(request: web.Request) -> web.Response:
    """Restart the local gateway service — the fix after an NCP replug
    (the running process keeps its old tty open and wedges on HUP; a
    restart re-probes /dev/serial/by-id).  Recovery, not configuration:
    no arguments, nothing to get wrong."""
    import subprocess as _sp
    unit = os.environ.get("NN_GW_UNIT", "nn-gw")
    try:
        r = _sp.run(["systemctl", "restart", unit], capture_output=True,
                    text=True, timeout=30)
    except Exception as e:                            # noqa: BLE001
        return _json_err(502, f"restart failed: {e}")
    if r.returncode != 0:
        return _json_err(502, (r.stderr or "restart failed").strip()[:200]
                         + "  (hub may lack permission to manage the unit)")
    return web.json_response({"ok": True, "unit": unit,
                              "note": "re-probing the NCP; the mesh takes "
                                      "up to ~5 min to re-form"})


def _camera_hosted_gateways(db) -> dict:
    """{gateway_id: camera slot} for every dormant identity the hub has
    minted for a camera slot (camera_gw_blob#<cam>).  The blob is the
    authority, not the "<cam>-gw" label: a label is only a string."""
    out = {}
    for key, raw in db.settings_with_prefix("camera_gw_blob#").items():
        try:
            gid = json.loads(raw).get("gw_id")
        except Exception:                                 # noqa: BLE001
            continue
        if gid:
            out[str(gid)] = key.split("#", 1)[1]
    return out


async def list_gateways(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    hosts = _camera_hosted_gateways(db)
    out = []
    for g in db.list_gateways():
        d = _gateway_dict(db, g)
        d["host_camera"] = hosts.get(g.id)
        out.append(d)
    return web.json_response(out)


async def get_radio_health(request: web.Request) -> web.Response:
    """Per-device radio health over the last `minutes` (default 30) from the
    devices' RADIO_STATS reports: CCA-busy share of transmit attempts (the
    channel is occupied), no-ack share (weak link), MAC retry share, hub-seen
    repeat share (our acks lost), reassembly failures/h, parent changes."""
    from . import radio_health
    db: DB = request.app["db"]
    try:
        minutes = max(5, min(24 * 60, int(request.query.get("minutes", "30"))))
    except ValueError:
        return _json_err(400, "minutes must be an integer")
    now = int(time.time())
    rows = db.radio_stats_since(None, now - minutes * 60)
    by_dev: dict = {}
    for r in rows:
        by_dev.setdefault(r["device_id"], []).append(r)
    out = []
    for did, reps in sorted(by_dev.items()):
        d = db.get_device(did)
        sm = radio_health.summarize(reps) or {}
        last = sm.get("latest", {})
        out.append({"device": d.name if d else did, "device_id": did,
                    "channel": last.get("channel"), "role": last.get("role"),
                    "parent": last.get("parent"), "parent_rssi": last.get("parent_rssi"),
                    "parent_lq": [last.get("parent_lq_in"), last.get("parent_lq_out")],
                    "driver_counters": last.get("driver_counters"),
                    "report_age_s": now - last.get("ts", now), "reports": sm.get("reports", 0),
                    "window": sm.get("window")})
    return web.json_response({"window_minutes": minutes, "devices": out})


def _channel_mgr(request: web.Request):
    router = request.app.get("proto_router")
    return getattr(router, "channel_manager", None)


async def get_radio_channel(request: web.Request) -> web.Response:
    """Current Thread channel, pending move, history, last scan, settings."""
    cm = _channel_mgr(request)
    if cm is None:
        return _json_err(503, "channel manager not running")
    return web.json_response(cm.status())


async def post_radio_channel_scan(request: web.Request) -> web.Response:
    """Start a scan + vote job (gateways, then sensors one at a time)."""
    cm = _channel_mgr(request)
    if cm is None:
        return _json_err(503, "channel manager not running")
    try:
        job = cm.start_scan(source="manual")
    except RuntimeError as e:
        return _json_err(409, str(e))
    return web.json_response({"job_id": job["id"]}, status=202)


async def get_radio_channel_scan(request: web.Request) -> web.Response:
    cm = _channel_mgr(request)
    job = cm.get_job(request.match_info["job"]) if cm else None
    if job is None:
        return _json_err(404, "no such scan job")
    return web.json_response(job)


async def post_radio_channel_migrate(request: web.Request) -> web.Response:
    """Move the whole mesh to a channel.  Body: {"channel": 20, "confirm": 20,
    "dry_run": false}.  `confirm` must repeat the channel: a change stops the
    mesh for a moment and every device follows it."""
    cm = _channel_mgr(request)
    if cm is None:
        return _json_err(503, "channel manager not running")
    try:
        body = await request.json()
        ch = int(body.get("channel"))
    except Exception:
        return _json_err(400, "body must be JSON with an integer channel")
    dry = bool(body.get("dry_run"))
    if not dry and str(body.get("confirm")) != str(ch):
        return _json_err(400, "confirm must repeat the channel number")
    try:
        res = await cm.migrate(ch, source="manual", dry_run=dry,
                               reason=str(body.get("reason") or "")[:200])
    except ValueError as e:
        return _json_err(400, str(e))
    except (RuntimeError, asyncio.TimeoutError) as e:
        return _json_err(409, str(e) or "gateway did not answer")
    return web.json_response(res)


async def get_radio_channel_settings(request: web.Request) -> web.Response:
    cm = _channel_mgr(request)
    if cm is None:
        return _json_err(503, "channel manager not running")
    return web.json_response(cm.settings())


async def put_radio_channel_settings(request: web.Request) -> web.Response:
    cm = _channel_mgr(request)
    if cm is None:
        return _json_err(503, "channel manager not running")
    try:
        body = await request.json()
        if not isinstance(body, dict):
            raise ValueError("body must be a JSON object")
        return web.json_response(cm.put_settings(body))
    except (ValueError, TypeError) as e:
        return _json_err(400, str(e))


async def get_radio_channel_auto(request: web.Request) -> web.Response:
    """What the automatic trigger would see right now (no action)."""
    cm = _channel_mgr(request)
    if cm is None:
        return _json_err(503, "channel manager not running")
    return web.json_response(cm.evaluate())


async def get_device_radio(request: web.Request) -> web.Response:
    """One device's RADIO_STATS history (`hours`, default 24) with the
    ratios between consecutive reports."""
    from . import radio_health
    db: DB = request.app["db"]
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    try:
        hours = max(1, min(168, int(request.query.get("hours", "24"))))
    except ValueError:
        return _json_err(400, "hours must be an integer")
    reps = db.radio_stats_since(d.id, int(time.time()) - hours * 3600)
    series = []
    for a, b in zip(reps, reps[1:]):
        series.append({"ts": b["ts"], **radio_health.ratios(radio_health.delta(a, b))})
    return web.json_response({"device": d.name, "reports": reps, "intervals": series})


async def post_debug_h2d_probe(request: web.Request) -> web.Response:
    """Diagnostics: send `count` H2D probe frames of `size` body bytes to
    `device` through a CHOSEN gateway (not the device's current route),
    `gap_ms` apart.  The probe cmd (0x0F70..0x0F7F) is unknown to device
    firmware, so it lands in the device's fallback handler, which logs
    "H2D received: cmd=0x0f7N len=L" -- counting those lines on the
    console measures delivery per (gateway, size) with no other traffic
    involved.  Bounded: count <= 50, size <= 400."""
    router = request.app.get("proto_router")
    db: DB = request.app["db"]
    if router is None:
        return _json_err(503, "proto_router not running")
    try:
        body = await request.json()
        d = _resolve_device(db, str(body["device"]))
        gw = str(body["gateway"])
        count = max(1, min(50, int(body.get("count", 10))))
        size = max(0, min(400, int(body.get("size", 0))))
        gap = max(0.2, float(body.get("gap_ms", 1500)) / 1000.0)
        cmd = 0x0F70 | (int(body.get("cell", 0)) & 0x0F)
    except Exception as e:                                # noqa: BLE001
        return _json_err(400, f"expected {{device, gateway, count, size, gap_ms, cell}}: {e}")
    if not d:
        return _json_err(404, "device not found")
    gw_full = next((g.id for g in db.list_gateways() if g.id.startswith(gw)), None)
    if not gw_full or not router._server.is_gateway_online(gw_full):
        return _json_err(409, f"gateway {gw} not online")
    sent = 0
    for i in range(count):
        inner = struct.pack("<HI", cmd, 0xD0000000 | i) + bytes([i & 0xFF]) * size
        frame = proto.encode(proto.FrameType.H2D, bytes.fromhex(d.id), inner, router._hub_priv)
        if await router._server.send_to_gateway(gw_full, frame):
            sent += 1
        await asyncio.sleep(gap)
    return web.json_response({"ok": True, "device": d.name, "gateway": gw_full,
                              "cmd": f"0x{cmd:04x}", "size": size,
                              "inner_len": 6 + size, "frame_len": len(frame), "sent": sent})


async def get_gateway_host(request: web.Request) -> web.Response:
    """Which camera slot hosts this gateway id.  A camera does not know its
    own slot name (the media-host registers the slot), so its gateway agent
    resolves it from the identity it was handed at provisioning."""
    db: DB = request.app["db"]
    gid = request.match_info["gw"]
    cam = _camera_hosted_gateways(db).get(gid)
    if not cam:
        return _json_err(404, "not a camera-hosted gateway")
    return web.json_response({"gateway_id": gid, "camera": cam})


GATEWAY_LIVE_WINDOW_S = 300


async def delete_gateway(request: web.Request) -> web.Response:
    """Forget a gateway record.  Refuses a live one and a camera's dormant
    identity unless ?force=1: the first would come straight back and lose
    its Thread state, the second would be re-minted on the next provision
    with a DIFFERENT id, orphaning the identity already on the camera."""
    db: DB = request.app["db"]
    gid = request.match_info["gw"]
    force = request.query.get("force") in ("1", "true", "yes")
    g = db.get_gateway(gid)
    if not g:
        return _json_err(404, "gateway not found")
    if not force:
        age = (time.time() - g.last_seen) if g.last_seen else None
        if age is not None and age < GATEWAY_LIVE_WINDOW_S:
            return _json_err(409, f"gateway was seen {int(age)} s ago -- it is "
                                  "live; pass ?force=1 to delete it anyway")
        cam = _camera_hosted_gateways(db).get(gid)
        if cam:
            return _json_err(409, f"this is the dormant gateway identity of "
                                  f"camera {cam}; pass ?force=1 to delete it "
                                  "and re-provision that camera afterwards")
    db.delete_gateway(gid)
    log.info("gateway %s (%s) deleted%s", gid, g.name, " (forced)" if force else "")
    return web.json_response({"ok": True, "deleted": gid, "name": g.name})


async def get_gateway(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    g = db.get_gateway(request.match_info["gw"])
    if not g:
        return _json_err(404, "gateway not found")
    out = _gateway_dict(db, g)
    out["devices_serviced"] = [
        {"id": d.id, "name": d.name, "type": d.type, "last_seen": d.last_seen}
        for d in db.list_devices_for_gateway(g.id)
    ]
    return web.json_response(out)


async def get_network(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    net = net_mod.get_network(db)
    if net is None:
        return web.json_response({
            "configured": False,
            "message": "no network — auto-generated on first gateway provisioning",
        })
    tlvs = net_mod.get_dataset_tlvs(db)
    return web.json_response({
        "configured":         True,
        "name":               net.name,
        "channel":            net.channel,
        "panid":              net.panid,
        "extpanid_hex":       net.extpanid_hex,
        "mesh_local_prefix":  f"{net.mesh_local_prefix_hex}::/64",
        "dataset_tlvs_bytes": len(tlvs),
        "dataset_tlvs_hex":   tlvs.hex(),
        "gateway_count":      len(db.list_gateways()),
        "device_count":       len(db.list_devices()),
    })


def _resolve_field_request(request: web.Request):
    """Common prelude for GET/PUT /field — resolves device + keys + router.
    Returns (router, device, device_pub_bytes, enc_priv) or a Response on error."""
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    if router is None:
        return _json_err(503, "proto_router not available "
                         "(nn-hub serve must be running with nn_proto)")
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    pi = db.get_provision_info(d.id)
    if not pi or not pi.enc_pubkey_b64:
        return _json_err(400, "device has no recorded X25519 pubkey")
    # Routing uses the gateway the device last spoke through, else the one
    # recorded at provisioning, else any online gateway (send_h2d).  Only
    # refuse when none of those exists.
    if (not pi.gateway_id and not router._resolve_gateway_for_device(d.id)
            and not router._fallback_gateway()):
        return _json_err(400, "device has no route: no gateway recorded or online")
    return router, d, base64.b64decode(pi.enc_pubkey_b64), request.app["enc_priv"]


async def get_field(request: web.Request) -> web.Response:
    res = _resolve_field_request(request)
    if isinstance(res, web.Response):
        return res
    router, d, dev_pub, enc_priv = res
    try:
        resp = await cc.get_field(router, d.id,
                                  request.match_info["field"],
                                  dev_pub, enc_priv)
    except asyncio.TimeoutError:
        return _json_err(504, "device did not reply within timeout")
    except Exception as e:
        return _json_err(502, f"relay upstream: {e}")
    if "err" in resp:
        return _json_err(400, resp["err"])
    _remember_field(router, d.id, resp)
    return web.json_response(resp)


def _remember_field(router, device_id: str, resp: dict) -> None:
    """A value the device just answered is as good as one it pushed: keep
    it in the live field cache so the next page load shows it at once
    instead of paying another mesh round trip.  Fields the device never
    pushes on its own (an actuator nobody wrote since boot, a button)
    otherwise cost a live read on EVERY refresh."""
    try:
        v = resp.get("value")
        if router is not None and isinstance(v, (int, float)) and isinstance(resp.get("name"), str):
            router.remember_field(device_id, resp["name"], float(v))
    except Exception:                                     # noqa: BLE001
        pass


async def put_field(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:
        return _json_err(400, "expected JSON body")
    if "value" not in body:
        return _json_err(400, "missing 'value'")
    try:
        value = float(body["value"])
    except (TypeError, ValueError):
        return _json_err(400, "'value' must be numeric")
    res = _resolve_field_request(request)
    if isinstance(res, web.Response):
        return res
    router, d, dev_pub, enc_priv = res
    fld = request.match_info["field"]
    # Record the intent BEFORE the mesh round trip: every viewer's card
    # shows it at once, and only the device's own report (reply, push or
    # a later read) moves the switch.  A timeout keeps it pending -- the
    # device often applied the write and only the reply was lost.
    if router is not None:
        router.set_desired(d.id, fld, value, str(body.get("by") or "operator"))
    try:
        resp = await cc.set_field(router, d.id, fld, value, dev_pub, enc_priv)
    except asyncio.TimeoutError:
        return _json_err(504, "device did not reply within timeout")
    except Exception as e:
        return _json_err(502, f"relay upstream: {e}")
    if "err" in resp:
        if router is not None:
            router.clear_desired(d.id, fld, f"rejected: {resp['err']}")
        return _json_err(400, resp["err"])
    _remember_field(router, d.id, resp)
    return web.json_response(resp)


def _parse_since(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s = s.strip().lower()
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if s and s[-1] in multipliers:
        try:
            return int(time.time()) - int(s[:-1]) * multipliers[s[-1]]
        except ValueError:
            return None
    try:
        return int(s)
    except ValueError:
        return None


async def list_logs(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    store: log_store_mod.LogStore = request.app["log_store"]
    q = request.rel_url.query
    device_id = q.get("device")
    if device_id:
        d = _resolve_device(db, device_id)
        if d:
            device_id = d.id
    try:
        limit = max(1, min(int(q.get("limit", "50")), 1000))
    except ValueError:
        limit = 50
    rows = store.query(
        device_id=device_id,
        level=q.get("level") or None,
        tag=q.get("tag") or None,
        since_ts=_parse_since(q.get("since")),
        limit=limit,
    )
    return web.json_response(rows)


async def post_register_gateway(request: web.Request) -> web.Response:
    """Register / upsert a gateway record.  Body::

        {"id": "<hex16>", "name": "<n>",
         "pubkey_b64": "<P-256 uncompressed, base64>",
         "mdns_addr": "<optional>"}
    """
    db: DB = request.app["db"]
    try:
        body = await request.json()
    except Exception:
        return _json_err(400, "expected JSON body")
    for field in ("id", "name", "pubkey_b64"):
        if field not in body or not body[field]:
            return _json_err(400, f"missing {field}")
    g = db.register_gateway(
        id=body["id"], name=body["name"],
        pubkey_b64=body["pubkey_b64"],
        mdns_addr=body.get("mdns_addr", ""),
    )
    return web.json_response(_gateway_dict(db, g), status=201)


async def post_register_device(request: web.Request) -> web.Response:
    """Register / upsert a device record.  Body::

        {"id": "<hex16>", "name": "<n>", "type": "end_device" | ...,
         "pubkey_b64": "..." (optional)}
    """
    db: DB = request.app["db"]
    try:
        body = await request.json()
    except Exception:
        return _json_err(400, "expected JSON body")
    for field in ("id", "name", "type"):
        if field not in body or not body[field]:
            return _json_err(400, f"missing {field}")
    d = db.register_device(
        id=body["id"], name=body["name"], type=body["type"],
        pubkey_b64=body.get("pubkey_b64", ""),
    )
    return web.json_response(_device_dict(db, d), status=201)


async def post_provision_device(request: web.Request) -> web.Response:
    """Set or update provision_info for *dev*.  Body fields all
    optional; missing keys preserve existing values.  ``capabilities``
    accepts a JSON array (will be re-serialized).
    """
    db: DB = request.app["db"]
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    try:
        body = await request.json()
    except Exception:
        return _json_err(400, "expected JSON body")

    import json as _json
    pi = db.get_provision_info(d.id)

    def pick(k, default=""):
        if k in body and body[k] is not None:
            return body[k]
        return getattr(pi, k, default) if pi else default

    caps_obj = body.get("capabilities")
    if caps_obj is None:
        caps = pi.capabilities if pi else "[]"
    elif isinstance(caps_obj, str):
        caps = caps_obj
    else:
        caps = _json.dumps(caps_obj)

    pi = db.set_provision_info(
        device_id=d.id,
        device_type=pick("device_type", d.type),
        enc_pubkey_b64=pick("enc_pubkey_b64", ""),
        mdns_addr=pick("mdns_addr", ""),
        ml_eid=pick("ml_eid", pick("coap_addr", "")),
        gateway_id=pick("gateway_id", ""),
        ble_addr=pick("ble_addr", ""),
        eui64=pick("eui64", ""),
        capabilities=caps,
    )
    return web.json_response(_device_dict(db, db.get_device(d.id)), status=200)


async def post_network_init(request: web.Request) -> web.Response:
    """Generate the OT network if absent, or replace it with --force=true."""
    db: DB = request.app["db"]
    try:
        body = await request.json()
    except Exception:
        body = {}
    force = bool(body.get("force", False))
    channel = body.get("channel")
    name = body.get("name")
    existing = net_mod.get_network(db)
    if existing and not force:
        return _json_err(409, "network already exists; pass force=true to replace")
    kw = {}
    if channel is not None: kw["channel"] = int(channel)
    if name    is not None: kw["name"]    = str(name)
    net = (net_mod.reset_network(db, **kw) if existing
           else net_mod.get_or_create_network(db, **kw))
    return web.json_response({
        "name":         net.name,
        "channel":      net.channel,
        "panid":        net.panid,
        "extpanid_hex": net.extpanid_hex,
    }, status=201)


async def get_auto_failsafe(request: web.Request) -> web.Response:
    """Hub-side cascade failsafe: rule table, watch queue, recent outcomes."""
    router = request.app.get("proto_router")
    fs = getattr(router, "auto_failsafe", None)
    if fs is None:
        return _json_err(503, "automation failsafe not running")
    return web.json_response(fs.status())


async def put_auto_failsafe(request: web.Request) -> web.Response:
    """Change the failsafe settings live: {"enabled": bool, "delay_s": number}."""
    db: DB = request.app["db"]
    try:
        body = await request.json()
    except Exception:
        return _json_err(400, "expected JSON body")
    if "enabled" in body:
        db.set_setting("auto_failsafe_enabled", "1" if body["enabled"] else "0")
    if "delay_s" in body:
        try:
            d = float(body["delay_s"])
        except (TypeError, ValueError):
            return _json_err(400, "delay_s must be a number")
        if not 0.5 <= d <= 600:
            return _json_err(400, "delay_s must be between 0.5 and 600")
        db.set_setting("auto_failsafe_delay_s", str(d))
    router = request.app.get("proto_router")
    fs = getattr(router, "auto_failsafe", None)
    return web.json_response(fs.status() if fs else {"ok": True})


async def get_auto_yaml(request: web.Request) -> web.Response:
    """The operator's automations.yaml working copy (last text POSTed to
    /auto/compile, valid or not).  Empty string when never saved."""
    db: DB = request.app["db"]
    return web.json_response(
        {"yaml": db.get_setting("automations_yaml") or ""})


async def post_auto_compile(request: web.Request) -> web.Response:
    """Compile an automations.yaml.  Accepts either:
       - YAML body (Content-Type: application/x-yaml or text/yaml)
       - JSON body {"yaml": "<...>"}.

    Returns the per-device compiled summaries.
    """
    db: DB = request.app["db"]
    ct = (request.content_type or "").lower()
    yaml_text: str
    if "yaml" in ct:
        yaml_text = (await request.read()).decode("utf-8", errors="replace")
    else:
        try:
            body = await request.json()
        except Exception:
            return _json_err(400, "expected YAML body or JSON {'yaml': '...'}")
        if "yaml" not in body:
            return _json_err(400, "missing 'yaml'")
        yaml_text = str(body["yaml"])

    # Persist the source before compiling so the webapp's Raw editor
    # round-trips even when the compile fails — the stored text is the
    # operator's working copy, not a known-good artifact.
    db.set_setting("automations_yaml", yaml_text)

    import tempfile
    from .auto_compiler import AutoCompiler
    with tempfile.NamedTemporaryFile("w", suffix=".yaml",
                                     delete=False) as f:
        f.write(yaml_text); tmp = Path(f.name)
    try:
        ac = AutoCompiler(db)
        try:
            payloads = ac.compile(tmp)
        except Exception as e:
            return _json_err(400, f"compile error: {e}")
        # Also produce binary per-device blobs for inspection.
        try:
            bin_blobs = ac.compile_binary(tmp)
        except Exception as e:
            log.warning("compile_binary: %s", e)
            bin_blobs = {}
    finally:
        tmp.unlink(missing_ok=True)

    # A rules change doubles as a cascade group-key rotation (Phase 4):
    # new epoch minted + pushed to every verified session; devices hold
    # the previous epoch so in-flight cascades still open.
    router = request.app.get("proto_router")
    if router is not None and payloads:
        # Only rotate when the compile actually produced device configs.
        # An empty compile (bad YAML shape, no matching devices) is not a
        # rules change, and minting a key for it just burns epochs and
        # pushes churn at the fleet.
        try:
            await router.rotate_group_key()
        except Exception as e:
            log.warning("group key rotation failed: %s", e)

    # Count rules per device by re-parsing the YAML so the summary is
    # something actionable (the binary blob is opaque and the in-memory
    # DeviceRoles struct isn't returned by compile()).
    import yaml as _yaml
    parsed = _yaml.safe_load(yaml_text) or {}
    autos = parsed.get("automations", []) if isinstance(parsed, dict) else []
    per_dev = {n: {"triggers": 0, "conditions": 0, "actions": 0}
               for n in payloads.keys()}
    for a in autos:
        for t in a.get("trigger", []) or []:
            d = t.get("device"); per_dev.setdefault(d, {"triggers":0,"conditions":0,"actions":0})
            per_dev[d]["triggers"] += 1
        for c in a.get("condition", []) or []:
            d = c.get("device"); per_dev.setdefault(d, {"triggers":0,"conditions":0,"actions":0})
            per_dev[d]["conditions"] += 1
        for ax in a.get("action", []) or []:
            d = ax.get("device"); per_dev.setdefault(d, {"triggers":0,"conditions":0,"actions":0})
            per_dev[d]["actions"] += 1

    summary = {}
    for name in payloads.keys():
        roles = per_dev.get(name, {"triggers":0,"conditions":0,"actions":0})
        summary[name] = {
            "triggers":     roles["triggers"],
            "conditions":   roles["conditions"],
            "actions":      roles["actions"],
            "binary_bytes": len(bin_blobs.get(name, b"")),
        }
    return web.json_response({
        "compiled":     summary,
        "device_count": len(summary),
    })


async def get_auto_show(request: web.Request) -> web.Response:
    """Return the compiled config currently stored for *dev*."""
    db: DB = request.app["db"]
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    cfg = db.get_config(d.id)
    if not cfg:
        return _json_err(404, "no compiled config for this device")
    return web.json_response({
        "device_id":  d.id,
        "device_name": d.name,
        "version":    cfg.version,
        "updated_at": cfg.updated_at,
        "payload":    cfg.payload,
    })


async def post_auto_push(request: web.Request) -> web.Response:
    """Push the stored compiled config to *dev* via H2D AUTO_PUSH.

    Looks up the device's ECIES X25519 pubkey + gateway route, ECIES-
    encrypts the raw auto_bin blob, sends as H2D AUTO_PUSH and awaits
    the matching D2H AUTO_ACK (1-byte status).  Returns the status."""
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    if router is None:
        return _json_err(503, "proto_router not available")
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    pi = db.get_provision_info(d.id)
    if not pi or not pi.enc_pubkey_b64:
        return _json_err(400, "device has no recorded X25519 pubkey")
    cfg = db.get_config(d.id)
    if not cfg:
        return _json_err(404, "no compiled config for this device")
    auto_b64 = cfg.payload.get("auto_bin") if isinstance(cfg.payload, dict) else None
    if not auto_b64:
        return _json_err(500, "config payload missing 'auto_bin'")
    try:
        raw = base64.b64decode(auto_b64)
    except Exception as e:
        return _json_err(500, f"bad base64: {e}")
    dev_pub = base64.b64decode(pi.enc_pubkey_b64)
    try:
        status = await router.push_auto_config(
            d.id, raw, dev_pub, request.app["enc_priv"])
    except asyncio.TimeoutError:
        return _json_err(504, "device did not ACK within timeout")
    except Exception as e:
        return _json_err(502, f"push upstream: {e}")
    return web.json_response({
        "device_id":   d.id,
        "device_name": d.name,
        "version":     cfg.version,
        "blob_bytes":  len(raw),
        "status":      status,
    })


async def post_device_ota(request: web.Request) -> web.Response:
    """Trigger OTA on *dev* via H2D OTA_HINT.

    The sensor ACKs immediately and runs check → download → apply on its
    own dedicated workqueue.  Response carries the 1-byte ACK status:
        0 = accepted; non-zero = sensor errno (e.g. EBUSY, EINVAL).

    Caller can later poll /api/v1/devices/{dev} or watch logs to confirm
    the version bump after the sensor reboots into the new image.
    """
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    if router is None:
        return _json_err(503, "proto_router not available")
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    key = _ota_image_key(db, d)
    fw = db.get_firmware_target(key) or (
        db.get_firmware_target(d.type) if key != d.type else None)
    if not fw:
        return _json_err(404,
            f"no firmware target uploaded for image={key!r}")
    try:
        status = await router.trigger_ota(d.id)
    except asyncio.TimeoutError:
        return _json_err(504, "device did not ACK within timeout")
    except Exception as e:
        return _json_err(502, f"trigger upstream: {e}")
    return web.json_response({
        "device_id":   d.id,
        "device_name": d.name,
        "device_type": d.type,
        "target_version": fw.target_version,
        "ack_status":  status,
    })


async def list_log_files(request: web.Request) -> web.Response:
    store: log_store_mod.LogStore = request.app["log_store"]
    out = []
    for p in store.list_files():
        try:
            sz = p.stat().st_size
        except Exception:
            sz = 0
        out.append({"name": p.name, "path": str(p), "size_bytes": sz})
    return web.json_response(out)


# ── firmware ────────────────────────────────────────────────────────────────


async def list_firmware_targets(request: web.Request) -> web.Response:
    """List all registered firmware targets (one per device_type)."""
    db: DB = request.app["db"]
    targets = db.list_firmware_targets()
    return web.json_response([
        {
            "device_type":    t.device_type,
            "target_version": t.target_version,
            "firmware_path":  t.firmware_path,
            "size_bytes":     t.size_bytes,
            "sha256":         t.sha256,
            "updated_at":     t.updated_at,
        }
        for t in targets
    ])


async def post_firmware_upload(request: web.Request) -> web.Response:
    """Upload a firmware binary and register it as the target for a
    device type.  Mirrors `nn-hub firmware upload <type> <ver> <file>`.

    Expects multipart/form-data with three parts:
      device_type : text
      version     : text
      firmware    : binary blob (the .signed.bin file)
    """
    import hashlib
    db: DB = request.app["db"]
    if not request.content_type.startswith("multipart"):
        return _json_err(400, "expected multipart/form-data")
    reader = await request.multipart()
    device_type = None
    version = None
    data: Optional[bytes] = None
    while True:
        part = await reader.next()
        if part is None:
            break
        name = part.name
        if name == "device_type":
            device_type = (await part.text()).strip()
        elif name == "version":
            version = (await part.text()).strip()
        elif name in ("firmware", "file"):
            data = await part.read(decode=False)
        else:
            await part.read()  # consume + discard
    if not device_type or not version or data is None:
        return _json_err(400,
            "missing required parts: device_type, version, firmware")
    if len(data) == 0:
        return _json_err(400, "firmware blob is empty")
    # The hub data dir comes from where serve was launched; the
    # firmware_http server expects files under <data_dir>/firmware/.
    # We mirror what the CLI does: name as <type>_<version>.bin.
    data_dir = Path.home() / ".nn-hub"
    (data_dir / "firmware").mkdir(parents=True, exist_ok=True)
    dest = data_dir / "firmware" / f"{device_type}_{version}.bin"
    dest.write_bytes(data)
    sha = hashlib.sha256(data).hexdigest()
    db.set_firmware_target(device_type, version, str(dest), len(data), sha)
    return web.json_response({
        "device_type":    device_type,
        "target_version": version,
        "size_bytes":     len(data),
        "sha256":         sha,
        "firmware_path":  str(dest),
    }, status=201)


# ── BLE provisioning: gateway ───────────────────────────────────────────────


async def post_gateway_new(request: web.Request) -> web.Response:
    """Provision a fresh gateway via BLE GATT (or `transport: net`)
    and register it in the hub DB.  Mirrors `nn-hub gateway new`.

    Body (JSON):
      ssid         : WiFi SSID                                  required
      psk          : WiFi PSK                                   required
      hub_host     : address gateway uses to reach the hub      required
      transport    : "ble" (default) | "net"                    optional
      addr         : BLE MAC or host[:port], skips scan          optional
      scan_time    : float seconds (default 10.0)               optional
      name         : human label                                optional
      mdns         : gateway mDNS hostname                      optional

    Long-running (typically 5-15 s for the BLE handshake).
    """
    import asyncio
    from . import network as net_mod
    try:
        body = await request.json()
    except Exception:
        return _json_err(400, "expected JSON body")
    for k in ("ssid", "psk", "hub_host"):
        if not body.get(k):
            return _json_err(400, f"missing '{k}'")

    db: DB = request.app["db"]
    enc_priv = request.app["enc_priv"]
    net_mod.get_or_create_network(db)
    ot_dataset_tlvs = net_mod.get_dataset_tlvs(db)

    transport = body.get("transport", "ble")
    common_kwargs = dict(
        ssid=body["ssid"], psk=body["psk"], hub_host=body["hub_host"],
        hub_x25519_priv=enc_priv, ot_dataset_tlvs=ot_dataset_tlvs,
        address=body.get("addr"),
        scan_time=float(body.get("scan_time", 10.0)),
    )
    try:
        if transport == "ble":
            from .ble_gateway_provisioner import (
                provision_gateway, BleGatewayProvisionError as PErr)
            result = await provision_gateway(**common_kwargs)
        elif transport == "net":
            from .net_gateway_provisioner import (
                provision_gateway_net, ProvisionError as PErr)
            result = await provision_gateway_net(**common_kwargs)
        else:
            return _json_err(400, f"unknown transport: {transport!r}")
    except Exception as e:
        log.warning("gateway_new failed: %s", e)
        return _json_err(502, f"provisioning failed: {e}")

    gw = db.register_gateway(
        result.gateway_id,
        body.get("name") or result.gateway_id,
        result.pubkey_b64,
        body.get("mdns") or "")
    return web.json_response({
        "id":         gw.id,
        "name":       gw.name,
        "address":    result.address,
        "transport":  transport,
        "pubkey_b64": gw.pubkey_b64,
    }, status=201)


# ── BLE provisioning: device ────────────────────────────────────────────────


async def post_device_new(request: web.Request) -> web.Response:
    """Provision a fresh sensor via BLE GATT and register it in the
    hub DB.  Mirrors `nn-hub device new` (server-side version of
    main.py::_device_new).

    Body (JSON):
      name         : device name (label)                        required
      device_type  : type string (e.g. 'sample_c6')             required
      gateway      : gateway id (hex16) to associate with        optional
      addr         : BLE MAC, skips scan                        optional
      scan_time    : float seconds (default 10.0)               optional
      dataset_hex  : explicit OT dataset (else use hub's net)   optional
    """
    from . import ble_provisioner as ble
    from . import network as net_mod
    from .crypto import encode_hub_config
    try:
        body = await request.json()
    except Exception:
        return _json_err(400, "expected JSON body")
    for k in ("name", "device_type"):
        if not body.get(k):
            return _json_err(400, f"missing '{k}'")
    name = body["name"]
    device_type = body["device_type"]

    db: DB = request.app["db"]
    enc_priv = request.app["enc_priv"]

    dataset_hex = body.get("dataset_hex")
    if dataset_hex:
        try:
            dataset_bytes = bytes.fromhex(
                dataset_hex.replace(" ", "").replace("\n", ""))
        except ValueError as e:
            return _json_err(400, f"bad dataset_hex: {e}")
    else:
        net_mod.get_or_create_network(db)
        dataset_bytes = net_mod.get_dataset_tlvs(db)

    hub_config = encode_hub_config(name, enc_priv)
    addr = body.get("addr")
    scan_time = float(body.get("scan_time", 10.0))
    try:
        if not addr:
            devices = await ble.scan(scan_time=scan_time)
            if not devices:
                return _json_err(404, "no BLE provisioning peripherals found")
            if len(devices) > 1:
                return _json_err(409,
                    f"{len(devices)} peripherals visible — pass 'addr' to disambiguate",
                    extra={"candidates": [
                        {"addr": d.address, "name": d.name} for d in devices]})
            addr = devices[0].address
        result = await ble.provision(
            addr=addr,
            dataset_bytes=dataset_bytes,
            hub_config_bytes=hub_config,
            hub_x25519_priv=enc_priv,
        )
    except Exception as e:
        log.warning("device_new failed: %s", e)
        return _json_err(502, f"provisioning failed: {e}")

    device_id   = result.device_x25519_pub.hex()[:16]
    enc_pub_b64 = base64.b64encode(result.device_x25519_pub).decode()
    mdns_addr   = f"{name}.local"

    db.register_device(device_id, name, "end_device", enc_pub_b64)
    db.set_provision_info(
        device_id=device_id,
        device_type=device_type,
        enc_pubkey_b64=enc_pub_b64,
        mdns_addr=mdns_addr,
        ml_eid="",
        gateway_id=body.get("gateway") or "",
        ble_addr=result.ble_addr,
    )
    db.log_event(device_id, "device_provisioned",
                 f"type={device_type}  ble={result.ble_addr}  (REST)")
    return web.json_response({
        "id":             device_id,
        "name":           name,
        "device_type":    device_type,
        "ble_addr":       result.ble_addr,
        "enc_pubkey_b64": enc_pub_b64,
        "mdns_addr":      mdns_addr,
        "gateway_id":     body.get("gateway") or "",
    }, status=201)


# ── device lifecycle: unregister → archive → re-provision ─────────────────────

UNREG_KEY = "unregister_op:"


def _automation_usage(db, dev_name: str) -> list[str]:
    """Rules that reference *dev_name*.  Hub-local YAML parse — no mesh."""
    import yaml as _yaml
    try:
        doc = _yaml.safe_load(db.get_setting("automations_yaml") or "") or {}
    except Exception:
        return []
    # Walk each rule generically: any "device:" value anywhere in the
    # rule counts.  The YAML shape has already drifted once (trigger is
    # a LIST, the key is "action" not "actions") — a recursive collect
    # can't be broken by the next drift.
    def _devices_in(node, acc):
        if isinstance(node, dict):
            v = node.get("device")
            if isinstance(v, str):
                acc.add(v)
            for x in node.values():
                _devices_in(x, acc)
        elif isinstance(node, list):
            for x in node:
                _devices_in(x, acc)

    rules = (doc.get("automations") if isinstance(doc, dict) else doc) or []
    used = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        names: set = set()
        _devices_in(rule, names)
        if dev_name in names:
            used.append(str(rule.get("alias") or rule.get("id") or "?"))
    return used


async def get_device_automation_usage(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    rules = _automation_usage(db, d.name)
    return web.json_response({"used": bool(rules), "rules": rules})


def _unreg_set(db, device_id: str, state: str, detail: str = "") -> None:
    import time as _t
    cur = {}
    try:
        cur = json.loads(db.get_setting(UNREG_KEY + device_id) or "{}")
    except Exception:
        pass
    cur.update({"state": state, "detail": detail, "updated_at": int(_t.time())})
    cur.setdefault("started_at", int(_t.time()))
    db.set_setting(UNREG_KEY + device_id, json.dumps(cur))


async def _unregister_run(app, d, force: bool) -> None:
    """The reset sequence.  State persisted per step so a hub restart
    resumes visibly rather than losing the operation."""
    db: DB = app["db"]
    router = app.get("proto_router")
    try:
        if not force:
            _unreg_set(db, d.id, "clearing")
            status = await router.request_clear_user_data(d.id)
            if status != 0:
                _unreg_set(db, d.id, "error",
                           f"device refused clear (status {status})")
                return
            _unreg_set(db, d.id, "cleared")
            # REBOOT: single attempt (not idempotent).  A lost ack is a
            # warning, not fatal — the armed flag clears the device on
            # its next natural reboot either way.
            try:
                await router.request_h2d(
                    d.id, req_cmd=proto.Cmd.REBOOT, body=b"",
                    expected_reply_cmd=proto.Cmd.REBOOT_ACK,
                    timeout=10.0, max_attempts=1)
                _unreg_set(db, d.id, "rebooting")
            except Exception as e:                    # noqa: BLE001
                _unreg_set(db, d.id, "rebooting",
                           f"reboot ack lost ({e}) — device resets on its "
                           f"next boot")
        else:
            _unreg_set(db, d.id, "cleared", "forced — device skipped")

        pi = db.get_provision_info(d.id)
        snapshot = {
            "device": {"id": d.id, "name": d.name, "type": d.type,
                       "device_type": getattr(d, "device_type", ""),
                       "registered_at": getattr(d, "registered_at", None),
                       "last_seen": getattr(d, "last_seen", None)},
            "provision_info": ({
                "device_type": pi.device_type,
                "enc_pubkey_b64": pi.enc_pubkey_b64,
                "gateway_id": pi.gateway_id,
                "mdns_addr": pi.mdns_addr,
                "capabilities": pi.capabilities,
            } if pi else None),
            "card_config": (db.get_setting("card_cfg#" + d.id)
                        or db.get_setting("card_cfg:" + d.name)),
            "field_cache": (router.get_field_cache(d.id)
                            if router else {}),
            "forced": force,
        }
        db.archive_device(d.id, d.name,
                          (pi.device_type if pi else d.type) or "",
                          snapshot)
        db.log_event(d.id, "device_archived",
                     f"forced={force}")
        # The departed device held the cascade group key — evict it.
        try:
            if router:
                await router.rotate_group_key()
        except Exception as e:                        # noqa: BLE001
            log.warning("group key rotation after archive: %s", e)
        _unreg_set(db, d.id, "archived")
    except KeyError:
        _unreg_set(db, d.id, "error",
                   "no verified session — device offline?  Retry when it "
                   "is up, or use force to archive without resetting it")
    except asyncio.TimeoutError:
        _unreg_set(db, d.id, "error",
                   "device did not acknowledge CLEAR_USER_DATA "
                   "(firmware < 0.0.28, or offline).  Retry, or force.")
    except Exception as e:                            # noqa: BLE001
        _unreg_set(db, d.id, "error", str(e)[:200])


async def post_device_unregister(request: web.Request) -> web.Response:
    """Start the unregister sequence: sealed CLEAR_USER_DATA → REBOOT →
    archive.  Body {"force": true} skips the device commands (for a dead
    device) and archives immediately."""
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    if router is None:
        return _json_err(503, "proto_router not running")
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    try:
        body = json.loads((await request.content.read(1024)).decode() or "{}")
    except Exception:
        body = {}
    force = bool(body.get("force"))
    # Server-side re-check closes the check-then-confirm race.
    rules = _automation_usage(db, d.name)
    if rules:
        return _json_err(409, "automation_in_use: " + ", ".join(rules))
    _unreg_set(db, d.id, "starting", "force" if force else "")
    asyncio.get_event_loop().create_task(
        _unregister_run(request.app, d, force))
    return web.json_response({"op": "started", "device": d.id,
                              "force": force}, status=202)


async def get_device_unregister_status(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    dev = request.match_info["dev"]
    d = _resolve_device(db, dev)
    did = d.id if d else None
    if did is None:
        a = db.get_archived(dev)
        if a:
            return web.json_response({"state": "archived"})
        return _json_err(404, "device not found")
    try:
        doc = json.loads(db.get_setting(UNREG_KEY + did) or "{}")
    except Exception:
        doc = {}
    if not doc:
        return _json_err(404, "no unregister operation for this device")
    return web.json_response(doc)


async def get_archive(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    items = db.list_archived()
    # summary only — snapshots can be large
    return web.json_response([{k: v for k, v in a.items() if k != "snapshot"}
                              for a in items])


async def get_archive_one(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    a = db.get_archived(request.match_info["dev"])
    if not a:
        return _json_err(404, "not in archive")
    return web.json_response(a)


async def get_archive_events(request: web.Request) -> web.Response:
    """Event recordings owned by an archived camera life (its window)."""
    db: DB = request.app["db"]
    a = db.get_archived(request.match_info["dev"])
    if not a:
        return _json_err(404, "not in archive")
    snap = a.get("snapshot") or {}
    win = snap.get("events_window")
    cam_id = snap.get("camera_id")
    if not win or not cam_id:
        return web.json_response([])
    root = request.app["media_store"].backends["local"].root
    return web.json_response(
        _walk_camera_events(root, cam_id, [cam_id], win[0], win[1])[:300])


async def delete_archive_one(request: web.Request) -> web.Response:
    """The archived-device DELETE: purge ALL historic hub data — for a
    camera life that includes its recorded event videos (the files in its
    events window)."""
    import shutil as _sh
    db: DB = request.app["db"]
    a = db.get_archived(request.match_info["dev"])
    if not a:
        return _json_err(404, "not in archive")
    removed, failed = 0, []
    snap = a.get("snapshot") or {}
    win = snap.get("events_window")
    cam_id = snap.get("camera_id")
    if win and cam_id:
        root = request.app["media_store"].backends["local"].root
        for ev in _walk_camera_events(root, cam_id, [cam_id],
                                      win[0], win[1]):
            try:
                _sh.rmtree(root / ev["day"] / ev["event"])
                removed += 1
            except Exception as e:                    # noqa: BLE001
                log.warning("purge: rm %s/%s: %s", ev["day"], ev["event"], e)
                failed.append(ev["event"])
    if failed:
        # A purge that silently leaves the videos it promised to delete
        # is worse than an error.  Keep the archive entry so the operator
        # can retry once the underlying cause is fixed.
        return _json_err(500, f"purged 0 records: {len(failed)} video "
                              f"dir(s) could not be removed "
                              f"({failed[0]}...) — archive entry kept")
    db.purge_archived(a["device_id"])
    return web.json_response({"ok": True, "purged": a["device_id"],
                              "videos_removed": removed})


_SLOT_DEVICE_KEYS = ("camera_addr#", "camera_name#", "camera_bundle:")


def _forget_slot_device(db, cam_id: str) -> None:
    """Drop everything that describes the BOARD in a slot (its BLE address,
    the operator's label, the bundle it runs) and keep the slot's own state
    (URL row, stream_policy).  Keys are deleted, not blanked."""
    for k in _SLOT_DEVICE_KEYS:
        db.delete_setting(k + str(cam_id))


def _sweep_stale_slot_devices(db) -> list:
    """One-off at startup: unregistered slots written before unregister
    cleared the device binding still carry it, and the binding of a slot
    with no board is by definition stale.  Returns the slots swept."""
    swept = []
    for cid in sorted(_blocked_cameras(db)):
        if any(db.get_setting(k + cid) for k in _SLOT_DEVICE_KEYS):
            _forget_slot_device(db, cid)
            swept.append(cid)
    return swept


def _blocked_cameras(db) -> set:
    try:
        return set(json.loads(db.get_setting("cameras_blocked") or "[]"))
    except Exception:
        return set()


def _set_blocked_cameras(db, ids: set) -> None:
    db.set_setting("cameras_blocked", json.dumps(sorted(ids)))


async def post_camera_unregister(request: web.Request) -> web.Response:
    """Camera unregister: CTRL_CLEAR via its video_service (the camera
    wipes its KV — identity included — and restarts unauthorized), then
    archive the camera's hub records and forget it.  {"force": true}
    skips the device command for a dead camera."""
    from . import cameras as cams_mod
    db: DB = request.app["db"]
    cam_id = request.match_info["cam"]
    c = next((x for x in cams_mod.load_cameras(db) if x.id == cam_id), None)
    if not c:
        return _json_err(404, "camera not found")
    try:
        body = json.loads((await request.content.read(1024)).decode() or "{}")
    except Exception:
        body = {}
    force = bool(body.get("force"))
    cleared = False
    if not force:
        try:
            async with ClientSession() as cs:
                async with cs.post(f"{c.url}/api/camera/clear",
                                   timeout=ClientTimeout(total=12)) as r:
                    body_r = await r.read()
                    if r.status != 200:
                        return _json_err(502, "camera refused clear: "
                                              f"{body_r.decode()[:150]}  "
                                              "(use force to archive anyway)")
                    reason = ""
                    try:
                        rj = json.loads(body_r)
                        cleared = bool(rj.get("applied"))
                        reason = str(rj.get("reason") or "")
                    except Exception:
                        cleared = False
                    if not cleared:
                        # The video service could not see the device reset.
                        # Say WHY when it tells us (already silent before
                        # the command = no camera app talking, e.g. a Linux
                        # board whose camera stack is not installed yet);
                        # only the silent-drop case is old firmware.
                        return _json_err(409, _MSG_CLEAR_NOT_APPLIED.format(
                            why=reason or "firmware ignored the clear "
                                          "command (predates CTRL_CLEAR)"))
        except Exception as e:
            return _json_err(502, f"camera service unreachable: {e}  "
                                  "(use force to archive anyway)")
    snapshot = {
        "camera": {"id": c.id, "name": c.name, "url": c.url},
        "bundle": db.get_setting("camera_bundle:" + c.id),
        "stream_policy": db.get_setting("stream_policy:" + c.id),
        "cleared": cleared, "forced": force,
    }
    # Archive key is synthetic (id@timestamp): a camera id is a HOST
    # config (NN_CAM_ID), so the same id can be unregistered more than
    # once over the years — each life keeps its own permanent entry.
    import time as _t
    arch_key = f"{c.id}@{int(_t.time())}"
    snapshot["camera_id"] = c.id
    # The window of event recordings this life owns.  Purging this
    # archive entry deletes exactly these files — no more, no less.
    snapshot["events_window"] = [int(_camera_life_start(db, c.id)),
                                 int(_t.time())]
    db.archive_device(arch_key, c.name, "camera", snapshot,
                      reason="unregister-camera")
    # The camera ROW stays.  It is the slot's plumbing — above all the
    # video-service URL a self-registered slot has nowhere else — and the
    # service's heartbeat cannot recreate it while the id is blocked (409).
    # Deleting it made the next provision into this slot fail with
    # "'cam3' is not a camera slot" (2026-09-14).  What must go is the
    # DEVICE's state: which board owned the slot (else _slot_for_addr
    # routes a wiped board straight back here), its bundle version and the
    # operator's label for it.  Slot tuning (stream_policy) is the slot's.
    _forget_slot_device(db, c.id)
    from . import media_host as _mh
    await _mh.set_register(db, c.id, False)      # the media host stops re-registering it
    # The block is its OWN list, not the archive entry: archives are
    # history (purge deletes records and nothing else), while the block
    # is fleet state cleared only by the explicit Adopt provisioning act.
    blocked = _blocked_cameras(db)
    blocked.add(c.id)
    _set_blocked_cameras(db, blocked)
    db.log_event(None, "camera_archived", f"{c.id} forced={force}")
    return web.json_response({"ok": True, "archived": arch_key,
                              "cleared": cleared})


async def post_camera_free(request: web.Request) -> web.Response:
    """Mark a camera slot as AWAITING A DEVICE (free for the wizard).

    A brand-new video service registers its slot on its first heartbeat, so
    the hub lists it as a live camera before any device exists: the UI tries
    to play a stream that is not there, and the wizard refuses to provision
    into a "live" slot.  Until now the only way into the free state was to
    unregister a camera (archive + block) — impossible for a slot that never
    had one.  This is the explicit operator act for that case: the slot is
    hidden from the live list and offered as a provisioning target; the next
    successful provisioning (or Adopt) re-admits it, exactly like a blocked
    slot after unregister.  No archive entry is written — nothing happened
    to any device."""
    db: DB = request.app["db"]
    cam_id = request.match_info["cam"]
    from . import cameras as _cm
    known = {c.id for c in _cm.load_cameras(db, include_blocked=True)}
    if cam_id not in known:
        return _json_err(404, f"{cam_id} is not a configured camera slot (no video "
                              "service has registered it)")
    blocked = _blocked_cameras(db)
    if cam_id in blocked:
        return _json_err(409, f"{cam_id} is already awaiting a device")
    blocked.add(cam_id)
    _set_blocked_cameras(db, blocked)
    _forget_slot_device(db, cam_id)   # awaiting a device: no board owns it
    from . import media_host as _mh
    await _mh.set_register(db, cam_id, False)
    db.log_event(None, "camera_slot_freed", cam_id)
    return web.json_response({"ok": True, "free": cam_id,
                              "note": "slot hidden from the camera list and offered "
                                      "to the Add-device wizard; provisioning a "
                                      "device into it re-admits it"})


async def post_camera_adopt(request: web.Request) -> web.Response:
    """The explicit provisioning act for a camera id slot: adopting means
    "a (new) device may register here again".  Deliberately NOT tied to
    archive records — history is never a control surface.  The device
    behind the id is expected to arrive with fresh keys (its clear minted
    a new keypair; authorize it on the streaming host / re-run BLE
    provisioning) and registers on its service's next heartbeat."""
    db: DB = request.app["db"]
    cam_id = request.match_info["cam"]
    blocked = _blocked_cameras(db)
    if cam_id not in blocked:
        return _json_err(404, "camera id is not blocked")
    blocked.discard(cam_id)
    _set_blocked_cameras(db, blocked)
    # New life starts NOW: the adopted device's event history begins
    # empty; everything older belongs to the archived predecessor.
    import time as _t
    db.set_setting("camera_life_start:" + cam_id, str(int(_t.time())))
    db.log_event(None, "camera_adopted", cam_id)
    from . import media_host as _mh
    await _mh.set_register(db, cam_id, True)
    return web.json_response({"ok": True, "adopted": cam_id,
                              "note": "registration re-opened — the camera "
                                      "appears on its service's next beat"})


async def get_blocked_cameras(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    return web.json_response({"blocked": sorted(_blocked_cameras(db))})


# ── provisioning wizard: scan + jobs ──────────────────────────────────────────
# In-RAM job table: provisioning is interactive; nothing here needs to
# survive a hub restart.
_PROV_JOBS: dict = {}
_PROV_SEQ = [0]

SENSOR_STEPS = ["scan/connect", "read device key", "write hub config",
                "write encrypted dataset", "wait for device SUCCESS",
                "register on hub"]
CAMERA_STEPS = ["scan/connect", "read device key", "write config",
                "write encrypted Wi-Fi", "wait for device SUCCESS",
                "wait for self-registration"]


async def get_last_wifi_ssid(request: web.Request) -> web.Response:
    """SSID-only prefill for the camera wizard (never the password)."""
    db: DB = request.app["db"]
    return web.json_response({"value": db.get_setting("last_wifi_ssid") or ""})


def _hub_lan_addr() -> str:
    """The address a camera on the LAN should dial back on — never
    127.0.0.1 (the device is not on this host)."""
    env = os.environ.get("NN_HUB_LAN_ADDR")
    if env:
        return env
    import socket as _s
    try:
        sk = _s.socket(_s.AF_INET, _s.SOCK_DGRAM)
        sk.connect(("8.8.8.8", 53))       # no packet sent; picks the route
        ip = sk.getsockname()[0]
        sk.close()
        return ip
    except Exception:
        return ""


# A slot that exists but whose video-service URL cannot be resolved is a
# different failure from an unknown slot name, and the old message ("is not a
# camera slot.  Free: cam3") said the opposite of the truth (2026-09-14).
_MSG_SLOT_NO_URL = ("slot '{slot}' exists but has no video-service URL on record, so "
                    "its stream key cannot be resolved — pass target_url (the slot's "
                    "control endpoint, e.g. http://127.0.0.1:89xx) or wait for the "
                    "video service's next heartbeat.")


_MSG_CLEAR_NOT_APPLIED = ("camera did not reset: {why} — check that the camera "
                          "app is running and reachable through its video "
                          "service (flash newer firmware if it predates "
                          "CTRL_CLEAR), or use force to archive without "
                          "resetting")


def _free_camera_slots(db) -> list:
    """Slots not currently occupied by a live camera — i.e. the ones a
    new device can be provisioned into."""
    from . import cameras as _cm
    live = {c.id for c in _cm.load_cameras(db)}
    return sorted(cid for cid in _blocked_cameras(db) if cid not in live)


def _auto_target_slot(db) -> str:
    free = _free_camera_slots(db)
    return free[0] if len(free) == 1 else ""


def _new_camera_target(db) -> str:
    """When no unregistered slot waits: a new camera id, created on the
    media host by the provisioning job."""
    from . import cameras as _cm
    from . import media_host as _mh
    taken = {c.id for c in _cm.load_cameras(db, include_blocked=True)}
    taken |= set(_all_camera_slots(db)) | _blocked_cameras(db)
    return _mh.next_camera_id(taken) if not _free_camera_slots(db) else ""


def _all_camera_slots(db) -> list:
    """Every configured slot, occupied or not — one per video service."""
    try:
        return [str(r["id"]) for r in (db.list_cameras() or [])]
    except Exception:
        return []


def _slot_for_addr(db, addr) -> str:
    """The slot this board last occupied, if any — so a re-provisioned
    camera returns to its own video service instead of a free one."""
    if not addr:
        return ""
    want = str(addr).upper()
    # An unregistered slot has no board: unregister/free clear its binding,
    # so a match there is stale data written before that rule existed
    # (cam4's camera_addr# routed the same BeagleY back into cam4 after it
    # had been re-provisioned as cam3, 2026-09-14).  Never route on it.
    blocked = _blocked_cameras(db)
    for cid in _all_camera_slots(db):
        if cid in blocked:
            continue
        if _slot_prev_addr(db, cid) == want:
            return cid
    return ""


def _slot_prev_addr(db, cam_id) -> str:
    """BLE address of the board last provisioned into this slot, upper-case,
    or "" if the slot has never been provisioned through the hub."""
    if not cam_id:
        return ""
    try:
        return (db.get_setting("camera_addr#" + str(cam_id)) or "").upper()
    except Exception:
        return ""


async def get_camera_pipelines(request: web.Request) -> web.Response:
    """The camera's Pipelines tab: per-pipeline health and effective
    settings from the media host (one document)."""
    from . import media_host as _mh
    db: DB = request.app["db"]
    try:
        return web.json_response(await _mh.camera_pipelines(db, request.match_info["cam"]))
    except Exception as e:                                # noqa: BLE001
        return _json_err(502, f"media host: {e}")


async def get_camera_pipeline_settings(request: web.Request) -> web.Response:
    from . import media_host as _mh
    cam, pipe = request.match_info["cam"], request.match_info["pipeline"]
    if pipe not in _mh.PIPELINES:
        return _json_err(404, "unknown pipeline")
    try:
        st, data = await _mh.get_settings(request.app["db"], cam, pipe)
    except Exception as e:                                # noqa: BLE001
        return _json_err(502, f"media host: {e}")
    return web.json_response(data, status=st)


async def put_camera_pipeline_settings(request: web.Request) -> web.Response:
    """The hub is the media host's only writer: the webapp's settings edits
    land here and are forwarded; the media host applies them on the next
    request for that camera, no restart."""
    from . import media_host as _mh
    cam, pipe = request.match_info["cam"], request.match_info["pipeline"]
    if pipe not in _mh.PIPELINES:
        return _json_err(404, "unknown pipeline")
    try:
        body = await request.json()
        assert isinstance(body, dict) and body
    except Exception:
        return _json_err(400, "expected a JSON object of settings")
    try:
        st, data = await _mh.put_settings(request.app["db"], cam, pipe, body)
    except Exception as e:                                # noqa: BLE001
        return _json_err(502, f"media host: {e}")
    if st < 300:
        request.app["db"].log_event(None, "camera_settings", f"{cam}/{pipe}: {json.dumps(body)[:200]}")
    return web.json_response(data, status=st)


async def post_camera_pipeline_reset(request: web.Request) -> web.Response:
    from . import media_host as _mh
    cam, pipe = request.match_info["cam"], request.match_info["pipeline"]
    if pipe not in _mh.PIPELINES:
        return _json_err(404, "unknown pipeline")
    try:
        st, data = await _mh.reset_pipeline(request.app["db"], cam, pipe)
    except Exception as e:                                # noqa: BLE001
        return _json_err(502, f"media host: {e}")
    if st < 300:
        request.app["db"].log_event(None, "camera_reset", f"{cam}/{pipe} (operator)")
    return web.json_response(data, status=st)


async def get_metrics_current(request: web.Request) -> web.Response:
    """The hour in progress: per-camera accumulators since the top of the
    hour (phase 5 of nn-video pipelines)."""
    m = request.app.get("metrics")
    if m is None:
        return _json_err(503, "metrics collector not running")
    return web.json_response(m.current())


async def get_metrics_reports(request: web.Request) -> web.Response:
    try:
        limit = max(1, min(int(request.rel_url.query.get("limit", "24")), 720))
    except ValueError:
        limit = 24
    return web.json_response({"reports": request.app["db"].list_metric_reports(limit)})


async def get_metrics_report(request: web.Request) -> web.Response:
    d = request.app["db"].get_metric_report(request.match_info["id"])
    if not d:
        return _json_err(404, "no such report")
    return web.json_response(d)


async def post_metrics_rollup(request: web.Request) -> web.Response:
    """Close the hour now (operator/test): one report for what was
    collected so far, accumulators reset."""
    m = request.app.get("metrics")
    if m is None:
        return _json_err(503, "metrics collector not running")
    try:
        await m.sample()
    except Exception:
        pass
    rep = m.rollup(reason="operator")
    return web.json_response({"ok": True, "report": rep})


async def get_media_host(request: web.Request) -> web.Response:
    """Where the media host is and how it is doing (for the Factory page
    and diagnostics)."""
    from . import media_host as _mh
    db: DB = request.app["db"]
    out = {"url": _mh.media_host_url(db)}
    try:
        st, h = await _mh.get(db, "/health", timeout=5)
        if st == 200:
            out.update({"uptime_s": h.get("uptime_s"), "rss_kb": h.get("rss_kb"),
                        "connections": sorted((h.get("connections") or {}).keys()),
                        "pipelines": {n: {"workers": p.get("workers"), "queue": p.get("queue")}
                                      for n, p in (h.get("pipelines") or {}).items()},
                        "events": (h.get("events") or [])[:10]})
        st, pi = await _mh.get(db, "/provinfo", timeout=5)
        if st == 200:
            out["provinfo"] = {k: pi.get(k) for k in ("ingest_port", "ingest_aliases", "host_key_id", "host_pub_hex", "cameras")}
        out["ok"] = True
    except Exception as e:                                # noqa: BLE001
        out.update({"ok": False, "error": str(e)[:200]})
    return web.json_response(out)


async def get_camera_slots(request: web.Request) -> web.Response:
    """Camera slots a device can be provisioned INTO — live ones and
    ones just unregistered (blocked).  The slot decides which
    video_service (and therefore which stream key) the camera uses, so
    the wizard must ask for it rather than guess from a typed label."""
    from . import cameras as _cm
    db: DB = request.app["db"]
    out, seen = [], set()
    for c in _cm.load_cameras(db):
        out.append({"id": c.id, "name": c.name, "state": "live", "free": False})
        seen.add(c.id)
    for cid in sorted(_blocked_cameras(db)):
        if cid not in seen:
            out.append({"id": cid, "name": cid,
                        "state": "unregistered", "free": True})
            seen.add(cid)
    # A camera that has never existed: the media host (one process for
    # every camera) creates its row on demand, so a free slot is no longer
    # a precondition — the wizard offers "new camera" beside the re-usable
    # unregistered slots.
    from . import media_host as _mh
    taken = seen | set(_all_camera_slots(db))
    nid = _mh.next_camera_id(taken)
    out.append({"id": nid, "name": "new camera", "state": "new", "free": True})
    return web.json_response(out)


# Linux cameras (BeagleY, nn-camera-byai) run the SAME provisioning GATT service
# and advert name as the ESP cameras — by design, so the wizard flow is shared.
# What differs is the FW_NAME characteristic: "<project> <version>", and the
# Linux build's project name is stable ("nn-camera-byai").  Classify on that
# prefix, never on the advert.
_LINUX_CAMERA_FW_PREFIXES = ("nn-camera-byai",)


def _camera_platform(fw_image: str) -> str:
    """'linux' for a BeagleY/Linux camera, else 'esp' (pure)."""
    fw = (fw_image or "").strip().lower()
    return "linux" if any(fw.startswith(p) for p in _LINUX_CAMERA_FW_PREFIXES) else "esp"


def _normalize_prov_kind(kind: str) -> tuple:
    """Job kind aliases → (flow kind, platform).  'camera_linux' is the same
    provisioning flow as 'camera_esp' (shared GATT contract)."""
    k = str(kind or "")
    if k == "camera_linux":
        return "camera_esp", "linux"
    return k, ("esp" if k == "camera_esp" else "")


async def post_provision_scan(request: web.Request) -> web.Response:
    """BLE scan for provisionable devices.  Both sensor and ESP-camera
    firmware advertise the same provisioning service UUID; the adv name
    tells them apart (media nodes advertise 'nn-media-net')."""
    from . import ble_provisioner as ble
    try:
        body = json.loads((await request.content.read(1024)).decode() or "{}")
    except Exception:
        body = {}
    try:
        devs = await ble.scan(scan_time=float(body.get("scan_time", 10.0)))
    except Exception as e:                            # noqa: BLE001
        return _json_err(502, f"BLE scan failed: {e}")
    out = []
    for dv in devs:
        name = getattr(dv, "name", "") or ""
        out.append({"addr": dv.address, "name": name,
                    "kind": ("camera_esp" if "media" in name.lower()
                             else "sensor")})
    # Enrich each candidate with its firmware identity (FW_NAME char
    # e7f00007, shared by sensor and camera provisioning services):
    # "<image> <version>", e.g. "nn-app-mdns-ot-esp32c6 0.0.31".  The
    # operator then sees WHAT they are adopting before adopting it, and
    # the OTA catalog key is known before the device joins.  Best-effort:
    # pre-e7f00007 firmware simply has no such characteristic, and a
    # candidate list is small (only unprovisioned devices advertise).
    FW_NAME_UUID = "e7f00007-6b3e-4f6b-9232-3e26d0d5a2f0"
    from bleak import BleakClient
    from . import ble_adapter
    for c in out:
        try:
            async with BleakClient(c["addr"], timeout=8.0,
                                   **ble_adapter.kwargs(None)) as cl:
                raw = bytes(await cl.read_gatt_char(FW_NAME_UUID))
            txt = raw.decode(errors="replace").strip()
            if txt:
                parts = txt.split(" ", 1)
                c["fw_image"]   = parts[0]
                c["fw_version"] = parts[1] if len(parts) > 1 else ""
        except Exception:
            pass                       # old firmware: no FW_NAME char
        if c["kind"] == "camera_esp":
            c["platform"] = _camera_platform(c.get("fw_image", ""))
            c["label"] = ("Linux camera (BeagleY)" if c["platform"] == "linux"
                          else "Wi-Fi camera (ESP)")
    return web.json_response({"candidates": out})


def _job_error_text(e: BaseException, step) -> str:
    """A job must never end with error="": str() of a bare TimeoutError or
    BleakError is empty, and one did (2026-09-14, after 'gateway identity
    attached').  Fall back to the exception type and name the step."""
    msg = str(e).strip()
    if not msg:
        msg = f"{type(e).__name__} (no message)"
        if step:
            msg += f" during step '{step}'"
    return msg[:300]


async def _prov_job_run(app, job: dict) -> None:
    db: DB = app["db"]
    enc_priv = app["enc_priv"]

    def step(name):
        job["step"] = name
        job["done_steps"].append(name)

    try:
        if job["kind"] == "sensor":
            from . import ble_provisioner as ble
            from . import network as net_mod
            from .crypto import encode_hub_config
            dataset = net_mod.get_dataset_tlvs(db)
            hub_config = encode_hub_config(job["name"], enc_priv)
            step("scan/connect")
            result = await ble.provision(
                addr=job["addr"], dataset_bytes=dataset,
                hub_config_bytes=hub_config, hub_x25519_priv=enc_priv)
            step("wait for device SUCCESS")
            device_id = result.device_x25519_pub.hex()[:16]
            enc_pub_b64 = base64.b64encode(result.device_x25519_pub).decode()
            db.register_device(device_id, job["name"], "end_device", enc_pub_b64)
            db.set_provision_info(
                device_id=device_id, device_type=job["device_type"],
                enc_pubkey_b64=enc_pub_b64,
                mdns_addr=f"{job['name']}.local", ml_eid="",
                gateway_id=job.get("gateway") or "",
                ble_addr=result.ble_addr)
            db.log_event(device_id, "device_provisioned",
                         f"type={job['device_type']} ble={result.ble_addr} (wizard)")
            step("register on hub")
            job["result"] = {"id": device_id, "name": job["name"]}
            job["state"] = "done"
            # The device's own post-attach INFO_REPLY usually arrives BEFORE
            # this registration (dropped as unknown), and nothing re-asks:
            # the record then has no image identity / capabilities until an
            # operator hits refresh (c6-s2, 2026-09-26).  Ask once it is up.
            asyncio.get_event_loop().create_task(
                _post_register_info_sync(app, device_id))
        elif job["kind"] == "camera_esp":
            from . import media_provisioner as mp
            step("scan/connect")
            # The STREAM key is not the hub key: each camera's
            # video_service owns its own keydir (nn-video2 → keys2, …).
            # Provisioning a camera with the hub key makes its ECDH
            # handshake "succeed" and then every sealed record fails with
            # InvalidTag — silent, and it cost a full re-provision cycle
            # to diagnose (2026-08-21).  Explicit or nothing.
            # Resolve the target service's real endpoint + key.  Asking
            # the operator for these is how you get an InvalidTag loop or
            # an empty control endpoint; the deployment already knows
            # them, so the wizard should never have to.
            spub = None
            if job.get("stream_pub"):
                spub = bytes.fromhex(job["stream_pub"])
            elif job.get("target_cam"):
                from . import cameras as _cm
                want = str(job["target_cam"]).strip().lower()
                # Match by slot id OR display name: an operator types the
                # label they see ("Wide NoIR"), not the internal id.
                # include_blocked: the slot being re-provisioned is
                # blocked BY DEFINITION (that is what unregister did), and
                # its URL may come from env config rather than the DB.
                cams = _cm.load_cameras(db, include_blocked=True)
                live_ids = {c.id for c in _cm.load_cameras(db)}
                tgt = next((c for c in cams if c.id.lower() == want), None)
                if tgt is None:
                    tgt = next((c for c in cams
                                if (c.name or "").strip().lower() == want), None)
                url = tgt.url if tgt else job.get("target_url")
                if tgt is not None and tgt.id in live_ids:
                    # OCCUPIED slot: a live camera already streams here.
                    # Provisioning a second device into it would point two
                    # cameras at one service (and one would win at random).
                    # The operator must say which slot they mean.
                    free = sorted(_blocked_cameras(db))
                    raise ValueError(
                        f"slot '{tgt.id}' ({tgt.name}) is already live with "
                        f"a camera — unregister that camera first, or pick "
                        f"the slot this device belongs to"
                        + (f" (awaiting a device: {', '.join(free)})"
                           if free else ""))
                if url is None or url == "":
                    # A slot that is currently BLOCKED (just unregistered)
                    # is absent from load_cameras, yet it is exactly the
                    # slot being re-provisioned — fall back to its env/DB
                    # URL so the wizard works mid-lifecycle.
                    for row in (db.list_cameras() or []):
                        if str(row["id"]).lower() == want:
                            url = str(row["url"]).rstrip("/")
                            job["target_cam"] = row["id"]
                            break
                from . import media_host as _mh
                if tgt is None and not url:
                    # a camera that has never existed: the media host owns
                    # the row; provision with ITS ingest port and default key
                    step("create camera on the media host")
                    await _mh.ensure_camera(db, want, job.get("name") or want)
                    pi = await _mh.provinfo(db)
                    spub = bytes.fromhex(pi["host_pub_hex"])
                    job["stream_port"] = int(pi["ingest_port"])
                    job["target_cam"] = want
                    job["host_key_id"] = pi.get("host_key_id") or "default"
                    step(f"new camera {want}: media host ingest port {job['stream_port']}")
                elif url:
                    async with ClientSession() as cs:
                        async with cs.get(f"{url}/api/camera/provinfo",
                                          timeout=ClientTimeout(total=6)) as r:
                            pi = await r.json()
                    if pi.get("stream_pub"):
                        spub = bytes.fromhex(pi["stream_pub"])
                        job["stream_port"] = int(pi.get("stream_port")
                                                 or job["stream_port"])
                        step("resolved stream endpoint from " + job["target_cam"])
                    try:
                        job["host_key_id"] = await _mh.camera_host_key_id(db, job["target_cam"])
                    except Exception:
                        job["host_key_id"] = job["target_cam"]
            if spub is not None and len(spub) != 32:
                raise ValueError("stream_pub must be 32 bytes hex")
            if not spub:
                free = _free_camera_slots(db)
                if not job.get("target_cam") and not free:
                    raise ValueError(
                        "every camera slot is already in use.  A new camera "
                        "needs its own video service on the media host — "
                        "add one (nn-video<N>: its own ingest port, control "
                        "port and keydir), then it appears here as a free "
                        "slot.")
                if not job.get("target_cam") and len(free) > 1:
                    raise ValueError(
                        "more than one camera slot is free (" +
                        ", ".join(free) + ") — say which one this device "
                        "should use.")
                tc = str(job.get("target_cam") or "")
                if tc and tc.lower() in {s.lower() for s in _all_camera_slots(db)}:
                    raise ValueError(_MSG_SLOT_NO_URL.format(slot=tc))
                raise ValueError(
                    f"'{tc}' is not a camera slot.  Free: "
                    + (", ".join(free) if free else "none")
                    + ".  (The name field is only a label.)")
            # A slot is not interchangeable with another: each carries its own
            # video service, keydir and encoder tuning.  If this slot was last
            # provisioned by a DIFFERENT board, the operator is about to swap
            # two cameras — refuse unless they say that is what they meant.
            _prev = _slot_prev_addr(db, job.get("target_cam"))
            if (_prev and job.get("addr")
                    and _prev != str(job["addr"]).upper()
                    and not job.get("allow_swap")):
                raise ValueError(
                    f"slot {job['target_cam']} was last used by {_prev}, but "
                    f"{job['addr']} is being provisioned into it.  Provision "
                    f"{_prev} back into {job['target_cam']}, or re-send with "
                    f"allow_swap=true if the cameras really did change places.")
            # Camera-as-gateway: every provision delivers the dormant
            # gateway identity (TLV 0x47), even to hardware that cannot
            # use it — enabling the role later must not need a
            # re-provision.  Old firmware ignores the trailing TLV.
            _gwb = b""
            if job.get("target_cam"):
                try:
                    _gwb = json.dumps(
                        _camera_gateway_creds(db, job["target_cam"]),
                        separators=(",", ":")).encode()
                    step("gateway identity attached (dormant)")
                except Exception as _ge:                  # noqa: BLE001
                    log.warning("gw creds for %s: %s", job["target_cam"], _ge)
            result = await mp.provision(
                job["addr"], name=job["name"],
                ssid=job["ssid"], password=job["password"],
                hub_x25519_priv=enc_priv,
                stream_x25519_pub=spub,
                stream_host=job["stream_host"], stream_port=job["stream_port"],
                hub_host=job["hub_host"], hub_port=job["hub_port"],
                gw_blob=_gwb or None)
            step("wait for device SUCCESS")
            # Provisioning a device INTO a slot IS the adoption of that
            # slot — requiring a separate Adopt click afterwards just
            # leaves the camera 409-ing forever (observed 2026-08-21).
            # The name the operator typed is what the webapp must show.
            if job.get("target_cam") and job.get("name"):
                db.set_setting("camera_name#" + job["target_cam"], job["name"])
            # Remember WHICH physical board owns this slot.  Without it the
            # hub cannot tell one ESP camera from another, so re-provisioning
            # two cameras at once silently swaps them — each slot keeps the
            # other's tuning (encoder QP, transcode path) and one of them
            # stops producing video (2026-08-21).  See _addr_slot_warning().
            if job.get("target_cam") and job.get("addr"):
                db.set_setting("camera_addr#" + job["target_cam"],
                               str(job["addr"]).upper())
            unconf = bool(getattr(result, "unconfirmed", False))
            job["unconfirmed"] = unconf
            tc = job.get("target_cam")
            # The media host learns the camera's own key now, so its first
            # connect is a lookup, and starts registering it with the hub.
            dpub = getattr(result, "device_x25519_pub", None)
            if tc and dpub:
                from . import media_host as _mh
                try:
                    await _mh.install_device_key(db, tc, dpub, job.get("host_key_id") or "default")
                    step("device key installed on the media host")
                except Exception as e:                    # noqa: BLE001
                    step(f"device key NOT installed on the media host ({e}); "
                         "the camera will be identified by trial decrypt")
                await _mh.set_register(db, tc, True)
            if tc:
                blocked = _blocked_cameras(db)
                if tc in blocked:
                    blocked.discard(tc)
                    _set_blocked_cameras(db, blocked)
                    import time as _t2
                    db.set_setting("camera_life_start:" + tc, str(int(_t2.time())))
                    db.log_event(None, "camera_adopted", f"{tc} (provisioned)")
                    step("adopted slot " + tc)
            job["result"] = {"ble_addr": getattr(result, "ble_addr", job["addr"]),
                             "name": job["name"], "slot": tc,
                             "unconfirmed": unconf}
            if unconf:
                # Do NOT claim success the device never confirmed.  The
                # slot is opened so it CAN register; registration is the
                # verdict the wizard waits for.
                job["state"] = "unconfirmed"
                job["error"] = ("device did not confirm provisioning — "
                                "waiting for it to register; if it never "
                                "does, the credentials were wrong")
                return
            # self-registration bullet is driven client-side by polling
            # /api/v1/cameras — the hub can't see Wi-Fi join progress.
            job["state"] = "done"
        else:
            raise ValueError(f"unknown kind {job['kind']}")
    except Exception as e:                            # noqa: BLE001
        job["state"] = "error"
        job["error"] = _job_error_text(e, job.get("step"))
    finally:
        # never keep credentials around after the job finishes
        job.pop("password", None)


async def post_provision_job(request: web.Request) -> web.Response:
    try:
        body = json.loads((await request.content.read(4096)).decode() or "{}")
    except Exception:
        return _json_err(400, "expected JSON body")
    kind = body.get("kind")
    kind, platform = _normalize_prov_kind(kind)
    if kind not in ("sensor", "camera_esp"):
        return _json_err(400, "kind must be sensor|camera_esp|camera_linux")
    for k in (("name", "addr") if kind == "sensor"
              else ("name", "addr", "ssid", "password")):
        if not body.get(k):
            return _json_err(400, f"missing '{k}'")
    _PROV_SEQ[0] += 1
    jid = f"job{_PROV_SEQ[0]}"
    job = {"id": jid, "kind": kind, "platform": platform, "state": "running", "step": "queued",
           "steps": SENSOR_STEPS if kind == "sensor" else CAMERA_STEPS,
           "done_steps": [], "error": None, "result": None,
           "name": body["name"], "addr": body["addr"],
           "device_type": body.get("device_type") or "sample_c6",
           "gateway": body.get("gateway"),
           "ssid": body.get("ssid"), "password": body.get("password"),
           "stream_host": body.get("stream_host") or _hub_lan_addr(),
           "stream_port": int(body.get("stream_port") or 8890),
           "stream_pub": body.get("stream_pub") or "",
           # target_cam = which camera SLOT (and therefore which
           # video_service) this device will stream to; defaults to a
           # slot matching the name, which is the common case for
           # re-provisioning an existing camera.
           # Slot is plumbing, not a user decision: default to THE free
           # slot when exactly one is waiting.  Only genuine ambiguity
           # (several free, or none) reaches the operator.
           # A board that has been here before goes back to ITS OWN slot —
           # "the one free slot" is only a fallback for a genuinely new
           # camera.  Without this, re-provisioning two cameras in one
           # session gives each whichever slot it happened to reach first.
           "target_cam": (body.get("target_cam")
                          or _slot_for_addr(request.app["db"], body["addr"])
                          or _auto_target_slot(request.app["db"])
                          or _new_camera_target(request.app["db"])),
           "allow_swap": bool(body.get("allow_swap")),
           "target_url": body.get("target_url") or "",
           "hub_host": body.get("hub_host") or _hub_lan_addr(),
           "hub_port": int(body.get("hub_port") or
                           os.environ.get("NN_MEDIA_CTRL_PORT") or 8772)}
    # remember the SSID (never the password) to prefill the next add
    if body.get("ssid"):
        request.app["db"].set_setting("last_wifi_ssid", body["ssid"])
    _PROV_JOBS[jid] = job
    asyncio.get_event_loop().create_task(_prov_job_run(request.app, job))
    return web.json_response({"id": jid}, status=202)


async def get_provision_job(request: web.Request) -> web.Response:
    job = _PROV_JOBS.get(request.match_info["id"])
    if not job:
        return _json_err(404, "no such job")
    return web.json_response({k: v for k, v in job.items()
                              if k not in ("password",)})


# ── app factory ────────────────────────────────────────────────────────────────


def _maybe_setup_video(app: web.Application) -> None:
    """Mount the live-video → WebRTC gateway, if configured.

    Opt-in via NN_VIDEO_SERVICE_URL (the video streaming service's control API,
    e.g. http://stream-host:8899).  Lazily imported so the hub still runs on
    deployments without aiortc/av installed.  Adds:
      POST   /api/v1/cameras/{cam}/stream              open a WebRTC channel
      POST   /api/v1/cameras/{cam}/stream/{sid}/offer  WebRTC signaling
      DELETE /api/v1/cameras/{cam}/stream/{sid}        tear down
      GET    /webrtc/{sid}                             viewer page
    """
    svc = os.environ.get("NN_VIDEO_SERVICE_URL")
    if not svc:
        return
    try:
        from .video_gateway import VideoGateway, setup_video_routes
    except Exception as e:  # aiortc/av not installed
        log.warning("video gateway not mounted (%s); pip install aiortc to enable", e)
        return
    recv_ip = os.environ.get("NN_VIDEO_RECV_IP", "127.0.0.1")
    gw = VideoGateway(svc, recv_ip)
    app["video_gateway"] = gw
    setup_video_routes(app, gw)

    async def _cleanup(app):
        for sid in list(gw.sessions):
            await gw.close_stream(sid)
    app.on_cleanup.append(_cleanup)
    log.info("video gateway mounted (service=%s recv_ip=%s)", svc, recv_ip)


_PENDING_HINT = (
    "Device is sending frames but is not enrolled in the hub. "
    "Recover via either: "
    "(a) BLE re-pair — `nn-hub device new` while the sensor advertises, or "
    "(b) Direct enroll — POST /api/v1/devices with this device_id and the "
    "sensor's X25519 pubkey if you already have it."
)


def _unknown_device_response(code: int, dev_query: str, router) -> web.Response:
    """404 body enriched with a recovery hint if dev_query matches a
    pending unknown-device sighting (i.e. the device IS talking but
    isn't registered yet)."""
    extra: dict = {}
    if router is not None and len(dev_query) == 16:
        try:
            if router.is_pending_device(dev_query):
                extra["pending"] = True
                extra["hint"]    = _PENDING_HINT
                extra["pending_url"] = "/api/v1/devices/pending"
        except Exception:
            pass
    return _json_err(code, "device not found", extra or None)

async def get_hub_identity(request: web.Request) -> web.Response:
    """Public hub identity for standalone provisioners to pin.

    A provisioner uses this to fetch:
      - hub_id      (8B, hex)        — outer nn_proto envelope id
      - p256_pub    (65B uncompressed, hex) — verify hub-signed frames
      - x25519_pub  (32B, hex)       — encrypt H2D/provisioning blobs

    All three are public; the endpoint is unauthenticated when no
    bearer token is configured.  The provisioner pins these and then
    BLE-writes them to the device/gateway being onboarded so the
    end-user's WiFi PSK never has to flow through the hub.
    """
    from .ble_gateway_provisioner import load_hub_identity
    enc_priv = request.app["enc_priv"]
    try:
        hub_id, p256_pub = load_hub_identity()
    except Exception as e:
        return _json_err(500, f"hub identity unavailable: {e}")
    x25519_pub = enc_priv.public_key().public_bytes_raw()
    return web.json_response({
        "hub_id":     hub_id.hex(),
        "p256_pub":   p256_pub.hex(),
        "x25519_pub": x25519_pub.hex(),
    })

async def list_pending_devices(request: web.Request) -> web.Response:
    """Devices the hub has seen on the mesh but doesn't have enrolled.

    The list is in-memory only (cleared on restart) and capped at
    PENDING_MAX entries; entries also age out after PENDING_TTL_SEC.
    Each entry carries a recovery hint pointing the operator at the
    `nn-hub device new` BLE flow or the direct POST /api/v1/devices
    pubkey-enroll path.
    """
    router = request.app.get("proto_router")
    if router is None:
        return web.json_response([])
    out = []
    for entry in router.get_pending_devices():
        e = dict(entry)
        e["hint"] = _PENDING_HINT
        out.append(e)
    return web.json_response(out)

async def get_device_log_cap(request: web.Request) -> web.Response:
    """Cap for the hub's device-log store (rotating sqlite files of
    sensor LOG_LINEs).  Also reports current usage so the setting has
    context."""
    db: DB = request.app["db"]
    store = request.app["log_store"]
    try:
        mb = int(db.get_setting("device_log_cap_mb", "256") or "256")
    except ValueError:
        mb = 256
    return web.json_response({"cap_mb": mb,
                              "used_mb": round(store.total_bytes() / 1048576, 1),
                              "files": len(store.list_files())})


async def put_device_log_cap(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    store = request.app["log_store"]
    try:
        body = await request.json()
        mb = int(body["cap_mb"])
        assert 16 <= mb <= 65536
    except Exception:
        return _json_err(400, "expected JSON {'cap_mb': 16..65536}")
    db.set_setting("device_log_cap_mb", str(mb))
    store.set_cap_mb(mb)                     # applies + prunes immediately
    return web.json_response({"cap_mb": mb,
                              "used_mb": round(store.total_bytes() / 1048576, 1),
                              "files": len(store.list_files())})


async def get_camera_log_cap(request: web.Request) -> web.Response:
    """Size cap (KB) for the Linux cameras' on-device app log.  The OTA
    agent reads this every tick and copy-truncates the log beyond it."""
    db: DB = request.app["db"]
    try:
        kb = int(db.get_setting("camera_log_cap_kb", "4096") or "4096")
    except (TypeError, ValueError):
        kb = 4096
    return web.json_response({"cap_kb": kb})

async def put_camera_log_cap(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    try:
        body = await request.json()
        kb = int(body["cap_kb"])
        assert 64 <= kb <= 1048576
    except Exception:
        return _json_err(400, "expected JSON {'cap_kb': 64..1048576}")
    db.set_setting("camera_log_cap_kb", str(kb))
    return web.json_response({"cap_kb": kb})

# ── stream policy: how a camera should degrade under congestion ────────
# Two strategies, chosen per camera by the operator:
#   quality : hold the picture sharp — pin fps to the chosen level and let
#             bitrate ride up to whatever the network sustains.
#   fps     : hold motion smooth — keep full fps and shed bitrate first,
#             only dropping frames once bitrate reaches the chosen floor.
# The slider means different things in each mode, so its resolved value is
# stored alongside the level (see stream_policy_resolved).
STREAM_POLICY_LEVELS = 5

def _stream_policy_doc(db, cam: str) -> dict:
    raw = db.get_setting(f"stream_policy:{cam}")
    try:
        d = json.loads(raw) if raw else {}
    except Exception:
        d = {}
    return {"mode": d.get("mode", "fps"), "level": int(d.get("level", 5))}

async def get_camera_stream_policy(request: web.Request) -> web.Response:
    """Current policy plus everything the slider needs to render itself:
    the sensor's max fps and the last measured link bandwidth, both read
    from the camera's own reports."""
    db: DB = request.app["db"]
    cam = request.match_info["cam"]
    doc = _stream_policy_doc(db, cam)
    max_fps, net_kbps = 30, None
    c = _get_camera_or_404(request)
    if c is not None:
        try:
            async with ClientSession() as cs:
                async with cs.get(f"{c.url}/api/camera/settings",
                                  timeout=ClientTimeout(total=4)) as r:
                    if r.status == 200:
                        s = (await r.json()).get("settings") or {}
                        max_fps = int(s.get("fps") or 30)
                        if s.get("net_kbps"):
                            net_kbps = int(s["net_kbps"])
        except Exception:
            pass
    doc["max_fps"] = max_fps
    doc["net_kbps"] = net_kbps
    doc["levels"] = STREAM_POLICY_LEVELS
    # the concrete stops the UI shows, computed the way the operator asked
    if doc["mode"] == "quality":
        step = (max_fps - 5) / STREAM_POLICY_LEVELS
        doc["stops"] = [round(5 + step * i) for i in range(STREAM_POLICY_LEVELS + 1)]
        doc["unit"] = "fps"
    else:
        top = max(int(net_kbps or 6000), 2000)
        step = (top - 1000) / STREAM_POLICY_LEVELS
        doc["stops"] = [round(1000 + step * i) for i in range(STREAM_POLICY_LEVELS + 1)]
        doc["unit"] = "kbps"
    doc["value"] = doc["stops"][max(0, min(doc["level"], len(doc["stops"]) - 1))]
    return web.json_response(doc)

async def put_camera_stream_policy(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    cam = request.match_info["cam"]
    try:
        body = await request.json()
        mode = str(body["mode"])
        level = int(body["level"])
        assert mode in ("fps", "quality")
        assert 0 <= level <= STREAM_POLICY_LEVELS
    except Exception:
        return _json_err(400, "expected {'mode': 'fps'|'quality', "
                              f"'level': 0..{STREAM_POLICY_LEVELS}}}")
    db.set_setting(f"stream_policy:{cam}", json.dumps({"mode": mode, "level": level}))
    return web.json_response({"mode": mode, "level": level})

async def put_camera_bundle(request: web.Request) -> web.Response:
    """Bundle-version report from a Linux camera's OTA agent.  The agent
    calls this every tick, so the hub's view survives hub restarts and
    catches rollbacks (the reported version simply moves back)."""
    return await _store_camera_bundle(request, request.match_info["cam"])


async def put_camera_bundle_by_addr(request: web.Request) -> web.Response:
    """Same report, addressed by the board's BLE address instead of the
    slot id.  A board does not know which slot it was provisioned into
    (the slot is hub-side plumbing), but it does know its own address,
    and the hub recorded that address against the slot at provisioning
    time (camera_addr#<slot>).  Used by nn-sysupd on the Buildroot image,
    which has no camera app and no NN_CAM_ID."""
    db: DB = request.app["db"]
    addr = request.match_info["addr"]
    slot = _slot_for_addr(db, addr)
    if not slot:
        return _json_err(404, f"no camera slot is bound to a board with BLE "
                              f"address {addr.upper()} — provision it first")
    return await _store_camera_bundle(request, slot)


async def _store_camera_bundle(request: web.Request, cam: str) -> web.Response:
    db: DB = request.app["db"]
    try:
        body = await request.json()
        version = str(body["version"])
        device_type = str(body.get("device_type", ""))
    except Exception:
        return _json_err(400, "expected JSON {'version', 'device_type'?}")
    import time
    doc = {"version": version, "device_type": device_type,
           "reported_at": int(time.time())}
    # The Linux camera has three independent update layers; the agent of
    # each reports its running version so the firmware panel shows all
    # of them: bundle (this doc's version), the edgeai platform, and the
    # A/B system slot (byai_system: kernel + rootfs, layout "ab-v1").
    for k in ("platform", "system", "layout", "slot", "sysupd"):
        v = body.get(k)
        if isinstance(v, str) and v:
            doc[k] = v[:64]
    # Watchdog telemetry (ESP cameras): reset reason of the current boot
    # and the lifetime count of watchdog-caused resets.  A camera that is
    # self-recovering in production must be visible here, not just green
    # between reboots.
    if isinstance(body.get("rst"), str) and body["rst"]:
        doc["rst"] = body["rst"]
    if isinstance(body.get("wdt_rc"), int):
        doc["wdt_rc"] = body["wdt_rc"]
    db.set_setting(f"camera_bundle:{cam}", json.dumps(doc))
    return web.json_response({"ok": True, "slot": cam})

async def get_camera_bundle(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    cam = request.match_info["cam"]
    raw = db.get_setting(f"camera_bundle:{cam}")
    try:
        return web.json_response(json.loads(raw) if raw else {})
    except Exception:
        return web.json_response({})

async def get_device_card_config(request: web.Request) -> web.Response:
    """Per-device webapp card config: which field controls are shown on
    the Devices-page sensor card.  Stored as a settings row so it needs
    no schema change.  Shape: {"hidden": ["field", ...]} — fields not
    listed are visible (new fields default to shown)."""
    db: DB = request.app["db"]
    dev = request.match_info["dev"]
    # Key by DEVICE ID, not by the URL string: {dev} may be a name or an
    # id (so a read and a write could disagree), and worse, a name is
    # REUSABLE — an archived device and its live replacement share it,
    # which let an archive purge delete a live device's config.
    d = _resolve_device(db, dev)
    did = d.id if d else dev
    raw = db.get_setting(f"card_cfg#{did}")
    if raw is None and d:
        # legacy name-keyed row: adopt it once, then it lives under the id
        legacy = db.get_setting(f"card_cfg:{d.name}")
        if legacy is not None:
            db.set_setting(f"card_cfg#{did}", legacy)
            raw = legacy
    try:
        cfg = json.loads(raw) if raw else {}
    except Exception:
        cfg = {}
    return web.json_response({"hidden": cfg.get("hidden", [])})

async def put_device_card_config(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    dev = request.match_info["dev"]
    try:
        body = await request.json()
        hidden = body.get("hidden", [])
        assert isinstance(hidden, list)
        hidden = [str(x) for x in hidden]
    except Exception:
        return _json_err(400, "expected JSON {'hidden': [field, ...]}")
    d = _resolve_device(db, dev)
    db.set_setting(f"card_cfg#{d.id if d else dev}",
                   json.dumps({"hidden": hidden}))
    return web.json_response({"hidden": hidden})

async def get_device_fields_cache(request: web.Request) -> web.Response:
    """Live actuator values from the device's fire-and-forget AUTO_EVENT
    pushes (emitted whenever an actuator changes, from any source — hub
    write, local rule, D2D cascade).  Served from the hub's in-memory
    cache: polling this costs NO mesh traffic, so the webapp cards can
    refresh every few seconds and track automation outcomes live."""
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _unknown_device_response(404, request.match_info["dev"], router)
    cache = router.get_field_cache(d.id) if router else {}
    return web.json_response({"fields": cache})


async def _post_register_info_sync(app, device_id: str) -> None:
    """After the wizard registers a sensor: INFO_QUERY it as soon as it is
    reachable (it attaches to the mesh a few seconds later), a few tries."""
    router = app.get("proto_router")
    if router is None:
        return
    for attempt in range(8):                       # ~2 min of joining time
        await asyncio.sleep(15)
        gw = router._resolve_gateway_for_device(device_id)
        if not gw or not router._server.is_gateway_online(gw):
            continue
        try:
            await router.request_h2d(device_id, req_cmd=proto.Cmd.INFO_QUERY,
                                     body=b"", expected_reply_cmd=proto.Cmd.INFO_REPLY,
                                     timeout=10.0, max_attempts=2)
            log.info("[%s] post-register INFO_QUERY answered (try %d)", device_id, attempt + 1)
            return
        except Exception as e:                     # noqa: BLE001
            log.info("[%s] post-register INFO_QUERY try %d: %s", device_id, attempt + 1, e)
    log.warning("[%s] post-register INFO_QUERY: no answer; use info/refresh", device_id)


async def post_device_info_refresh(request: web.Request) -> web.Response:
    """Pull a fresh config sync from *dev*: H2D INFO_QUERY → INFO_REPLY.

    The reply's `fields` descriptors are persisted into
    provision_info.capabilities by the proto_router's INFO_REPLY consumer
    (same path as an unsolicited sync); the parsed document is returned so
    the webapp can render immediately without a second round trip.
    """
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    if router is None:
        return _json_err(503, "proto_router not running")
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _unknown_device_response(404, request.match_info["dev"], router)
    try:
        body = await router.request_h2d(
            d.id,
            req_cmd=proto.Cmd.INFO_QUERY,
            body=b"",
            expected_reply_cmd=proto.Cmd.INFO_REPLY,
            timeout=10.0,
            max_attempts=3,
        )
    except Exception as e:
        return _json_err(504, f"device did not answer INFO_QUERY: {e}")
    try:
        doc = json.loads(body.decode("utf-8", errors="replace") or "{}")
    except Exception:
        return _json_err(502, "device sent unparseable INFO_REPLY")
    return web.json_response(doc)


async def get_device_uptime(request: web.Request) -> web.Response:
    """Passive sensor uptime: the DEVICE_HEARTBEAT frame has carried
    uptime_ms in its first 4 bytes all along (30 s idle-backstop cadence);
    the router now keeps the latest per device.  404 = no heartbeat heard
    since the hub started — the caller can fall back to a live INFO_QUERY."""
    import time as _t
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    if router is None:
        return _json_err(503, "proto_router not running")
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _unknown_device_response(404, request.match_info["dev"], router)
    hb = getattr(router, "hb_uptime", {}).get(d.id)
    if not hb:
        return _json_err(404, "no heartbeat heard yet (hub recently "
                              "restarted, or device offline)")
    age = _t.time() - hb["at"]
    return web.json_response({
        # uptime advances with wall clock between heartbeats
        "up_s": int(hb["uptime_ms"] / 1000 + age),
        "age_s": round(age, 1),
        "source": "heartbeat",
    })


async def post_device_reboot(request: web.Request) -> web.Response:
    """Operator-requested device reboot (fw 0.0.27+).

    H2D REBOOT → device ACKs (REBOOT_ACK, 1B status) and warm-reboots
    ~500 ms later, after the ACK has left the radio.  A 200 here means
    "the device acknowledged and is going down", NOT "it is back" — the
    device then re-attaches on its own (MANUAL_START + auto-attach), which
    takes tens of seconds on the mesh.  Older firmware doesn't know the
    command and simply never answers → 504, reported as such."""
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    if router is None:
        return _json_err(503, "proto_router not running")
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _unknown_device_response(404, request.match_info["dev"], router)
    try:
        # max_attempts=1, deliberately: a REBOOT retry is not idempotent.
        # The device cannot dedup across the reset (RAM is gone), so a
        # retry that arrives after re-attach reboots it AGAIN — observed
        # on c6-s1 as a double reboot right after rollout.  One attempt;
        # a lost ACK surfaces as 504 and the operator can see the device
        # came back anyway.
        body = await router.request_h2d(
            d.id, req_cmd=proto.Cmd.REBOOT, body=b"",
            expected_reply_cmd=proto.Cmd.REBOOT_ACK,
            timeout=10.0, max_attempts=1)
    except Exception as e:
        return _json_err(504, "device did not acknowledge REBOOT "
                              f"(firmware < 0.0.27, offline, or the ack was "
                              f"lost — check whether it comes back anyway): {e}")
    status = body[0] if body else 255
    if status != 0:
        return _json_err(502, f"device refused reboot (status {status})")
    return web.json_response({"ok": True, "device": d.id,
                              "note": "acknowledged — rebooting; expect the "
                                      "device back on the mesh within ~60 s"})


async def get_camera_uptime(request: web.Request) -> web.Response:
    """Device uptime for a camera, proxied from its video_service — which
    reads it off traffic the device already sends (heartbeat / periodic
    status record).  No device round trip happens here."""
    from . import cameras as cams_mod
    db: DB = request.app["db"]
    c = next((x for x in cams_mod.load_cameras(db)
              if x.id == request.match_info["cam"]), None)
    if not c:
        return _json_err(404, "camera not found")
    try:
        async with ClientSession() as cs:
            async with cs.get(f"{c.url}/api/camera/uptime",
                              timeout=ClientTimeout(total=5)) as r:
                return web.Response(body=await r.read(), status=r.status,
                                    content_type="application/json")
    except Exception as e:
        return _json_err(502, f"camera service unreachable: {e}")


async def post_camera_reboot(request: web.Request) -> web.Response:
    """Operator reboot for a CAMERA: proxied to its video_service, which
    sends CTRL_REBOOT (0xC7 cmd 20) down the device control channel.

    Fire-and-forget on the wire: the device drops its session as it goes
    down, so there is no ack — the proof is the stream dropping and coming
    back, which the webapp watches.  ESP cameras hard-restart; cam3's app
    restarts cleanly (a dirty kill would wedge its C7x).  Firmware that
    predates cmd 20 ignores it, which surfaces as "no visible effect"."""
    from . import cameras as cams_mod
    db: DB = request.app["db"]
    cam_id = request.match_info["cam"]
    c = next((x for x in cams_mod.load_cameras(db) if x.id == cam_id), None)
    if not c:
        return _json_err(404, "camera not found")
    try:
        async with ClientSession() as cs:
            async with cs.post(f"{c.url}/api/camera/reboot",
                               timeout=ClientTimeout(total=8)) as r:
                body = await r.read()
                return web.Response(body=body, status=r.status,
                                    content_type="application/json")
    except Exception as e:
        return _json_err(502, f"camera service unreachable: {e}")


async def get_device_ota(request: web.Request) -> web.Response:
    """Snapshot of in-flight / most recent OTA for *dev*.

    Derived from cached OTA_CHECK + OTA_BLOCK_REQ events in the
    proto_router.  Use this to drive a progress UI without scraping
    the hub log.
    """
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    if router is None:
        return _json_err(503, "proto_router not available")
    dev_q = request.match_info["dev"]
    d = _resolve_device(db, dev_q)
    if not d:
        return _unknown_device_response(404, dev_q, router)
    snap = router.get_ota_status(d.id)
    snap["device_id"]   = d.id
    snap["device_name"] = d.name
    snap["device_type"] = d.type
    fw = db.get_firmware_target(d.type)
    if fw:
        snap.setdefault("target_version", fw.target_version)
        snap.setdefault("target_size",    int(fw.size_bytes))
    return web.json_response(snap)

def _catalog(request: web.Request):
    """Resolve the FirmwareCatalog from the request, or 503 if not configured."""
    cat = request.app.get("firmware_catalog")
    if cat is None:
        return None
    return cat

def _entry_format(e) -> str:
    """Image format from the manifest ('mcuboot' when absent)."""
    try:
        return str(json.loads(e.manifest_json or "{}").get("format") or "mcuboot")
    except Exception:
        return "mcuboot"


def _catalog_entry_dict(e) -> dict:
    return {
        "device_type":   e.device_type,
        "version":       e.version,
        "format":        _entry_format(e),
        "source_name":   e.source_name,
        "asset_uri":     e.asset_uri,
        "sha256":        e.sha256,
        "size_bytes":    e.size_bytes,
        "is_cached":     e.is_cached,
        "local_path":    e.local_path,
        "downloaded_at": e.downloaded_at,
        "first_seen_at": e.first_seen_at,
        "last_seen_at":  e.last_seen_at,
    }

async def list_firmware_sources(request: web.Request) -> web.Response:
    """List configured firmware sources + last sync state."""
    cat = _catalog(request)
    if cat is None:
        return _json_err(503, "firmware_catalog is not configured")
    out = []
    for name in cat.source_names():
        state = cat.source_state(name)
        out.append({
            "name":          name,
            "kind":          type(state.source).__name__,
            "poll_seconds":  state.poll_seconds,
            "last_sync_at":  state.last_sync_at,
            "last_error":    state.last_error,
        })
    return web.json_response(out)

# ── Factory: flash a device over a hub-attached serial port ────────────────
#
# Contract: every flashable firmware ships a `flash.sh` NEXT TO its
# image.signed.bin + manifest.json (third release asset on GitHub, third
# file in a local source dir).  The hub does not know chips or offsets —
# the script does.  It runs with:
#   NN_FLASH_PORT     the chosen serial device (e.g. /dev/ttyACM1)
#   NN_FLASH_IMAGE    path to the cached image
#   NN_FLASH_ESPTOOL  esptool invocation prefix (the hub venv's)
# Trust model: catalog sources are operator-configured (their own repos /
# their own directory), so the script carries the same trust as the
# firmware itself.

_flash_jobs: dict = {}


def _serial_ports() -> list:
    """Hub-attached serial ports, gateway's NCP ports marked busy."""
    import glob as _glob
    import subprocess as _sp
    out = []
    for link in sorted(_glob.glob("/dev/serial/by-id/*")):
        dev = os.path.realpath(link)
        holder = ""
        try:
            r = _sp.run(["fuser", dev], capture_output=True, timeout=5)
            if r.stdout.strip():
                holder = "in use"
        except Exception:
            pass
        out.append({"id": os.path.basename(link), "path": link,
                    "dev": dev, "busy": bool(holder), "holder": holder})
    return out


def _gateway_locked_devs() -> set:
    """Devices the gateway ACTUALLY holds (published by the root helper
    nn-gw-portstat — the hub can't read root's /proc fds itself)."""
    try:
        return {l.strip() for l in
                Path("/run/nn-gw-active-ports").read_text().splitlines()
                if l.strip()}
    except Exception:
        return set()


async def get_factory_ports(request: web.Request) -> web.Response:
    ports = await asyncio.to_thread(_serial_ports)
    # Precision matters here in BOTH directions: locking every Espressif
    # CANDIDATE blocked flashing a freshly plugged blank board, while
    # missing the real NCP would let a flash kill the mesh.  Rule:
    #  - ports the gateway process HOLDS (portstat file): hard lock;
    #  - UART-bridge candidates (CH34x): lock too — the NCP board's
    #    second cable is indistinguishable from a fresh board there;
    #  - other native USB ports: free.
    held = _gateway_locked_devs()
    try:
        async with ClientSession() as cs:
            async with cs.get("http://127.0.0.1:8769/api/v1/gateway/service",
                              timeout=ClientTimeout(total=5)) as r:
                gw = await r.json()
        claimed = set(gw.get("ncp_ports") or [])
    except Exception:
        claimed = set()
    for p in ports:
        if p["dev"] in held:
            p["busy"] = True
            p["holder"] = "gateway NCP (in use) — never flash this"
        elif (p["path"] in claimed or p["dev"] in claimed) and "1a86" in p["id"]:
            p["busy"] = True
            p["holder"] = "possible gateway cable (UART bridge) — flash from a bench if you are sure"
    return web.json_response({"ports": ports})


def _flash_script_for(entry: dict, cache_dir) -> str:
    """Locate/settle flash.sh for a catalog entry; '' if the release has none."""
    uri = str(entry.get("asset_uri") or "")
    if uri and not uri.startswith(("http://", "https://")):
        cand = Path(uri).parent / "flash.sh"
        return str(cand) if cand.is_file() else ""
    if uri.startswith(("http://", "https://")):
        # GitHub release: sibling asset named flash.sh, cached beside the image
        import urllib.request
        dst = Path(cache_dir) / entry["device_type"] / entry["version"] / "flash.sh"
        if dst.is_file():
            return str(dst)
        sib = uri.rsplit("/", 1)[0] + "/flash.sh"
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            req = urllib.request.Request(sib)
            tok_env = entry.get("pat_env")
            if tok_env and os.environ.get(tok_env):
                req.add_header("Authorization", "token " + os.environ[tok_env])
            with urllib.request.urlopen(req, timeout=20) as r:
                dst.write_bytes(r.read())
            return str(dst)
        except Exception:
            return ""
    return ""


async def post_factory_flash(request: web.Request) -> web.Response:
    """Start a flash job: {port, device_type, version} → {job}."""
    cat = _catalog(request)
    if cat is None:
        return _json_err(503, "firmware_catalog is not configured")
    try:
        body = json.loads((await request.content.read(2048)).decode() or "{}")
    except Exception:
        return _json_err(400, "invalid JSON body")
    port = str(body.get("port") or "")
    dtype = str(body.get("device_type") or "")
    version = str(body.get("version") or "")
    if not (port and dtype and version):
        return _json_err(400, "port, device_type and version are required")

    ports = {p["path"]: p for p in await asyncio.to_thread(_serial_ports)}
    ports.update({p["id"]: p for p in ports.copy().values()})
    sel = ports.get(port)
    if sel is None:
        return _json_err(404, f"no such serial port: {port}")
    # re-check the gateway's actually-held ports at flash time
    if sel["dev"] in _gateway_locked_devs():
        return _json_err(409, "that port is the gateway NCP — flashing it "
                              "would take down the mesh")
    if sel["busy"]:
        return _json_err(409, f"port is busy: {sel['holder'] or 'in use'}")

    db: DB = request.app["db"]
    rows = [_catalog_entry_dict(e) for e in db.list_catalog(dtype)
            if e.version == version]
    if not rows:
        return _json_err(404, f"{dtype} {version} is not in the catalog")
    entry = rows[0]
    try:
        img = await cat.ensure_cached(dtype, version, entry.get("source_name"))
    except Exception as e:                            # noqa: BLE001
        return _json_err(502, f"could not cache the image: {e}")
    script = await asyncio.to_thread(_flash_script_for, entry, cat._cache_dir)
    if not script:
        return _json_err(422, f"{dtype} {version} has no flash.sh — this "
                              "release cannot be flashed from the hub.  "
                              "Publish a flash.sh beside its image and "
                              "manifest.")

    jid = f"flash{len(_flash_jobs) + 1}"
    job = {"id": jid, "state": "running", "port": sel["dev"],
           "device_type": dtype, "version": version, "log": "", "rc": None}
    _flash_jobs[jid] = job

    async def run():
        env = dict(os.environ)
        env["NN_FLASH_PORT"] = sel["dev"]
        env["NN_FLASH_IMAGE"] = str(img)
        env["NN_FLASH_ESPTOOL"] = sys.executable + " -m esptool"
        try:
            proc = await asyncio.create_subprocess_exec(
                "bash", script,
                cwd=str(Path(script).parent), env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT)
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                job["log"] = (job["log"] + line.decode(errors="replace"))[-8000:]
            rc = await asyncio.wait_for(proc.wait(), timeout=300)
            job["rc"] = rc
            job["state"] = "done" if rc == 0 else "error"
        except asyncio.TimeoutError:
            try: proc.kill()
            except Exception: pass
            job["state"] = "error"
            job["log"] += "\n[hub] flash timed out after 300s"
        except Exception as e:                        # noqa: BLE001
            job["state"] = "error"
            job["log"] += f"\n[hub] {e!r}"
    asyncio.get_event_loop().create_task(run())
    return web.json_response({"id": jid, "state": "running"})


async def get_factory_flash_job(request: web.Request) -> web.Response:
    job = _flash_jobs.get(request.match_info["jid"])
    if job is None:
        return _json_err(404, "no such flash job")
    return web.json_response(job)


# ── Factory: write a whole-disk image to a hub-attached micro-SD ────────────
#
# Linux cameras (BeagleY-AI) boot from a micro-SD.  Their factory image is a
# whole-disk `image.img.xz` (catalog format "sdcard.img.xz"; manifest also
# carries raw_size_bytes + raw_sha256 of the decompressed stream).  The hub
# lists removable USB/SD disks, and writes through ONE root helper,
# /usr/local/sbin/nn-sdflash, reachable via a single sudoers line.  The
# helper re-derives every safety rule from the kernel itself — the hub's
# filter below only drives the UI.
#
# Authorization: the device port (:8769) is open on the LAN because cameras
# POST to it.  A whole-disk write must not be reachable from there, so the
# POST is accepted only when the request arrived on a loopback socket (i.e.
# through the nginx basic-auth vhost that proxies to 127.0.0.1:8769) or when
# the hub runs with a bearer token (middleware already verified it).

_SD_FORMAT = "sdcard.img.xz"
_SDFLASH_HELPER = "/usr/local/sbin/nn-sdflash"
_SD_USER_MOUNT_PREFIXES = ("/media/", "/run/media/", "/mnt/")
_SD_FLASH_TIMEOUT_S = 3 * 3600


def _lsblk_truthy(v) -> bool:
    return v in (True, 1) or str(v).lower() in ("1", "true", "yes")


def _disk_candidates(lsblk: dict) -> list:
    """Pure: filter `lsblk -J -b` output down to whole-disk removable targets.
    Mirrors the helper's rule: whole disk, removable/hotplug, transport usb
    (or an mmcblk SD slot), holding no mount outside the user-media prefixes."""
    out = []
    for d in (lsblk or {}).get("blockdevices") or []:
        if d.get("type") != "disk":
            continue
        name = str(d.get("name") or "")
        tran = str(d.get("tran") or "").lower()
        removable = _lsblk_truthy(d.get("rm")) or _lsblk_truthy(d.get("hotplug"))
        is_usb = tran == "usb"
        is_sd_slot = tran in ("", "none", "mmc") and name.startswith("mmcblk")
        if not (removable and (is_usb or is_sd_slot)):
            continue
        nodes = [d] + list(d.get("children") or [])
        mounts = [m for n in nodes for m in (n.get("mountpoints") or [])
                  if m] + [n["mountpoint"] for n in nodes if n.get("mountpoint")]
        if any(not m.startswith(_SD_USER_MOUNT_PREFIXES) for m in mounts):
            continue                      # holds a system mount: never a target
        size = int(d.get("size") or 0)
        out.append({
            "path":       str(d.get("path") or f"/dev/{name}"),
            "name":       name,
            "size_bytes": size,
            "has_media":  size > 0,
            "model":      str(d.get("model") or "").strip(),
            "vendor":     str(d.get("vendor") or "").strip(),
            "serial":     str(d.get("serial") or "").strip(),
            "tran":       tran or "mmc",
            "mounted":    sorted(set(mounts)),
            "partitions": len(d.get("children") or []),
        })
    return out


def _lsblk_json() -> dict:
    import subprocess as _sp
    r = _sp.run(["lsblk", "-J", "-b", "-o",
                 "NAME,PATH,SIZE,RM,HOTPLUG,TRAN,TYPE,MODEL,VENDOR,SERIAL,MOUNTPOINTS"],
                capture_output=True, text=True, timeout=10)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or "lsblk failed").strip())
    return json.loads(r.stdout or "{}")


def _origin_trusted(has_token: bool, local_host: str) -> bool:
    """Pure: may this request perform a privileged Factory op?"""
    if has_token:
        return True                       # _bearer_auth already verified it
    h = str(local_host or "")
    return h in ("::1", "::ffff:127.0.0.1") or h.startswith("127.")


def _request_trusted(request: web.Request) -> bool:
    try:
        sock = request.transport.get_extra_info("sockname") if request.transport else None
    except Exception:
        sock = None
    return _origin_trusted(bool(request.app.get("auth_token")),
                           sock[0] if sock else "")


_DD_PROGRESS = re.compile(rb"(\d+) bytes")


def _sdflash_feed(job: dict, chunk: bytes) -> None:
    """Pure-ish: fold helper output into the job (progress, phase, result)."""
    for raw in re.split(rb"[\r\n]", chunk):
        line = raw.strip()
        if not line:
            continue
        if line.startswith(b"PHASE "):
            job["progress"]["phase"] = line[6:].decode(errors="replace")
            job["log"] = (job["log"] + f"[{job['progress']['phase']}]\n")[-8000:]
        elif line.startswith(b"NN_SDFLASH_RESULT "):
            try:
                job["result"] = json.loads(line[18:].decode(errors="replace"))
            except Exception:
                job["result"] = {"ok": False, "error": line.decode(errors="replace")}
            job["log"] = (job["log"] + line.decode(errors="replace") + "\n")[-8000:]
        else:
            m = _DD_PROGRESS.match(line)
            if m:
                job["progress"]["bytes"] = int(m.group(1))
                job["progress"]["line"] = line.decode(errors="replace")
            else:
                job["log"] = (job["log"] + line.decode(errors="replace") + "\n")[-8000:]


async def get_factory_disks(request: web.Request) -> web.Response:
    try:
        disks = _disk_candidates(await asyncio.to_thread(_lsblk_json))
    except Exception as e:                                # noqa: BLE001
        return _json_err(500, f"could not list disks: {e}")
    return web.json_response({
        "disks": disks,
        "helper_installed": os.path.exists(_SDFLASH_HELPER),
        "trusted_origin": _request_trusted(request),
    })


async def post_factory_flash_disk(request: web.Request) -> web.Response:
    """Write a card image: {disk, device_type, version, verify?} → {job}."""
    if not _request_trusted(request):
        return _json_err(403, "whole-disk writes are only accepted through the "
                              "hub's authenticated address (the web vhost), not "
                              "on the open device port")
    cat = _catalog(request)
    if cat is None:
        return _json_err(503, "firmware_catalog is not configured")
    try:
        body = json.loads((await request.content.read(2048)).decode() or "{}")
    except Exception:
        return _json_err(400, "invalid JSON body")
    disk = str(body.get("disk") or "")
    dtype = str(body.get("device_type") or "")
    version = str(body.get("version") or "")
    verify = bool(body.get("verify", True))
    if not (disk and dtype and version):
        return _json_err(400, "disk, device_type and version are required")

    try:
        disks = {d["path"]: d for d in _disk_candidates(await asyncio.to_thread(_lsblk_json))}
    except Exception as e:                                # noqa: BLE001
        return _json_err(500, f"could not list disks: {e}")
    sel = disks.get(disk)
    if sel is None:
        return _json_err(409, f"{disk} is not a removable USB/SD disk (or holds a "
                              "system mount) — refusing")
    if not sel["has_media"]:
        return _json_err(409, f"no card in {sel['model'] or disk}")
    for j in _flash_jobs.values():
        if j.get("kind") == "sdcard" and j["state"] == "running" and j["disk"] == sel["path"]:
            return _json_err(409, f"{disk} is already being written (job {j['id']})")

    db: DB = request.app["db"]
    rows = [e for e in db.list_catalog(dtype) if e.version == version]
    if not rows:
        return _json_err(404, f"{dtype} {version} is not in the catalog")
    ent = rows[0]
    if _entry_format(ent) != _SD_FORMAT:
        return _json_err(422, f"{dtype} {version} is a {_entry_format(ent)} image, "
                              f"not a card image ({_SD_FORMAT})")
    try:
        man = json.loads(ent.manifest_json or "{}")
    except Exception:
        man = {}
    raw_size = int(man.get("raw_size_bytes") or 0)
    raw_sha = str(man.get("raw_sha256") or "")
    if raw_size and sel["size_bytes"] < raw_size:
        return _json_err(409, f"card too small: {sel['size_bytes'] / 2**30:.1f} GiB, "
                              f"image needs {raw_size / 2**30:.2f} GiB")
    if not os.path.exists(_SDFLASH_HELPER):
        return _json_err(503, f"{_SDFLASH_HELPER} is not installed on the hub — "
                              "run deploy/install-sdflash.sh as root there")
    try:
        img = await cat.ensure_cached(dtype, version, ent.source_name)
    except Exception as e:                                # noqa: BLE001
        return _json_err(502, f"could not cache the image: {e}")

    jid = f"sd{len(_flash_jobs) + 1}"
    job = {"id": jid, "kind": "sdcard", "state": "running", "disk": sel["path"],
           "disk_model": sel["model"], "device_type": dtype, "version": version,
           "verify": verify, "log": "", "rc": None, "result": None,
           "progress": {"bytes": 0, "total": raw_size, "phase": "starting"}}
    _flash_jobs[jid] = job

    async def run():
        argv = ["sudo", "-n", _SDFLASH_HELPER, "write", sel["path"], str(img)]
        if raw_size:
            argv += ["--raw-size", str(raw_size)]
        if raw_sha:
            argv += ["--raw-sha256", raw_sha]
        if not verify:
            argv += ["--no-verify"]
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT)

            async def pump():
                while True:
                    chunk = await proc.stdout.read(4096)
                    if not chunk:
                        return
                    _sdflash_feed(job, chunk)
            await asyncio.wait_for(pump(), timeout=_SD_FLASH_TIMEOUT_S)
            rc = await proc.wait()
            job["rc"] = rc
            ok = rc == 0 and bool((job.get("result") or {}).get("ok"))
            job["state"] = "done" if ok else "error"
            if ok:
                job["progress"]["bytes"] = raw_size or job["progress"]["bytes"]
                job["progress"]["phase"] = "done"
        except asyncio.TimeoutError:
            try: proc.kill()
            except Exception: pass
            job["state"] = "error"
            job["log"] += f"\n[hub] card write timed out after {_SD_FLASH_TIMEOUT_S}s"
        except Exception as e:                            # noqa: BLE001
            job["state"] = "error"
            job["log"] += f"\n[hub] {e!r}"
    asyncio.get_event_loop().create_task(run())
    return web.json_response({"id": jid, "state": "running"})


# ── Camera-as-gateway ───────────────────────────────────────────────────────
#
# A Linux camera with an NCP attached can carry the gateway role; ESP
# cameras cannot (no second radio to host).  Either way, provisioning
# delivers a DORMANT gateway identity into the camera (CONFIG TLV 0x47),
# so enabling the role later needs no re-provision.  The identity is the
# standard gateway auth: P-256 keypair, id = SHA256(pubkey)[:8], hub
# registers the pubkey in the gateways table like any other gateway.

def _camera_gateway_creds(db, cam_id: str) -> dict:
    """Mint (once) and return the gateway identity for a camera slot."""
    key = f"camera_gw_blob#{cam_id}"
    raw = db.get_setting(key)
    if raw:
        return json.loads(raw)
    import hashlib
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import serialization
    priv = ec.generate_private_key(ec.SECP256R1())
    pub_u = priv.public_key().public_bytes(
        serialization.Encoding.X962,
        serialization.PublicFormat.UncompressedPoint)      # 65B
    gw_id = hashlib.sha256(pub_u).hexdigest()[:16]         # 8B hex
    priv_hex = format(priv.private_numbers().private_value, "064x")
    blob = {"gw_id": gw_id,
            "gw_priv_hex": priv_hex,
            "gw_pub_b64": base64.b64encode(pub_u).decode(),
            "hub_host": _hub_lan_addr(),
            "proto_port": 8767}
    db.set_setting(key, json.dumps(blob))
    # Register alongside real gateways so the hub will authenticate it
    # the day it dials in.  Shows as an offline row until then.
    try:
        db.register_gateway(gw_id, f"{cam_id}-gw", blob["gw_pub_b64"])
    except Exception as e:                                # noqa: BLE001
        log.warning("camera %s: gateway registration failed: %s", cam_id, e)
    return blob


def _camera_gw_supported(db, cam_id: str) -> tuple:
    """(supported, reason) — ESP camera hardware can never host the role;
    a Linux camera's own agent reports NCP presence."""
    img = db.get_setting(f"device_image:{cam_id}") or ""
    bundle = db.get_setting(f"camera_bundle:{cam_id}") or "{}"
    try:
        dtype = json.loads(bundle).get("device_type") or img
    except Exception:
        dtype = img
    if "esp" in (dtype or "").lower():
        return False, "ESP32 camera hardware cannot host the gateway role"
    raw = db.get_setting(f"camera_gw_state#{cam_id}")
    if not raw:
        return False, "no gateway report from the camera yet"
    try:
        st = json.loads(raw)
        return bool(st.get("supported")), str(st.get("reason") or "")
    except Exception:
        return False, "malformed gateway report"


async def get_camera_gateway(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    cam = request.match_info["cam"]
    supported, reason = _camera_gw_supported(db, cam)
    enabled = (db.get_setting(f"camera_gw_enabled#{cam}") or "1") == "1"
    has_creds = bool(db.get_setting(f"camera_gw_blob#{cam}"))
    return web.json_response({"supported": supported, "reason": reason,
                              "enabled": enabled, "has_creds": has_creds})


async def put_camera_gateway(request: web.Request) -> web.Response:
    """Operator toggle: gateway role on/off for this camera."""
    db: DB = request.app["db"]
    cam = request.match_info["cam"]
    try:
        body = json.loads((await request.content.read(512)).decode() or "{}")
        enabled = bool(body["enabled"])
    except Exception:
        return _json_err(400, "expected {'enabled': bool}")
    db.set_setting(f"camera_gw_enabled#{cam}", "1" if enabled else "0")
    return web.json_response({"ok": True, "enabled": enabled})


async def post_camera_gateway_report(request: web.Request) -> web.Response:
    """Linux camera agent: NCP presence report (every tick)."""
    db: DB = request.app["db"]
    cam = request.match_info["cam"]
    try:
        body = json.loads((await request.content.read(1024)).decode() or "{}")
        st = {"supported": bool(body["supported"]),
              "reason": str(body.get("reason") or ""),
              "reported_at": int(time.time())}
    except Exception:
        return _json_err(400, "expected {'supported': bool, 'reason'?}")
    db.set_setting(f"camera_gw_state#{cam}", json.dumps(st))
    return web.json_response({"ok": True})


async def post_gateway_provision_net(request: web.Request) -> web.Response:
    """Provision a gateway that is LISTENING on the network — the last
    step of camera-as-gateway: the camera's nn-gw-agent starts gw_linux
    in provision-net mode and then asks the hub to provision it here.
    Runs the same flow as `nn-hub gateway new --transport=net`.

    body: {addr: "ip:port", name?, ssid?, psk?}
    """
    db: DB = request.app["db"]
    try:
        body = json.loads((await request.content.read(2048)).decode() or "{}")
        addr = str(body["addr"])
    except Exception:
        return _json_err(400, "expected {'addr': 'ip:port', ...}")
    from . import network as net_mod
    from .net_gateway_provisioner import provision_gateway_net
    from .crypto import load_or_generate_enc_key
    from pathlib import Path as _P
    data_dir = _P(os.environ.get("NN_HUB_DATA", os.path.expanduser("~/.nn-hub")))
    enc_priv = load_or_generate_enc_key(data_dir)
    net_mod.get_or_create_network(db)
    dataset = net_mod.get_dataset_tlvs(db)
    # ssid/psk ride the blob for historical reasons; a wired camera-host
    # gateway never uses them.  Default to the remembered SSID.
    ssid = str(body.get("ssid") or db.get_setting("prov_last_ssid") or "nn")
    # gw_provision_set requires a NON-EMPTY psk (returns -EINVAL otherwise,
    # surfaced as the misleading "NVS" status 0x05 — cost a real debug
    # session on 2026-08-28).  A wired camera host never uses it, so send
    # an explicit placeholder rather than the operator's real PSK.
    psk = str(body.get("psk") or "unused-wired-host")
    try:
        result = await provision_gateway_net(
            ssid=ssid, psk=psk, hub_host=_hub_lan_addr(),
            hub_x25519_priv=enc_priv,
            ot_dataset_tlvs=dataset,
            address=addr, scan_time=5.0)
    except Exception as e:                                # noqa: BLE001
        return _json_err(502, f"net provisioning failed: {e}")
    gw = db.register_gateway(result.gateway_id,
                             str(body.get("name") or result.gateway_id),
                             result.pubkey_b64, "")
    return web.json_response({"ok": True, "gateway_id": gw.id,
                              "name": gw.name})


async def get_camera_gateway_config(request: web.Request) -> web.Response:
    """The dormant gateway identity — the Linux agent stores this locally
    the same way ESP cameras store the CONFIG TLV in NVS."""
    db: DB = request.app["db"]
    cam = request.match_info["cam"]
    return web.json_response(_camera_gateway_creds(db, cam))


def _sources_yaml_path(cat):
    p = getattr(cat, "_sources_yaml_path", None)
    return p


def _rw_sources_yaml(cat, mutate):
    """Read sources.yaml, apply mutate(list)->list, write back atomically."""
    import yaml as _yaml
    path = _sources_yaml_path(cat)
    if path is None:
        raise RuntimeError("hub started without a sources.yaml path")
    doc = {}
    if path.is_file():
        doc = _yaml.safe_load(path.read_text()) or {}
    srcs = doc.get("sources") or []
    doc["sources"] = mutate(list(srcs))
    tmp = path.with_suffix(".tmp")
    tmp.write_text(_yaml.safe_dump(doc, sort_keys=False))
    tmp.rename(path)


async def post_firmware_source_add(request: web.Request) -> web.Response:
    """Add a firmware source (the webapp Factory section's Add button).

    body: {name, kind: "github"|"local", repo?|root?, pat_env?, poll_seconds?}

    Secrets policy: a GitHub token is referenced by ENV VAR NAME (pat_env),
    never stored in the yaml or the DB — same rule as everywhere else in
    this system.  Public repos need no token at all.
    """
    cat = _catalog(request)
    if cat is None:
        return _json_err(503, "firmware_catalog is not configured")
    try:
        body = json.loads((await request.content.read(4096)).decode() or "{}")
    except Exception:
        return _json_err(400, "invalid JSON body")
    name = str(body.get("name") or "").strip()
    kind = str(body.get("kind") or "github").strip()
    if not name or not name.replace("-", "").replace("_", "").isalnum():
        return _json_err(400, "name: letters/digits/-/_ required")
    if name in cat.source_names():
        return _json_err(409, f"source {name!r} already exists")
    entry = {"name": name, "kind": kind,
             "poll_seconds": int(body.get("poll_seconds") or 300)}
    if kind == "github":
        repo = str(body.get("repo") or "").strip()
        if repo.count("/") != 1:
            return _json_err(400, "repo must be owner/name")
        entry["repo"] = repo
        if body.get("pat_env"):
            entry["pat_env"] = str(body["pat_env"]).strip()
    elif kind == "local":
        root = str(body.get("root") or "").strip()
        if not root:
            return _json_err(400, "local source needs root")
        entry["root"] = root
    else:
        return _json_err(400, f"unknown kind {kind!r} (github|local)")
    try:
        _rw_sources_yaml(cat, lambda srcs: srcs + [entry])
        cat.reload()
        synced = await cat.sync_one(name)
    except Exception as e:                            # noqa: BLE001
        return _json_err(502, f"source added but first sync failed: {e}")
    await cat.start_polling()
    return web.json_response({"ok": True, "name": name, "versions_found": synced})


async def delete_firmware_source(request: web.Request) -> web.Response:
    """Remove a source from sources.yaml.  Catalog rows and cached images
    it already produced are NOT deleted — history stays, discovery stops."""
    cat = _catalog(request)
    if cat is None:
        return _json_err(503, "firmware_catalog is not configured")
    name = request.match_info["name"]
    if name not in cat.source_names():
        return _json_err(404, f"no source named {name!r}")
    try:
        _rw_sources_yaml(cat, lambda srcs:
                         [s for s in srcs if s.get("name") != name])
        cat.reload()
    except Exception as e:                            # noqa: BLE001
        return _json_err(502, f"remove failed: {e}")
    await cat.start_polling()
    return web.json_response({"ok": True, "removed": name})


async def post_firmware_source_sync(request: web.Request) -> web.Response:
    """Force-trigger a discovery sync on one source."""
    cat = _catalog(request)
    if cat is None:
        return _json_err(503, "firmware_catalog is not configured")
    name = request.match_info["name"]
    try:
        count = await cat.sync_one(name)
    except KeyError:
        return _json_err(404, f"no source named {name!r}")
    except Exception as e:  # pragma: no cover
        return _json_err(502, f"sync failed: {e}")
    return web.json_response({"source": name, "discovered": count})

async def list_firmware_catalog(request: web.Request) -> web.Response:
    """List all catalog entries (optionally filtered by device_type)."""
    db: DB = request.app["db"]
    device_type = request.query.get("device_type")
    entries = db.list_catalog(device_type)
    out = [_catalog_entry_dict(e) for e in entries]
    # flashable = the release ships a flash.sh (Factory → Flash a device, over
    # serial), OR it is a whole-disk card image (format sdcard.img.xz) which
    # the hub writes itself to a removable micro-SD.  Linux app bundles
    # (byai_camera tar.gz) deliberately are neither — they are applied by the
    # camera's own agent.
    for e in out:
        uri = str(e.get("asset_uri") or "")
        if e.get("format") == _SD_FORMAT:
            e["flashable"] = True
            e["flash_target"] = "sdcard"
        else:
            e["flashable"] = bool(
                uri and not uri.startswith(("http://", "https://"))
                and (Path(uri).parent / "flash.sh").is_file())
            e["flash_target"] = "serial"
    return web.json_response(out)

async def delete_firmware_catalog_entry(request: web.Request) -> web.Response:
    """Retire an image from the catalog (all sources) and drop its cached copy.
    Refuses the device_type's ACTIVE OTA target unless ?force=1 — retiring the
    image the fleet is being upgraded to is almost never what you mean.  Used
    when a published image turns out to be bad (e.g. a card image the ROM will
    not boot) so the Factory can no longer offer it."""
    db: DB = request.app["db"]
    device_type = request.match_info["device_type"]
    version = request.match_info["version"]
    force = request.query.get("force") in ("1", "true", "yes")
    rows = [e for e in db.list_catalog(device_type) if e.version == version]
    if not rows:
        return _json_err(404, f"{device_type} {version} is not in the catalog")
    tgt = db.get_firmware_target(device_type)
    if tgt is not None and tgt.target_version == version and not force:
        return _json_err(409, f"{device_type} {version} is the active OTA target — "
                              "promote another version first, or pass ?force=1")
    removed_files = []
    for e in rows:
        lp = e.local_path
        if lp and os.path.isfile(lp):
            try:
                os.remove(lp); removed_files.append(lp)
            except OSError as ex:
                return _json_err(500, f"could not remove cached image {lp}: {ex}")
    n = db.delete_catalog_entry(device_type, version)
    return web.json_response({"device_type": device_type, "version": version,
                              "rows_removed": n, "cache_removed": removed_files,
                              "was_active_target": bool(tgt and tgt.target_version == version)})


async def post_firmware_promote(request: web.Request) -> web.Response:
    """Promote a catalog entry to the active firmware_targets row.
    Downloads + verifies the image first if it isn't cached locally yet."""
    cat = _catalog(request)
    if cat is None:
        return _json_err(503, "firmware_catalog is not configured")
    device_type = request.match_info["device_type"]
    version = request.match_info["version"]
    try:
        body = await request.json()
    except Exception:
        body = {}
    source_name = body.get("source") if isinstance(body, dict) else None
    try:
        result = await cat.promote(device_type, version, source_name)
    except ValueError as e:
        return _json_err(404, str(e))
    except Exception as e:  # pragma: no cover
        return _json_err(502, f"promote failed: {e}")
    return web.json_response(result, status=200)

async def get_ota_fleet(request: web.Request) -> web.Response:
    """Per-device OTA readiness for a device_type.  The operator watches
    this until every device reports state=armed, then POSTs /apply."""
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    if router is None:
        return _json_err(503, "proto_router not running")
    device_type = request.query.get("device_type")
    if not device_type:
        return _json_err(400, "device_type query parameter required")

    target = db.get_firmware_target(device_type)
    devices = [d for d in db.list_devices() if d.type == device_type]
    if not devices:
        # devices.type may be 'end_device' on some installs — fall back
        # to provision_info.device_type.
        devices = [d for d in db.list_devices()
                   if (pi := db.get_provision_info(d.id)) is not None
                   and pi.device_type == device_type]

    out = []
    n_armed = 0
    for d in devices:
        snap = router.get_ota_status(d.id)
        if snap.get("state") == "armed":
            n_armed += 1
        out.append({
            "device_id":       d.id,
            "device_name":     d.name,
            "last_seen":       d.last_seen,
            **{k: snap.get(k) for k in (
                "state", "running_version", "armed", "armed_version",
                "progress_percent")},
        })
    return web.json_response({
        "device_type":    device_type,
        "target_version": target.target_version if target else None,
        "total":          len(out),
        "armed":          n_armed,
        "all_armed":      bool(out) and n_armed == len(out),
        "devices":        out,
    })

async def get_fleet(request: web.Request) -> web.Response:
    """Cached fleet view: what every device was last known to run, from
    hub-persisted state only — camera bundle reports and the KV copy of
    each sensor's self-reported version.  Answers instantly; freshness
    comes from /fleet/ws, which streams changes and re-asks stale
    sensors in the background."""
    db: DB = request.app["db"]
    from . import cameras as cams_mod
    targets = {t.device_type: t.target_version
               for t in db.list_firmware_targets()}
    rows = []
    for c in cams_mod.load_cameras(db):
        raw = db.get_setting(f"camera_bundle:{c.id}")
        try:
            b = json.loads(raw) if raw else {}
        except Exception:
            b = {}
        rows.append({"name": c.name or c.id, "kind": "camera",
                     "key": b.get("device_type", ""),
                     "running": b.get("version", ""),
                     "seen_at": b.get("reported_at")})
    for d in db.list_devices():
        at = db.get_setting(f"device_running_at:{d.id}")
        rows.append({"name": d.name or d.id, "kind": "sensor",
                     "key": _ota_image_key(db, d),
                     "running": db.get_setting(f"device_running:{d.id}") or "",
                     "seen_at": int(at) if at else None})
    return web.json_response({"targets": targets, "rows": rows})


_FLEET_REFRESH_LOCK: asyncio.Lock | None = None


async def _fleet_refresh_stale(app, max_age_s: int = 120) -> None:
    """Re-ask sensors whose cached version is older than max_age_s.
    STRICTLY serialized fleet-wide (one lock, one device at a time):
    concurrent sealed INFO refreshes starve each other on the mesh.
    Results are not collected here — the router's INFO_REPLY consumer
    persists them and wakes every /fleet/ws subscriber."""
    global _FLEET_REFRESH_LOCK
    if _FLEET_REFRESH_LOCK is None:
        _FLEET_REFRESH_LOCK = asyncio.Lock()
    if _FLEET_REFRESH_LOCK.locked():
        return                                   # a sweep is already running
    async with _FLEET_REFRESH_LOCK:
        db, router = app["db"], app.get("proto_router")
        if router is None:
            return
        import time as _t
        now = int(_t.time())
        for d in db.list_devices():
            at = int(db.get_setting(f"device_running_at:{d.id}") or 0)
            if now - at < max_age_s:
                continue
            try:
                await router.request_h2d(
                    d.id, req_cmd=proto.Cmd.INFO_QUERY, body=b"",
                    expected_reply_cmd=proto.Cmd.INFO_REPLY,
                    timeout=10.0, max_attempts=2)
            except Exception:
                pass                             # offline device: keep cache


async def ws_fleet(request: web.Request) -> web.WebSocketResponse:
    """Push {name, running, at} whenever a device's reported firmware
    version changes.  Connecting also triggers one background sweep that
    re-asks stale sensors, so an open Fleet tab converges on live truth
    without the page ever blocking on the mesh."""
    if not _check_ws_ticket(request):
        return web.Response(status=401, text="ws ticket required")
    router = request.app.get("proto_router")
    ws = web.WebSocketResponse(heartbeat=30.0)
    await ws.prepare(request)
    if router is None:
        await ws.send_json({"error": "proto_router not running"})
        await ws.close()
        return ws
    db: DB = request.app["db"]
    q = router.subscribe_fleet()
    asyncio.ensure_future(_fleet_refresh_stale(request.app))

    async def pump():
        import time as _t
        while True:
            ev = await q.get()
            d = db.get_device(ev["device_id"])
            await ws.send_json({"name": (d.name if d else None) or ev["device_id"],
                                "running": ev["running"],
                                "at": int(_t.time())})

    pump_task = asyncio.ensure_future(pump())
    try:
        async for _ in ws:                       # drain until client closes
            pass
    finally:
        pump_task.cancel()
        router.unsubscribe_fleet(q)
    return ws


def _ota_image_key(db, d) -> str:
    """Catalog key for a device: its self-reported image name (firmware
    "img" field, stored by proto_router) with device_type as fallback
    for firmware that predates image identity."""
    return db.get_setting(f"device_image:{d.id}") or d.type

async def _ota_apply_device(router, d) -> dict:
    """Send OTA_APPLY to one ARMED device (shared by the per-device apply
    endpoint and the schedule runner).  Mirrors the fleet-apply logic."""
    snap = router.get_ota_status(d.id)
    if snap.get("state") != "armed":
        return {"device_id": d.id, "device_name": d.name,
                "applied": False, "reason": f"not armed ({snap.get('state')})"}
    router.note_apply_sent(d.id)
    try:
        reply = await router.request_h2d(
            d.id, req_cmd=proto.Cmd.OTA_APPLY, body=b"",
            expected_reply_cmd=proto.Cmd.OTA_HINT_ACK,
            timeout=8.0, max_attempts=3)
        status = reply[0] if reply else 255
        return {"device_id": d.id, "device_name": d.name,
                "applied": status == 0, "ack_status": status}
    except Exception as e:
        return {"device_id": d.id, "device_name": d.name,
                "applied": False, "ack_lost": True,
                "reason": (str(e) or type(e).__name__)
                          + " — device may still apply; watchdog will confirm"}

async def post_device_ota_download(request: web.Request) -> web.Response:
    """Download (but do not apply) a chosen firmware version to the
    device's persist slot: promote *version* to the active target for
    the device's type, then OTA_HINT the device — it downloads, arms,
    and waits for OTA_APPLY."""
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    cat = _catalog(request)
    if router is None or cat is None:
        return _json_err(503, "router/catalog not available")
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    try:
        body = await request.json()
        version = str(body["version"])
    except Exception:
        return _json_err(400, "expected JSON {'version': '...'}")
    try:
        await cat.promote(_ota_image_key(db, d), version, None)
    except ValueError as e:
        return _json_err(404, str(e))
    except Exception as e:
        return _json_err(502, f"promote failed: {e}")
    try:
        reply = await router.request_h2d(
            d.id, req_cmd=proto.Cmd.OTA_HINT, body=b"",
            expected_reply_cmd=proto.Cmd.OTA_HINT_ACK,
            timeout=8.0, max_attempts=3)
        status = reply[0] if reply else 255
    except Exception as e:
        return _json_err(504, f"device did not ack OTA hint: {e}")
    return web.json_response({"device_id": d.id, "version": version,
                              "ack_status": status})

async def post_device_ota_apply(request: web.Request) -> web.Response:
    """Apply the armed firmware on one device now."""
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    if router is None:
        return _json_err(503, "proto_router not available")
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    return web.json_response(await _ota_apply_device(router, d))

def _load_ota_schedules(db) -> dict:
    raw = db.get_setting("ota_schedules")
    try:
        return json.loads(raw) if raw else {}
    except Exception:
        return {}

async def get_device_ota_schedule(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    return web.json_response(_load_ota_schedules(db).get(d.id) or {})

async def put_device_ota_schedule(request: web.Request) -> web.Response:
    """Schedule an OTA apply: {"version": "...", "at": <unix seconds>}.
    A hub-side runner arms the device beforehand if needed and sends
    OTA_APPLY once *at* passes and the right version is armed."""
    db: DB = request.app["db"]
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    try:
        body = await request.json()
        version = str(body["version"])
        at = int(body["at"])
    except Exception:
        return _json_err(400, "expected JSON {'version': str, 'at': unix}")
    sched = _load_ota_schedules(db)
    sched[d.id] = {"version": version, "at": at}
    db.set_setting("ota_schedules", json.dumps(sched))
    return web.json_response(sched[d.id])

async def delete_device_ota_schedule(request: web.Request) -> web.Response:
    db: DB = request.app["db"]
    d = _resolve_device(db, request.match_info["dev"])
    if not d:
        return _json_err(404, "device not found")
    sched = _load_ota_schedules(db)
    sched.pop(d.id, None)
    db.set_setting("ota_schedules", json.dumps(sched))
    return web.json_response({})

async def post_ota_fleet_apply(request: web.Request) -> web.Response:
    """Operator-confirmed apply: send OTA_APPLY to every armed device of
    the given device_type.  Devices ACK then reboot ~500 ms later, so
    the fleet swaps near-simultaneously.  Returns per-device results."""
    db: DB = request.app["db"]
    router = request.app.get("proto_router")
    if router is None:
        return _json_err(503, "proto_router not running")
    try:
        body = await request.json()
    except Exception:
        return _json_err(400, "expected JSON body")
    device_type = body.get("device_type")
    if not device_type:
        return _json_err(400, "device_type required")

    devices = [d for d in db.list_devices() if d.type == device_type]
    if not devices:
        devices = [d for d in db.list_devices()
                   if (pi := db.get_provision_info(d.id)) is not None
                   and pi.device_type == device_type]

    import asyncio as _asyncio
    results = []

    async def _apply_one(d):
        snap = router.get_ota_status(d.id)
        if snap.get("state") != "armed":
            return {"device_id": d.id, "device_name": d.name,
                    "applied": False, "reason": f"not armed ({snap.get('state')})"}
        # Mark apply-sent BEFORE the request: even when the device's ACK
        # is lost on the mesh (it reboots ~500ms later regardless), the
        # applying-watchdog will poll with INFO_QUERY until the new
        # version confirms.
        router.note_apply_sent(d.id)
        try:
            reply = await router.request_h2d(
                d.id,
                req_cmd=proto.Cmd.OTA_APPLY,
                body=b"",
                expected_reply_cmd=proto.Cmd.OTA_HINT_ACK,
                timeout=8.0,
                max_attempts=3,
            )
            status = reply[0] if reply else 255
            return {"device_id": d.id, "device_name": d.name,
                    "applied": status == 0, "ack_status": status}
        except Exception as e:
            return {"device_id": d.id, "device_name": d.name,
                    "applied": False, "ack_lost": True,
                    "reason": (str(e) or type(e).__name__)
                              + " — device may still apply; watchdog will confirm"}

    results = await _asyncio.gather(*[_apply_one(d) for d in devices])
    applied = sum(1 for r in results if r.get("applied"))
    return web.json_response({
        "device_type": device_type,
        "applied":     applied,
        "total":       len(results),
        "devices":     list(results),
    })


def make_app(db: DB,
             log_store: log_store_mod.LogStore,
             enc_priv,
             auth_token: Optional[str] = None,
             proto_router=None,
             firmware_catalog=None,
             metrics=None) -> web.Application:
    app = web.Application(middlewares=[_bearer_auth, _static_cache_mw])
    app["started_at"] = time.time()     # camera status: "checking" for the first minute
    app["db"]           = db
    app["log_store"]    = log_store
    app["enc_priv"]     = enc_priv
    app["auth_token"]   = auth_token
    app["proto_router"] = proto_router  # Phase 6: relay-based field R/W
    app["firmware_catalog"] = firmware_catalog
    app["metrics"] = metrics                      # hourly camera metrics collector (phase 5), may be None
    swept = _sweep_stale_slot_devices(db)
    if swept:
        log.info("swept stale device bindings from unregistered slots: %s",
                 ", ".join(swept))
    from . import media_store as media_store_mod
    app["media_store"] = media_store_mod.MediaStore()

    r = app.router
    r.add_get ("/api/v1/healthz",                          healthz)
    r.add_get ("/api/v1/hub/identity",                     get_hub_identity)
    r.add_get ("/api/v1/devices",                          list_devices)
    r.add_post("/api/v1/devices",                          post_register_device)
    r.add_get ("/api/v1/devices/pending",                  list_pending_devices)
    r.add_get ("/api/v1/devices/{dev}",                    get_device)
    r.add_post("/api/v1/devices/{dev}/provision",          post_provision_device)
    r.add_get ("/api/v1/devices/{dev}/field/{field}",      get_field)
    r.add_put ("/api/v1/devices/{dev}/field/{field}",      put_field)
    r.add_get ("/api/v1/gateway/service",                  get_gateway_service)
    r.add_post("/api/v1/gateway/service/restart",          post_gateway_service_restart)
    r.add_get ("/api/v1/gateways",                         list_gateways)
    r.add_post("/api/v1/gateways",                         post_register_gateway)
    r.add_get ("/api/v1/gateways/{gw}",                    get_gateway)
    r.add_get ("/api/v1/gateways/{gw}/host",               get_gateway_host)
    r.add_post("/api/v1/debug/h2d-probe",                   post_debug_h2d_probe)
    r.add_get ("/api/v1/radio/health",                      get_radio_health)
    r.add_get ("/api/v1/devices/{dev}/radio",               get_device_radio)
    r.add_get ("/api/v1/radio/channel",                     get_radio_channel)
    r.add_post("/api/v1/radio/channel/scan",                post_radio_channel_scan)
    r.add_get ("/api/v1/radio/channel/scan/{job}",          get_radio_channel_scan)
    r.add_post("/api/v1/radio/channel/migrate",             post_radio_channel_migrate)
    r.add_get ("/api/v1/radio/channel/settings",            get_radio_channel_settings)
    r.add_put ("/api/v1/radio/channel/settings",            put_radio_channel_settings)
    r.add_get ("/api/v1/radio/channel/auto",                get_radio_channel_auto)
    r.add_delete("/api/v1/gateways/{gw}",                  delete_gateway)
    r.add_get ("/api/v1/network",                          get_network)
    r.add_post("/api/v1/network/init",                     post_network_init)
    r.add_get ("/api/v1/logs",                             list_logs)
    r.add_get ("/api/v1/log_files",                        list_log_files)
    r.add_get ("/api/v1/auto/yaml",                        get_auto_yaml)
    r.add_get ("/api/v1/auto/failsafe",                    get_auto_failsafe)
    r.add_put ("/api/v1/auto/failsafe",                    put_auto_failsafe)
    r.add_post("/api/v1/auto/compile",                     post_auto_compile)
    r.add_get ("/api/v1/auto/{dev}",                       get_auto_show)
    r.add_post("/api/v1/auto/{dev}/push",                  post_auto_push)
    r.add_post("/api/v1/devices/{dev}/ota",                post_device_ota)
    r.add_post("/api/v1/devices/{dev}/ota/download",       post_device_ota_download)
    r.add_post("/api/v1/devices/{dev}/ota/apply",          post_device_ota_apply)
    r.add_get ("/api/v1/devices/{dev}/ota/schedule",       get_device_ota_schedule)
    r.add_put ("/api/v1/devices/{dev}/ota/schedule",       put_device_ota_schedule)
    r.add_delete("/api/v1/devices/{dev}/ota/schedule",     delete_device_ota_schedule)
    r.add_get ("/api/v1/devices/{dev}/ota",                get_device_ota)
    r.add_post("/api/v1/devices/{dev}/info/refresh",       post_device_info_refresh)
    r.add_get ("/api/v1/devices/{dev}/uptime",             get_device_uptime)
    r.add_get ("/api/v1/devices/{dev}/automation-usage",   get_device_automation_usage)
    r.add_post("/api/v1/devices/{dev}/unregister",         post_device_unregister)
    r.add_get ("/api/v1/devices/{dev}/unregister/status",  get_device_unregister_status)
    r.add_get ("/api/v1/archive",                          get_archive)
    r.add_get ("/api/v1/archive/{dev}",                    get_archive_one)
    r.add_get ("/api/v1/archive/{dev}/events",             get_archive_events)
    r.add_delete("/api/v1/archive/{dev}",                  delete_archive_one)
    r.add_post("/api/v1/cameras/{cam}/unregister",         post_camera_unregister)
    r.add_post("/api/v1/cameras/{cam}/free",                post_camera_free)
    r.add_post("/api/v1/cameras/{cam}/adopt",              post_camera_adopt)
    r.add_get ("/api/v1/cameras-blocked",                  get_blocked_cameras)
    r.add_get ("/api/v1/settings/last-wifi-ssid",          get_last_wifi_ssid)
    r.add_get ("/api/v1/media-host",                       get_media_host)
    r.add_get ("/api/v1/metrics/current",                  get_metrics_current)
    r.add_get ("/api/v1/metrics/reports",                  get_metrics_reports)
    r.add_get ("/api/v1/metrics/reports/{id}",             get_metrics_report)
    r.add_post("/api/v1/metrics/rollup",                   post_metrics_rollup)
    r.add_get ("/api/v1/cameras/{cam}/pipelines",          get_camera_pipelines)
    r.add_get ("/api/v1/cameras/{cam}/pipeline-settings/{pipeline}", get_camera_pipeline_settings)
    r.add_put ("/api/v1/cameras/{cam}/pipeline-settings/{pipeline}", put_camera_pipeline_settings)
    r.add_post("/api/v1/cameras/{cam}/pipeline-reset/{pipeline}",    post_camera_pipeline_reset)
    r.add_get ("/api/v1/camera-slots",                     get_camera_slots)
    r.add_post("/api/v1/provision/scan",                   post_provision_scan)
    r.add_post("/api/v1/provision/jobs",                   post_provision_job)
    r.add_get ("/api/v1/provision/jobs/{id}",              get_provision_job)
    r.add_post("/api/v1/devices/{dev}/reboot",             post_device_reboot)
    r.add_get ("/api/v1/devices/{dev}/fields/cache",       get_device_fields_cache)
    r.add_get ("/api/v1/devices/{dev}/card-config",        get_device_card_config)
    r.add_put ("/api/v1/devices/{dev}/card-config",        put_device_card_config)
    r.add_get ("/api/v1/firmware",                         list_firmware_targets)
    r.add_get ("/api/v1/fleet",                            get_fleet)
    r.add_get ("/api/v1/fleet/ws",                         ws_fleet)
    r.add_post("/api/v1/firmware",                         post_firmware_upload)
    r.add_get ("/api/v1/cameras/{cam}/gateway",            get_camera_gateway)
    r.add_put ("/api/v1/cameras/{cam}/gateway",            put_camera_gateway)
    r.add_post("/api/v1/cameras/{cam}/gateway/report",     post_camera_gateway_report)
    r.add_get ("/api/v1/cameras/{cam}/gateway/config",     get_camera_gateway_config)
    r.add_post("/api/v1/gateways/provision-net",           post_gateway_provision_net)
    r.add_get ("/api/v1/factory/ports",                    get_factory_ports)
    r.add_post("/api/v1/factory/flash",                    post_factory_flash)
    r.add_get("/api/v1/factory/disks",                     get_factory_disks)
    r.add_post("/api/v1/factory/flash-disk",               post_factory_flash_disk)
    r.add_get ("/api/v1/factory/flash/{jid}",              get_factory_flash_job)
    r.add_get ("/api/v1/firmware/sources",                 list_firmware_sources)
    r.add_post("/api/v1/firmware/sources",                 post_firmware_source_add)
    r.add_delete("/api/v1/firmware/sources/{name}",        delete_firmware_source)
    r.add_post("/api/v1/firmware/sources/{name}/sync",     post_firmware_source_sync)
    r.add_get ("/api/v1/firmware/catalog",                 list_firmware_catalog)
    r.add_delete("/api/v1/firmware/catalog/{device_type}/{version}",
               delete_firmware_catalog_entry)
    r.add_post("/api/v1/firmware/catalog/{device_type}/{version}/promote",
                                                            post_firmware_promote)
    r.add_get ("/api/v1/ota/fleet",                        get_ota_fleet)
    r.add_post("/api/v1/ota/fleet/apply",                  post_ota_fleet_apply)
    r.add_post("/api/v1/gateways/new",                     post_gateway_new)
    r.add_post("/api/v1/devices/new",                      post_device_new)
    r.add_post("/api/v1/media/uploads",                    post_media_uploads)
    r.add_put ("/api/v1/media/upload/{token}",             put_media_upload)
    r.add_get ("/api/v1/media/events",                     list_media_events)
    r.add_post("/api/v1/media/events/delete",              delete_media_events)
    r.add_get ("/api/v1/media/file/{day}/{event}/{name}",  get_media_file)
    # ── /devices webapp (static) + single-origin camera proxies ─────────────
    r.add_get ("/devices",                                 devices_page)
    # legacy / redundant web UI entry points → the unified /devices webapp
    r.add_get ("/",                                        redirect_to_devices)
    r.add_get ("/media",                                   redirect_to_devices)
    r.add_get ("/api/v1/media/browse",                     redirect_to_devices)
    r.add_get ("/api/v1/cameras",                          list_cameras)
    r.add_post("/api/v1/cameras",                          register_camera)
    r.add_get ("/api/v1/settings/camera-log-cap",          get_camera_log_cap)
    r.add_get ("/api/v1/settings/device-log-cap",          get_device_log_cap)
    r.add_put ("/api/v1/settings/device-log-cap",          put_device_log_cap)
    r.add_put ("/api/v1/settings/camera-log-cap",          put_camera_log_cap)
    r.add_get ("/api/v1/cameras/{cam}/stream-policy",      get_camera_stream_policy)
    r.add_put ("/api/v1/cameras/{cam}/stream-policy",      put_camera_stream_policy)
    r.add_put ("/api/v1/cameras/by-addr/{addr}/bundle",    put_camera_bundle_by_addr)
    r.add_put ("/api/v1/cameras/{cam}/bundle",             put_camera_bundle)
    r.add_get ("/api/v1/cameras/{cam}/bundle",             get_camera_bundle)
    r.add_get ("/api/v1/cameras/{cam}/inference",          get_camera_inference)
    r.add_put ("/api/v1/cameras/{cam}/inference",          put_camera_inference)
    r.add_post("/api/v1/clientlog",                        post_client_log)
    r.add_get ("/api/v1/clientlog",                        get_client_log)
    r.add_get ("/api/v1/clientlog/debug",                  get_client_debug)
    r.add_get ("/api/v1/logs/sources",                     get_log_sources)
    r.add_get ("/api/v1/logs/tail",                        get_log_tail)
    r.add_get ("/api/v1/logs/ws",                          ws_log_tail)
    r.add_get ("/api/v1/ws-ticket",                        get_ws_ticket)
    r.add_get ("/api/v1/inferd/stats",                     get_inferd_stats)
    r.add_post("/api/v1/inferd/stats",                     post_inferd_stats)
    r.add_post("/api/v1/clientlog/debug",                  set_client_debug)
    r.add_delete("/api/v1/cameras/{cam}",                  unregister_camera)
    r.add_get ("/api/v1/cameras/{cam}/snapshot.jpg",       camera_snapshot)
    r.add_get ("/api/v1/cameras/{cam}/detections",         camera_detections)
    r.add_get ("/api/v1/cameras/{cam}/device",              camera_device_status)
    r.add_get ("/api/v1/cameras/{cam}/hls/{name}",         camera_hls)
    r.add_get ("/api/v1/cameras/{cam}/uptime",             get_camera_uptime)
    r.add_post("/api/v1/cameras/{cam}/reboot",             post_camera_reboot)
    r.add_post("/api/v1/cameras/{cam}/stream-report",      post_stream_report)
    r.add_get ("/api/v1/stream-reports",                   get_stream_reports)
    r.add_get ("/api/v1/cameras/{cam}/ws",                 camera_ws)
    r.add_get ("/api/v1/cameras/{cam}/events",             camera_events)
    r.add_static("/app/", _STATIC_DIR)   # devices.html assets: app.js/app.css/jmuxer
    _maybe_setup_video(app)   # live video → WebRTC (opt-in via NN_VIDEO_SERVICE_URL)
    return app


async def serve_api(db: DB,
                    log_store: log_store_mod.LogStore,
                    enc_priv,
                    host: str = "127.0.0.1",
                    port: int = 8769,
                    auth_token: Optional[str] = None,
                    proto_router=None,
                    firmware_catalog=None,
                    metrics=None) -> None:
    """Start the REST API server and block forever."""
    app = make_app(db, log_store, enc_priv, auth_token=auth_token,
                   proto_router=proto_router,
                   firmware_catalog=firmware_catalog, metrics=metrics)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    log.info("REST API listening on http://%s:%d/api/v1  (auth=%s)",
             host, port, "bearer" if auth_token else "off")
    await asyncio.Event().wait()
