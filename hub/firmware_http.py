"""HTTP file server for gateway-side OTA pulls.

Serves a registered firmware target as raw bytes at:

    GET /gw_firmware/<device_type>/<version>

The endpoint is open (no auth at the HTTP layer) but the bytes are
MCUboot-signed: the gateway's MCUboot will reject any image that
doesn't match the project's signing key, so the worst an attacker can
do here is enumerate existing versions.

Run alongside the rest of `nn-hub serve`; the loop is registered into
the gather() list in main.py.

Phase 1 ships plain HTTP on port 8770.  TLS can be layered later by
swapping the runner for a TLSContext-aware one (out of scope here —
the hub<->gateway channel is on the LAN and signed payloads carry
their own integrity).
"""

from __future__ import annotations

import logging
from pathlib import Path

from aiohttp import web

from .db import DB

log = logging.getLogger("hub.firmware_http")


def _resolve_target(db: DB, device_type: str, version: str):
    target = db.get_firmware_target(device_type)
    if not target:
        raise web.HTTPNotFound(reason=f"no target for type={device_type}")
    if target.target_version != version:
        raise web.HTTPNotFound(
            reason=f"have {target.target_version}, requested {version}")
    path = Path(target.firmware_path)
    if not path.is_file():
        raise web.HTTPNotFound(reason=f"file missing: {path}")
    return target, path


_META_EXTRA_KEYS = ("format", "raw_size_bytes", "raw_sha256", "requires",
                    "kernel", "git", "boot_chain")


def _manifest_extras(db: DB, device_type: str, version: str) -> dict:
    """Manifest fields beyond type/version/size/sha256 for the promoted
    catalog entry, or {} when the target was uploaded without one."""
    import json
    try:
        rows = [e for e in db.list_catalog(device_type) if e.version == version]
        if not rows:
            return {}
        man = json.loads(rows[0].manifest_json or "{}")
    except Exception:                                  # noqa: BLE001
        return {}
    return {k: man[k] for k in _META_EXTRA_KEYS if k in man}


def make_app(db: DB) -> web.Application:
    app = web.Application()

    async def handle_meta(request: web.Request) -> web.Response:
        """JSON manifest the gateway fetches BEFORE the binary so it
        can sanity-check size + sha256 before erasing slot1.

        Format: { "type", "version", "size", "sha256" }.  All fields
        are also echoed in response headers on the binary GET so
        clients can skip the metadata round-trip if they want — but
        having a manifest endpoint lets future tooling (e.g. signed
        manifests with a release-rollout policy) layer in without
        touching the byte stream."""
        device_type = request.match_info["device_type"]
        version     = request.match_info["version"]
        target, _   = _resolve_target(db, device_type, version)
        meta = {
            "type":    target.device_type,
            "version": target.target_version,
            "size":    target.size_bytes,
            "sha256":  target.sha256,
        }
        # Linux agents need more than the byte digest before they write a
        # slot: the image format, the digest of the DECOMPRESSED stream
        # (checked while streaming through xz into the partition) and the
        # manifest's gates (requires.layout / boot_chain_min).  Those live
        # in the catalog entry's manifest, which the promote step copied
        # verbatim from the release, so serve them from there.
        meta.update(_manifest_extras(db, device_type, version))
        return web.json_response(meta)

    async def handle_get(request: web.Request) -> web.StreamResponse:
        device_type = request.match_info["device_type"]
        version     = request.match_info["version"]
        target, path = _resolve_target(db, device_type, version)

        log.info("[firmware_http] serving %s v%s (%d B) -> %s",
                 device_type, version, target.size_bytes,
                 request.remote)
        resp = web.StreamResponse(
            status=200,
            headers={
                "Content-Type":   "application/octet-stream",
                "Content-Length": str(target.size_bytes),
                "X-FW-Sha256":    target.sha256,
                "X-FW-Type":      target.device_type,
                "X-FW-Version":   target.target_version,
            },
        )
        await resp.prepare(request)
        with path.open("rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                await resp.write(chunk)
        await resp.write_eof()
        return resp

    app.router.add_get("/gw_firmware/{device_type}/{version}/meta", handle_meta)
    app.router.add_get("/gw_firmware/{device_type}/{version}",      handle_get)
    return app


async def serve_firmware_http(db: DB, host: str = "0.0.0.0",
                              port: int = 8770) -> None:
    """Start the HTTP firmware server and run forever."""
    app = make_app(db)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    log.info("firmware HTTP listening on %s:%d", host, port)
    # Block forever — caller wraps this in asyncio.gather()
    import asyncio
    await asyncio.Event().wait()
