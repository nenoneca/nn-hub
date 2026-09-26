#!/usr/bin/env python3
"""
Media OTA service — REST front-end for the camera (ESP32-P4 + ESP32-C6) OTA.

Wraps the split download/swap flow (media_ota_push.run_ota) behind a one-shot
REST trigger so the hub can update the camera with a single API call.  On a
trigger it briefly owns the encrypted control channel the C6 dials into
(ctrl-port, default 8770), stages both images, verifies, arms, applies
(P4-first then C6), and reports the result.

  POST /api/v1/media/{cam}/ota   run stage+verify+arm+apply; returns
                                 {"result":"ok","c6":"0.1.2","p4":"0.1.2", ...}
  GET  /api/v1/media/{cam}/ota   {"busy": bool, "last": <last result>}

The target firmware bundle is configured at startup.  nn-hub proxies
POST /api/v1/cameras/{cam}/ota here (see _maybe_setup_media_ota in api.py).

  media_ota_service.py --p4-bin <p4.bin> --p4-ver 0.1.2 \
                       --c6-bin <c6.bin> --c6-ver 0.1.2 \
                       [--ctrl-port 8770] [--http-port 8771]
"""
import argparse
import asyncio
import sys
from pathlib import Path

from aiohttp import web

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, "/home/chalos/ext/mx500/nn_project_nowest/hub")

from media_ota_push import run_ota
from hub import crypto


async def post_ota(request: web.Request) -> web.Response:
    cam = request.match_info.get("cam", "media-cam-1")
    cfg = request.app["cfg"]
    if request.app["busy"]:
        return web.json_response({"error": "an OTA is already running"}, status=409)
    request.app["busy"] = True
    lines = []
    def log(m):
        lines.append(str(m)); print(m, flush=True)
    try:
        loop = asyncio.get_event_loop()
        res = await loop.run_in_executor(None, lambda: run_ota(
            cfg["p4"], cfg["c6"], cfg["p4_ver"], cfg["c6_ver"], cfg["priv"],
            port=cfg["port"], apply=True, log=log))
        request.app["last"] = res
        code = 200 if res["result"] in ("ok", "armed") else 502
        return web.json_response({"cam": cam, **res, "log": lines[-14:]}, status=code)
    finally:
        request.app["busy"] = False


async def get_ota(request: web.Request) -> web.Response:
    return web.json_response({"busy": request.app["busy"], "last": request.app.get("last")})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--p4-bin", required=True)
    ap.add_argument("--p4-ver", required=True)
    ap.add_argument("--c6-bin", required=True)
    ap.add_argument("--c6-ver", required=True)
    ap.add_argument("--ctrl-port", type=int, default=8770)
    ap.add_argument("--http-port", type=int, default=8771)
    ap.add_argument("--keydir", default="/tmp/ble_hub_data")
    a = ap.parse_args()

    app = web.Application()
    app["cfg"] = {
        "p4": Path(a.p4_bin).read_bytes(), "c6": Path(a.c6_bin).read_bytes(),
        "p4_ver": a.p4_ver, "c6_ver": a.c6_ver, "port": a.ctrl_port,
        "priv": crypto.load_or_generate_enc_key(Path(a.keydir)),
    }
    app["busy"] = False
    app["last"] = None
    app.router.add_post("/api/v1/media/{cam}/ota", post_ota)
    app.router.add_get("/api/v1/media/{cam}/ota", get_ota)
    print(f">> media OTA service: REST :{a.http_port}  control :{a.ctrl_port}  "
          f"target p4={a.p4_ver} c6={a.c6_ver}", flush=True)
    web.run_app(app, host="0.0.0.0", port=a.http_port, print=None)


if __name__ == "__main__":
    main()
