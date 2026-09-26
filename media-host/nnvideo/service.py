#!/usr/bin/env python3
"""nn-video (pipelines) — one process for every camera.

    python3 -m nnvideo.service --store /var/lib/nn-media/nn-media.db \\
        --api-port 8899 --ingest-port 8886 [--ingest-alias 8888,8890,8892,8894] \\
        [--import-slots /etc/nn-media/slots.yaml] [--file-camera camX=/path.h264]

The API serves the framework routes (see api.py) plus every camera's legacy
control API under /cam/{id}/… (proxied to the camera's ephemeral loopback
app, so the routes stay byte-for-byte what the hub expects).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
import time
from pathlib import Path

from aiohttp import ClientSession, ClientTimeout, web

from .api import make_app as make_framework_app
from .camera import FileHandler, Ingress
from .pipeline import Engine
from .pipes import ControlPipeline, DetectPipeline, EventPipeline, HubPipeline, IngestPipeline, StreamPipeline
from .registry import Registry
from .request import Kind, Request
from .settings import Store

HERE = Path(__file__).resolve().parent.parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))


def import_slots(store: Store, path: Path) -> list[str]:
    """One-time migration from the supervisor's slots.yaml: camera rows,
    per-camera stream settings, and each slot's host key (imported under the
    camera's id) so already-provisioned cameras keep connecting."""
    import yaml
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    raw = yaml.safe_load(path.read_text()) or {}
    common = {k: str(v) for k, v in (raw.get("env") or {}).items()}
    done = []
    for s in raw.get("slots") or []:
        cid = str(s["id"])
        store.add_camera(cid, s.get("name") or cid, True)
        env = dict(common); env.update({k: str(v) for k, v in (s.get("env") or {}).items()})
        cfg = {"hls_dir": s.get("hls_dir") or f"/dev/shm/nn-hls-{cid}",
               "legacy_ingest_port": int(s["stream_port"]), "legacy_control_port": int(s["control_port"])}
        if s.get("snapshot_dir"):
            cfg["snapshot_dir"] = s["snapshot_dir"]
        for env_key, key in (("NN_HLS_REENCODE", "hls_reencode"), ("NN_HLS_WALLCLOCK", "hls_wallclock"),
                             ("NN_TRANSCODED", "transcoded"), ("NN_HLS_PDT_MAX_SKEW", "hls_pdt_max_skew"),
                             ("NN_HLS_GOP", "hls_gop"), ("NN_HLS_SEGTIME", "hls_segtime")):
            if env_key in env:
                cfg[key] = env[env_key]
        cfg["keydir"] = str(s["keydir"])
        args = [str(a) for a in (s.get("args") or [])]
        det_cfg, ev_cfg = {}, {}
        for flag, target, key, conv in (("--enc-qp", cfg, "enc_qp", int), ("--motion-decoder", cfg, "motion_decoder", str),
                                        ("--motion-interval-ms", det_cfg, "interval_ms", int),
                                        ("--motion-thresh", det_cfg, "diff_thresh", int),
                                        ("--yolo-url", ev_cfg, "yolo_url", str), ("--yolo-model", ev_cfg, "yolo_model", str),
                                        ("--event-motion-thresh", ev_cfg, "motion_thresh", float),
                                        ("--hub-url", ev_cfg, "hub_url", str)):
            if flag in args:
                target[key] = conv(args[args.index(flag) + 1])
        if "--no-adapt" in args:
            store.set_settings(cid, "control", {"adapt": False})
        if "--no-audio" in args:
            cfg["audio"] = False
        store.set_settings(cid, "stream", cfg)
        if det_cfg:
            store.set_settings(cid, "detect", det_cfg)
        if ev_cfg:
            store.set_settings(cid, "event", ev_cfg)
        kd = Path(str(s["keydir"])) / "hub_enc_key.raw"
        if kd.exists():
            raw_priv = kd.read_bytes()
            priv = X25519PrivateKey.from_private_bytes(raw_priv)
            store.set_host_key(cid, priv.public_key().public_bytes_raw(), raw_priv)
            store.set_setting(cid, "stream", "host_key_id", cid)
        done.append(cid)
    return done


def ensure_default_host_key(store: Store) -> bytes:
    """The one host key every NEW camera is provisioned with (imported
    legacy cameras keep their per-slot keys under their own id).  Generated
    once, kept in the store; returns the public key."""
    hk = store.host_key("default")
    if hk is None:
        from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
        priv = X25519PrivateKey.generate()
        store.set_host_key("default", priv.public_key().public_bytes_raw(), priv.private_bytes_raw())
        print("[nn-video] generated the default host key", flush=True)
        hk = store.host_key("default")
    return hk[0]


async def serve(a) -> None:
    store = Store(a.store)
    if a.import_slots:
        print(f"[nn-video] imported slots: {import_slots(store, Path(a.import_slots))}", flush=True)
    ensure_default_host_key(store)
    registry = Registry()
    engine = Engine(store, pool_workers=a.pool, stall_exit_s=a.stall_exit_s, malloc_trim_s=a.malloc_trim_s)
    stream = StreamPipeline(registry)
    detect, event = DetectPipeline(stream), EventPipeline(stream)
    api_url = f"http://127.0.0.1:{a.api_port}"
    for p in (IngestPipeline(), stream, detect, event, ControlPipeline(stream, registry),
              HubPipeline(api_url, stream, event)):
        engine.add(p)
    await engine.start()
    loop = asyncio.get_running_loop()

    def frame_sink(cam: str, frame) -> None:
        """Called on the GStreamer streaming thread by the decode branch for
        the frames the gate let through (one per detection interval); the
        Frame is views over the decoder's buffer, released downstream."""
        req = Request(cam, "detect", Kind.FRAME, frame, meta={"w": frame.w, "h": frame.h, "ts_ms": frame.ts_ms})
        asyncio.run_coroutine_threadsafe(engine.submit(req), loop)
    stream.frame_sink = frame_sink
    stream.frame_gate = detect.wants_frame          # asked BEFORE the frame is copied out of GStreamer

    # ONE GLib main loop for every camera's graph
    import threading
    from gi.repository import GLib
    glib_loop = GLib.MainLoop()
    threading.Thread(target=glib_loop.run, daemon=True, name="glib-main").start()

    # graphs for every enabled camera come up on first request; hub ticks
    # register them even before a camera connects (the hub shows the slot)
    ports = [a.ingest_port] + [int(p) for p in (a.ingest_alias.split(",") if a.ingest_alias else []) if p]
    store.set_settings("*", "stream", {"ingest_port": a.ingest_port}) \
        if engine.settings.get("*", "stream").get("ingest_port") != a.ingest_port else None
    ingress = Ingress(engine, store, registry, ports=ports)
    # legacy per-slot ingest ports from the imported settings are aliases too
    # (opt-in: while the supervisor still owns those ports they must stay free)
    if a.legacy_aliases:
        for c in store.cameras():
            lp = engine.settings.get(c["id"], "stream").get("legacy_ingest_port")
            if lp and int(lp) not in ports:
                ports.append(int(lp))
    if a.no_register:
        # side-by-side phase: never overwrite the hub's slot URLs; a camera
        # opts in with PUT /cameras/{id}/settings/hub {"register": true}
        store.set_settings("*", "hub", {"register": False})
    ingress.ports = ports
    ingress.start()

    files = []
    for spec in a.file_camera or []:
        cam, path = spec.split("=", 1)
        if not store.camera(cam):
            store.add_camera(cam, cam, True)
        fh = FileHandler(engine, registry, cam, path, fps=a.file_fps)
        fh.start(); files.append(fh)

    # API: framework routes + /cam/{id}/{tail} → the camera's own app
    app = make_framework_app(engine, registry, provinfo={"ingest_port": a.ingest_port,
                                                          "ingest_aliases": [p for p in ports if p != a.ingest_port],
                                                          "api_url": api_url})
    http = ClientSession(timeout=ClientTimeout(total=60))

    async def cam_proxy(r: web.Request):
        cam, tail = r.match_info["id"], r.match_info.get("tail", "")
        cs = stream.streams.get(cam)
        if cs is None:
            if not store.camera(cam):
                return web.json_response({"err": "no such camera"}, status=404)
            cfg = engine.settings.get(cam, "stream")
            cs = await stream.ensure(cam, cfg)
        url = f"http://127.0.0.1:{cs.port}/{tail}"
        if r.query_string:
            url += "?" + r.query_string
        if r.headers.get("Upgrade", "").lower() == "websocket":
            return await ws_proxy(r, url)
        body = await r.read()
        headers = {k: v for k, v in r.headers.items() if k.lower() not in ("host", "content-length")}
        try:
            async with http.request(r.method, url, data=body, headers=headers) as up:
                data = await up.read()
                out_headers = {k: v for k, v in up.headers.items()
                               if k.lower() in ("content-type", "cache-control")}
                return web.Response(status=up.status, body=data, headers=out_headers)
        except Exception as e:                                   # noqa: BLE001
            return web.json_response({"err": f"camera app unreachable: {e}"}, status=502)

    async def ws_proxy(r: web.Request, url: str):
        """Both directions pumped between the browser/hub and the camera's
        own /ws (or /diag/isp) app."""
        import aiohttp
        down = web.WebSocketResponse(max_msg_size=0)
        await down.prepare(r)
        try:
            async with http.ws_connect(url, max_msg_size=0) as up:
                async def pump(src, dst):
                    async for m in src:
                        if m.type == aiohttp.WSMsgType.BINARY:
                            await dst.send_bytes(m.data)
                        elif m.type == aiohttp.WSMsgType.TEXT:
                            await dst.send_str(m.data)
                        else:
                            break
                    await dst.close()
                await asyncio.gather(pump(down, up), pump(up, down), return_exceptions=True)
        except Exception as e:                                   # noqa: BLE001
            print(f"[nn-video] ws proxy {url}: {e}", flush=True)
        return down

    app.router.add_route("*", "/cam/{id}", cam_proxy)
    app.router.add_route("*", "/cam/{id}/{tail:.*}", cam_proxy)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", a.api_port).start()
    print(f"[nn-video] API on :{a.api_port}; cameras {[c['id'] for c in store.cameras()]}; "
          f"ingest ports {ports}", flush=True)

    async def ticker():
        while True:
            for c in store.cameras(enabled_only=True):
                await engine.submit(Request(c["id"], "hub", Kind.TICK))
                if registry.get(c["id"]):
                    await engine.submit(Request(c["id"], "control", Kind.TICK))
            await asyncio.sleep(a.tick_s)
    tick_task = asyncio.create_task(ticker())

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    print("[nn-video] stopping", flush=True)
    tick_task.cancel()
    for fh in files:
        fh.stop()
    ingress.stop()
    await engine.stop()
    for cs in list(stream.streams.values()):
        await cs.close()
    await http.close()
    await runner.cleanup()


def main() -> None:
    ap = argparse.ArgumentParser(description="nn-video pipelines: one process, every camera")
    ap.add_argument("--store", default="/var/lib/nn-media/nn-media.db")
    ap.add_argument("--api-port", type=int, default=8899)
    ap.add_argument("--ingest-port", type=int, default=8886)
    ap.add_argument("--ingest-alias", default="", help="comma-separated extra ingest ports (legacy slots)")
    ap.add_argument("--import-slots", default=None, help="one-time migration from slots.yaml")
    ap.add_argument("--legacy-aliases", action="store_true",
                    help="also listen on every imported slot's legacy ingest port")
    ap.add_argument("--no-register", action="store_true",
                    help="side-by-side: do not register cameras with the hub unless a camera's "
                         "hub.register setting says so")
    ap.add_argument("--file-camera", action="append", help="camX=/path/to/file.h264 (test source)")
    ap.add_argument("--file-fps", type=float, default=30.0)
    ap.add_argument("--tick-s", type=float, default=10.0)
    ap.add_argument("--pool", type=int, default=4)
    ap.add_argument("--stall-exit-s", type=float, default=120.0)
    ap.add_argument("--malloc-trim-s", type=float, default=60.0,
                    help="return freed heap to the OS this often (0 = never)")
    a = ap.parse_args()
    asyncio.run(serve(a))


if __name__ == "__main__":
    main()
