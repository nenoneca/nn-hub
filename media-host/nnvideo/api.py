"""The single control API.  Written for the hub, which manages everything:
it adds cameras, installs keys and edits settings here; operators never
touch the store directly.  Camera-scoped compatibility routes
(/cam/{id}/api/...) are mounted by the service in phase 1."""
from __future__ import annotations

import base64

from aiohttp import web

from .pipeline import Engine
from .registry import Registry
from .settings import GLOBAL

HERE_DIR = __import__("pathlib").Path(__file__).resolve().parent.parent


def make_app(engine: Engine, registry: Registry, provinfo: dict | None = None) -> web.Application:
    store = engine.store
    app = web.Application()
    provinfo = dict(provinfo or {})

    def err(code: int, msg: str) -> web.Response:
        return web.json_response({"err": msg}, status=code)

    async def health(_r):
        h = engine.health()
        h["connections"] = registry.all()
        return web.json_response(h)

    async def get_provinfo(_r):
        """What provisioning writes into a NEW camera: this host's single
        ingest port and the default host public key.  The hub's add-device
        wizard reads this instead of asking a per-slot worker; the camera's
        own public key comes back through PUT /cameras/{id}/key."""
        hk = store.host_key("default")
        if hk is None:
            return err(503, "no default host key yet")
        return web.json_response({**provinfo, "host_key_id": "default",
                                  "host_pub_hex": hk[0].hex(), "host_pub_b64": base64.b64encode(hk[0]).decode(),
                                  "cameras": [c["id"] for c in store.cameras()]})

    async def pipelines(_r):
        return web.json_response({n: {"mode": p.mode, "policy": p.policy, "budget_s": p.budget_s,
                                      "workers": len(engine.workers.get(n, [])),
                                      "defaults": engine.settings.get(GLOBAL, n)}
                                  for n, p in engine.pipelines.items()})

    async def set_pipeline(r):
        name = r.match_info["name"]
        if name not in engine.pipelines:
            return err(404, "no such pipeline")
        try:
            body = await r.json()
            n = int(body["workers"])
        except Exception:
            return err(400, "expected JSON {\"workers\": n}")
        n = await engine.set_workers(name, n)
        return web.json_response({"pipeline": name, "workers": n})

    async def cameras(_r):
        out = []
        for c in store.cameras():
            c["connection"] = registry.get(c["id"])
            c["runtime"] = store.runtime(c["id"])
            out.append(c)
        return web.json_response(out)

    async def add_camera(r):
        try:
            body = await r.json()
            cid = str(body["id"])
        except Exception:
            return err(400, "expected JSON {id, name?, enabled?, device_pub_b64?, settings?}")
        if store.camera(cid):
            return err(409, f"camera {cid} exists")
        cam = store.add_camera(cid, body.get("name", cid), bool(body.get("enabled", True)))
        if body.get("device_pub_b64"):
            store.set_device_key(cid, base64.b64decode(body["device_pub_b64"]),
                                 body.get("host_key_id", "default"))
        for pipe, values in (body.get("settings") or {}).items():
            if pipe in engine.pipelines and isinstance(values, dict):
                store.set_settings(cid, pipe, values)
        return web.json_response(cam, status=201)

    async def get_camera(r):
        cid = r.match_info["id"]
        cam = store.camera(cid)
        if not cam:
            return err(404, "no such camera")
        cam["connection"] = registry.get(cid)
        cam["runtime"] = store.runtime(cid)
        cam["settings"] = {p: engine.settings.get(cid, p) for p in engine.pipelines}
        return web.json_response(cam)

    async def del_camera(r):
        cid = r.match_info["id"]
        if not store.remove_camera(cid):
            return err(404, "no such camera")
        for name in engine.pipelines:
            await engine.queues[name].purge(cid)
        registry.forget(cid)
        return web.json_response({"ok": True, "removed": cid})

    async def set_key(r):
        cid = r.match_info["id"]
        if not store.camera(cid):
            return err(404, "no such camera")
        try:
            body = await r.json()
            pub = base64.b64decode(body["device_pub_b64"])
            if len(pub) != 32:
                return err(400, "device_pub_b64 must decode to 32 bytes")
        except Exception:
            return err(400, "expected JSON {device_pub_b64, host_key_id?}")
        store.set_device_key(cid, pub, body.get("host_key_id", "default"))
        return web.json_response({"ok": True, "camera": cid})

    async def get_settings(r):
        cid, pipe = r.match_info["id"], r.match_info["pipeline"]
        if pipe not in engine.pipelines:
            return err(404, "no such pipeline")
        if cid != GLOBAL and not store.camera(cid):
            return err(404, "no such camera")
        return web.json_response({"camera": cid, "pipeline": pipe,
                                  "effective": engine.settings.get(cid, pipe),
                                  "stored": store.settings_versioned(cid, pipe),
                                  "version": store.version})

    async def put_settings(r):
        cid, pipe = r.match_info["id"], r.match_info["pipeline"]
        if pipe not in engine.pipelines:
            return err(404, "no such pipeline")
        if cid != GLOBAL and not store.camera(cid):
            return err(404, "no such camera")
        try:
            body = await r.json()
            assert isinstance(body, dict)
        except Exception:
            return err(400, "expected a JSON object of settings")
        v = store.set_settings(cid, pipe, body)
        return web.json_response({"camera": cid, "pipeline": pipe, "version": v,
                                  "effective": engine.settings.get(cid, pipe)})

    async def reset_camera_pipeline(r):
        """Operator reset: rebuild this camera's state in one pipeline (its
        graph, detector, engine, session controller) without touching any
        other camera or restarting the process — the same path a graph
        error or a failure storm takes."""
        cid, pipe = r.match_info["id"], r.match_info["pipeline"]
        if pipe not in engine.pipelines:
            return err(404, "no such pipeline")
        if not store.camera(cid):
            return err(404, "no such camera")
        await engine.reset(pipe, cid, "operator request")
        return web.json_response({"ok": True, "camera": cid, "pipeline": pipe,
                                  "resets": engine.state(pipe, cid).resets})

    async def runtime(r):
        cid = r.match_info["id"]
        if not store.camera(cid):
            return err(404, "no such camera")
        return web.json_response({"camera": cid, "connection": registry.get(cid),
                                  "runtime": store.runtime(cid)})

    async def debug_objects(_r):
        """What Python itself is holding.  gc.get_objects() only sees
        container objects, so bytes and numpy arrays (the media payloads)
        are counted from tracemalloc when it is on (POST /debug/tracemalloc
        {"on": true}; costs CPU, off by default) — that separates 'Python
        holds it' from 'the allocator or GStreamer holds it' when the
        resident size is questioned."""
        import gc
        import collections
        import tracemalloc
        from .pipeline import rss_kb
        gc.collect()
        objs = gc.get_objects()
        out = {"rss_kb": rss_kb(), "gc_tracked_objects": len(objs),
               "top_types": collections.Counter(type(o).__name__ for o in objs).most_common(10),
               "threads": len(__import__("threading").enumerate()),
               "tracemalloc": tracemalloc.is_tracing()}
        if tracemalloc.is_tracing():
            cur, peak = tracemalloc.get_traced_memory()
            snap = tracemalloc.take_snapshot()
            out["python_allocated_mb"] = round(cur / 1048576, 1)
            out["python_peak_mb"] = round(peak / 1048576, 1)
            out["top_sites"] = [{"site": str(st.traceback[0]).replace(str(HERE_DIR), ""),
                                 "mb": round(st.size / 1048576, 2), "count": st.count}
                                for st in snap.statistics("lineno")[:15]]
        return web.json_response(out)

    async def debug_tracemalloc(r):
        """DO NOT use on a live service with cameras connected.  Tried on the
        OPi 2026-09-22 11:24 with three cameras: tracking every allocation of
        this workload took the process from 300 MB to 1.9 GB in three minutes
        and slowed it until 16,847 event requests were dropped (video kept
        flowing).  It stays for a bench with a file camera only, and refuses
        unless the caller also sends {"i_know": true}; frames are kept to 1."""
        import tracemalloc
        try:
            body = await r.json()
        except Exception:
            body = {}
        on = bool(body.get("on", True))
        if on and not tracemalloc.is_tracing():
            if not body.get("i_know"):
                return err(400, "tracemalloc on a live service drove RSS to 1.9 GB and dropped requests; "
                                "send {\"on\": true, \"i_know\": true} on a bench only")
            tracemalloc.start(1)
        elif not on and tracemalloc.is_tracing():
            tracemalloc.stop()
        return web.json_response({"tracing": tracemalloc.is_tracing()})

    async def debug_malloc_trim(_r):
        from .pipeline import malloc_trim, rss_kb
        before = rss_kb()
        rc = await engine.loop.run_in_executor(engine.pool, malloc_trim)
        return web.json_response({"rc": rc, "rss_before_kb": before, "rss_after_kb": rss_kb()})

    app.router.add_get("/health", health)
    app.router.add_get("/debug/objects", debug_objects)
    app.router.add_post("/debug/tracemalloc", debug_tracemalloc)
    app.router.add_post("/debug/malloc_trim", debug_malloc_trim)
    app.router.add_get("/provinfo", get_provinfo)
    app.router.add_get("/pipelines", pipelines)
    app.router.add_put("/pipelines/{name}", set_pipeline)
    app.router.add_get("/cameras", cameras)
    app.router.add_post("/cameras", add_camera)
    app.router.add_get("/cameras/{id}", get_camera)
    app.router.add_delete("/cameras/{id}", del_camera)
    app.router.add_put("/cameras/{id}/key", set_key)
    app.router.add_get("/cameras/{id}/settings/{pipeline}", get_settings)
    app.router.add_put("/cameras/{id}/settings/{pipeline}", put_settings)
    app.router.add_get("/cameras/{id}/runtime", runtime)
    app.router.add_post("/cameras/{id}/reset/{pipeline}", reset_camera_pipeline)
    return app
