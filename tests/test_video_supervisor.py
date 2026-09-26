"""nn-video supervisor (media-host/video_supervisor.py) against a fake worker.

The fake worker speaks the two things the supervisor relies on — the slot's
/status endpoint with a `fed` counter, and HLS segment files in hls_dir — and
misbehaves on command through a control file: freeze, hang the API, exit with
a code, or segfault itself.
"""
import asyncio
import json
import os
import signal
import sys
import textwrap
import time
from pathlib import Path

import pytest
import yaml
from aiohttp import ClientSession
from aiohttp.test_utils import TestClient, TestServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "media-host"))
import video_supervisor as vs  # noqa: E402

pytestmark = pytest.mark.asyncio

FAKE_WORKER = textwrap.dedent('''
    import asyncio, json, os, signal, sys, time
    from aiohttp import web
    args = sys.argv[1:]
    def opt(name, default=None):
        return args[args.index(name) + 1] if name in args else default
    port = int(opt("--control-port")); hls = opt("--hls-dir"); ctl = os.environ["FAKE_CTL"]
    os.makedirs(hls, exist_ok=True)
    fed = 0
    print("fake worker up cam=" + os.environ.get("NN_CAM_ID", "?"), flush=True)
    async def status(_r):
        c = json.load(open(ctl)) if os.path.exists(ctl) else {}
        if c.get("hang_api"):
            await asyncio.sleep(30)
        return web.json_response({"fed": fed})
    async def tick():
        global fed
        while True:
            c = json.load(open(ctl)) if os.path.exists(ctl) else {}
            if c.get("exit") is not None:
                print("fake worker exiting " + str(c["exit"]), flush=True); os._exit(int(c["exit"]))
            if c.get("segv"):
                os.kill(os.getpid(), signal.SIGSEGV)
            if not c.get("freeze"):
                fed += 1
            if not c.get("no_segments"):
                open(os.path.join(hls, "seg%06d.ts" % fed), "w").close()
            await asyncio.sleep(0.2)
    async def main():
        app = web.Application(); app.router.add_get("/health", status); app.router.add_get("/status", status)
        r = web.AppRunner(app); await r.setup(); await web.TCPSite(r, "127.0.0.1", port).start()
        await tick()
    asyncio.run(main())
''')


def _setup(tmp_path: Path, monkeypatch, slots: list[dict]):
    worker = tmp_path / "fake_worker.py"
    worker.write_text(FAKE_WORKER)
    monkeypatch.setattr(vs, "WORKER", str(worker))
    monkeypatch.setattr(vs, "BACKOFF_S", (0.2, 0.2, 0.2))
    ctl = tmp_path / "ctl.json"
    os.environ["FAKE_CTL"] = str(ctl)
    yml = tmp_path / "slots.yaml"
    yml.write_text(yaml.safe_dump({"hub_url": "http://127.0.0.1:1", "slots": slots}))
    sup = vs.Supervisor(yml, api_port=0, hub_url="http://127.0.0.1:1",
                        hang_grace_s=1.0, startup_grace_s=0.5,
                        status_fail_limit=2, check_interval_s=0.3)
    return sup, ctl


def _slot(tmp_path, sid="cam9", cport=18999, sport=18888):
    return {"id": sid, "name": "fake", "stream_port": sport, "control_port": cport,
            "keydir": str(tmp_path / "keys"), "hls_dir": str(tmp_path / f"hls-{sid}")}


async def _wait(cond, timeout=8.0, step=0.1):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        await asyncio.sleep(step)
    return False


async def _run(sup):
    for cfg in sup.load():
        sup.start_slot(cfg)
    hl = asyncio.create_task(sup.health_loop())
    return hl


async def _teardown(sup, hl):
    hl.cancel()
    await asyncio.gather(*(s.stop() for s in list(sup.slots.values())), return_exceptions=True)
    try:
        await hl
    except (asyncio.CancelledError, Exception):
        pass


async def test_slot_runs_and_logs_are_prefixed(tmp_path, monkeypatch, capsys):
    sup, ctl = _setup(tmp_path, monkeypatch, [_slot(tmp_path)])
    hl = await _run(sup)
    try:
        s = sup.slots["cam9"]
        assert await _wait(lambda: s.state == "running" and s.fed is not None and s.fed > 0)
        h = sup.health()
        assert h["summary"]["running"] == 1 and h["slots"][0]["pid"]
        out = capsys.readouterr().out
        assert "[cam9] fake worker up cam=cam9" in out
    finally:
        await _teardown(sup, hl)


async def test_crash_is_classified_and_restarted(tmp_path, monkeypatch):
    sup, ctl = _setup(tmp_path, monkeypatch, [_slot(tmp_path)])
    hl = await _run(sup)
    try:
        s = sup.slots["cam9"]
        assert await _wait(lambda: s.state == "running")
        pid1 = s.proc.pid
        ctl.write_text(json.dumps({"exit": 43}))          # uncaught thread exception
        assert await _wait(lambda: s.restarts >= 1 and s.crashes.get("thread-exception") == 1)
        ctl.write_text("{}")
        assert await _wait(lambda: s.state == "running" and s.proc and s.proc.pid != pid1)
        assert s.last_exit_name == "thread-exception"
    finally:
        await _teardown(sup, hl)


async def test_segfault_is_counted(tmp_path, monkeypatch):
    sup, ctl = _setup(tmp_path, monkeypatch, [_slot(tmp_path)])
    hl = await _run(sup)
    try:
        s = sup.slots["cam9"]
        assert await _wait(lambda: s.state == "running")
        ctl.write_text(json.dumps({"segv": True}))
        assert await _wait(lambda: s.crashes.get("segfault", 0) >= 1)
        ctl.write_text("{}")
        assert await _wait(lambda: s.state == "running")
    finally:
        await _teardown(sup, hl)


async def test_hung_pipeline_is_killed(tmp_path, monkeypatch):
    """Video keeps arriving (fed advances) but no new segment appears."""
    sup, ctl = _setup(tmp_path, monkeypatch, [_slot(tmp_path)])
    hl = await _run(sup)
    try:
        s = sup.slots["cam9"]
        assert await _wait(lambda: s.state == "running" and (s.fed or 0) > 2)
        ctl.write_text(json.dumps({"no_segments": True}))
        assert await _wait(lambda: s.restarts >= 1 and "HLS segment" in (s.last_error or ""), timeout=10)
        ctl.write_text("{}")
        assert await _wait(lambda: s.state == "running" and s.restarts >= 1)
    finally:
        await _teardown(sup, hl)


async def test_offline_camera_is_not_a_fault(tmp_path, monkeypatch):
    """fed frozen AND no segments = camera offline: no kill."""
    sup, ctl = _setup(tmp_path, monkeypatch, [_slot(tmp_path)])
    hl = await _run(sup)
    try:
        s = sup.slots["cam9"]
        assert await _wait(lambda: s.state == "running")
        ctl.write_text(json.dumps({"freeze": True, "no_segments": True}))
        await asyncio.sleep(2.5)
        assert s.restarts == 0 and s.state == "running"
    finally:
        await _teardown(sup, hl)


async def test_unreachable_api_is_killed(tmp_path, monkeypatch):
    sup, ctl = _setup(tmp_path, monkeypatch, [_slot(tmp_path)])
    hl = await _run(sup)
    try:
        s = sup.slots["cam9"]
        assert await _wait(lambda: s.state == "running")
        ctl.write_text(json.dumps({"hang_api": True}))
        assert await _wait(lambda: s.restarts >= 1 and "unreachable" in (s.last_error or ""), timeout=15)
    finally:
        await _teardown(sup, hl)


async def test_add_remove_slots_live_and_persist(tmp_path, monkeypatch):
    sup, ctl = _setup(tmp_path, monkeypatch, [_slot(tmp_path)])
    hl = await _run(sup)
    try:
        async with TestClient(TestServer(sup.make_app())) as cli:
            r = await cli.post("/slots", json=_slot(tmp_path, "cam8", 18998, 18887))
            assert r.status == 201
            assert await _wait(lambda: sup.slots["cam8"].state == "running")
            saved = yaml.safe_load(sup.slots_path.read_text())
            assert sorted(s["id"] for s in saved["slots"]) == ["cam8", "cam9"]
            r = await cli.post("/slots", json=_slot(tmp_path, "cam7", 18998, 18886))
            assert r.status == 409, "control port clash must be refused"
            r = await cli.delete("/slots/cam8")
            assert r.status == 200 and "cam8" not in sup.slots
            saved = yaml.safe_load(sup.slots_path.read_text())
            assert [s["id"] for s in saved["slots"]] == ["cam9"]
            h = await (await cli.get("/health")).json()
            assert h["service"] == "nn-video" and [x["id"] for x in h["slots"]] == ["cam9"]
    finally:
        await _teardown(sup, hl)


def test_command_line_matches_the_legacy_unit(tmp_path):
    """The worker must be invoked exactly like the old per-slot units, so
    ports, key dirs and URLs the hub knows keep working."""
    sup = vs.Supervisor(tmp_path / "s.yaml", 0, "http://127.0.0.1:8769")
    cfg = {"id": "cam3", "name": "BeagleY Wide NoIR", "stream_port": 8892, "control_port": 8902,
           "hls_port": 5568, "keydir": "/var/lib/nn-media/keys3", "hls_dir": "/dev/shm/nn-hls-cam3",
           "snapshot_dir": "/x/snapshots3", "env": {"NN_HLS_REENCODE": "0"},
           "args": ["--motion-interval-ms", "200", "--yolo-url", ""]}
    s = vs.Slot(cfg, sup)
    cmd = s.command()
    assert cmd[2:] == ["--stream-port", "8892", "--control-port", "8902", "--keydir",
                       "/var/lib/nn-media/keys3", "--hls-dir", "/dev/shm/nn-hls-cam3",
                       "--hls-port", "5568", "--snapshot-dir", "/x/snapshots3",
                       "--hub-url", "http://127.0.0.1:8769", "--motion-interval-ms", "200",
                       "--yolo-url", ""]
    env = s.environment()
    assert env["NN_CAM_ID"] == "cam3" and env["NN_CAM_NAME"] == "BeagleY Wide NoIR"
    assert env["NN_CAM_URL"] == "http://127.0.0.1:8902" and env["NN_HLS_REENCODE"] == "0"
    assert env["PYTHONFAULTHANDLER"] == "1"
