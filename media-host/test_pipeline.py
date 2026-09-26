#!/usr/bin/env python3
"""Acceptance test for the nn media pipeline — run it after moving to a new machine.

Feeds a synthetic H.264 stream into video_service.py's --test-file ingest and asserts
that every stage downstream actually works: HW decode, HLS transcode + segment
production, snapshot decode, the motion→YOLO detection branch (CPU or NPU), and the
event pipeline.  Proves a port before you point a real camera at it.

Why it catches real bugs: the ingest deliberately pushes the bitstream in 4 KB chunks
(NOT access units), exactly like the live C6 feed.  On the CIX P1 a mis-declared
`alignment=au` silently stalled EVERY v4l2 decode branch — HLS produced no segments and
the detection branch never delivered a frame, with no error logged anywhere.  A
"does the service start?" check passes in that state; these checks do not.

The fixture always contains MOTION (the object is panned), because YOLO is motion-gated:
a static looping image would never trigger inference and the AI checks would be vacuous.

ISOLATION: video_service hardcodes its HLS dir to the script's own directory AND wipes it
at startup, so we run a throwaway copy in a temp dir on alternate ports.  A production
instance on the same box is never touched.

LIMITS — what this does NOT cover, so nobody reads a green run as more than it is:
  * The fixture is conformant x264.  The ESP32-P4's own bitstream is NOT conformant
    (it breaks software decoders outright), so P4-specific decoder quirks can only be
    caught with the real camera attached.  Verified: with `alignment=au` reintroduced,
    the detection branch and event mux fail here but HLS still passes, whereas on the
    real P4 stream HLS died too.
  * The sectun-encrypted ingest, device pairing, /webrtc, and A/V sync all need a real
    device; --test-file bypasses them.

Usage:
    python3 test_pipeline.py                                   # pipeline only
    python3 test_pipeline.py --model ~/models/yolox_s.cix      # + NPU inference
    python3 test_pipeline.py --model ~/models/yolox_s.onnx \\
                            --image bus.jpg --expect person,bus   # + AI accuracy

Exit code 0 = every check passed.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))


class Checks:
    """Minimal PASS/FAIL recorder — no pytest dependency, runs on a bare target."""

    def __init__(self):
        self.rows: list[tuple[str, bool, str]] = []

    def add(self, name: str, ok: bool, detail: str = "", fail_detail: str = "") -> bool:
        """detail is shown either way; fail_detail replaces it when the check fails."""
        shown = (detail if ok else (fail_detail or detail))
        self.rows.append((name, bool(ok), shown))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {shown}" if shown else ""),
              flush=True)
        return bool(ok)

    def info(self, name: str, detail: str) -> None:
        print(f"  [info] {name} — {detail}", flush=True)

    @property
    def failed(self) -> list[str]:
        return [n for n, ok, _ in self.rows if not ok]


def http_get(url: str, timeout: float = 15.0) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read()


def http_json(url: str, timeout: float = 15.0):
    return json.loads(http_get(url, timeout))


def http_post(url: str, timeout: float = 30.0):
    req = urllib.request.Request(url, data=b"", method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read() or b"{}")


def hub_reachable(url: str) -> bool:
    """Any HTTP answer — even a 404 — means something is listening."""
    try:
        urllib.request.urlopen(url, timeout=5)
        return True
    except urllib.error.HTTPError:
        return True
    except Exception:
        return False


def wait_for(fn, timeout: float, interval: float = 1.0):
    """Poll fn() until it returns something truthy or timeout expires."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            val = fn()
            if val:
                return val
        except Exception:
            pass
        time.sleep(interval)
    return None


# ── fixture ──────────────────────────────────────────────────────────────────
def build_fixture(path: str, image: str | None, w: int, h: int, secs: int, fps: int,
                  bitrate: str = "24M") -> None:
    """Encode a raw Annex-B H.264 test clip that always contains motion.

    With --image the photo is composited onto a canvas and panned, so YOLOX sees a
    real object AND the motion detector arms.  Without it, `testsrc` supplies a
    moving pattern (pipeline-only test — nothing for YOLOX to recognise).
    """
    if image:
        pan = "if(lt(mod(t,2),1), 40*mod(t,2), 40*(1-mod(t,2)))"   # sawtooth pan, 0..40 px
        cmd = ["ffmpeg", "-v", "error", "-y",
               "-loop", "1", "-t", str(secs), "-i", image,
               "-f", "lavfi", "-t", str(secs), "-i",
               f"color=c=gray:s={w}x{h}:r={fps}",
               "-filter_complex",
               f"[0:v]scale={w - 80}:{h - 80}:force_original_aspect_ratio=decrease[im];"
               f"[1:v][im]overlay=x='{pan}':y=40:shortest=1[v]",
               "-map", "[v]"]
    else:
        # Noise is not decoration: it makes the picture incompressible so access
        # units land in the 10s of KB, like the real P4 stream at ~24 Mbps.  A clean
        # testsrc encodes to ~3 KB/AU, which fits in ONE 4 KB fragment — and then
        # `alignment=au` is accidentally TRUE and the regression escapes the test
        # (verified: the alignment bug passed every check with a clean fixture).
        cmd = ["ffmpeg", "-v", "error", "-y",
               "-f", "lavfi", "-t", str(secs), "-i", f"testsrc=size={w}x{h}:rate={fps}",
               "-vf", "noise=alls=32:allf=t"]

    # Baseline + Annex-B: what the P4 emits and what the HW decoders expect.
    # sliced-threads=0 forces ONE slice per picture (zerolatency would otherwise
    # emit one slice per core), matching the P4 and keeping AU framing simple.
    # Cap the bitrate at roughly what the real camera sends (~24 Mbps at this
    # geometry).  WITHOUT a cap, the noise above encodes at default CRF and runs
    # away to ~250 Mbps -> ~1 MB per access unit and a 188 MB fixture, which
    # swamps the 4 KB-chunk ingest and the service never finishes starting.
    # The cap keeps AUs in the ~100 KB range: still many fragments each (which is
    # the property this fixture exists to exercise), but realistic.
    cmd += ["-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
            "-x264-params", "sliced-threads=0",
            "-b:v", f"{bitrate}", "-maxrate", f"{bitrate}", "-bufsize", "8M",
            "-pix_fmt", "yuv420p", "-profile:v", "baseline",
            "-g", str(fps), "-r", str(fps), "-bsf:v", "h264_mp4toannexb",
            "-f", "h264", path]
    subprocess.run(cmd, check=True, capture_output=True)
    if os.path.getsize(path) == 0:
        raise RuntimeError("encode produced an empty file")


# ── isolated service instance ────────────────────────────────────────────────
def start_service(work: str, fixture: str, a) -> subprocess.Popen:
    # copy the service + sibling modules so its hardcoded HLS_DIR lands in `work`
    for name in os.listdir(HERE):
        if name.endswith(".py"):
            shutil.copy2(os.path.join(HERE, name), os.path.join(work, name))
    keydir, snapdir = os.path.join(work, "keys"), os.path.join(work, "snapshots")
    os.makedirs(keydir, exist_ok=True)
    os.makedirs(snapdir, exist_ok=True)

    cmd = [sys.executable, os.path.join(work, "video_service.py"),
           "--test-file", fixture,
           "--control-port", str(a.ctrl_port), "--stream-port", str(a.stream_port),
           "--keydir", keydir, "--snapshot-dir", snapdir,
           "--hub-url", a.hub_url,
           "--no-audio",                       # no audio records in a file ingest
           "--no-adapt",                       # no sectun back-channel to adapt over
           "--yolo-url", "",                   # never the HTTP NPU server; local only
           "--yolo-model", a.model or "",      # default points at a beagle-specific path
           "--motion-decoder", a.motion_decoder,
           "--motion-interval-ms", "200",
           "--hls-port", str(a.hls_port),
           "--event-motion-thresh", str(a.event_thresh)]
    if a.enc_qp:
        cmd += ["--enc-qp", str(a.enc_qp)]

    env = dict(os.environ)
    env.setdefault("PYTHONPATH", os.path.expanduser("~/nn_project_nowest/hub"))
    log = open(os.path.join(work, "service.log"), "wb")
    return subprocess.Popen(cmd, cwd=work, stdout=log, stderr=subprocess.STDOUT,
                            env=env, start_new_session=True)


def stop_service(proc: subprocess.Popen) -> None:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(os.getpgid(proc.pid), sig)
            proc.wait(timeout=15)
            return
        except Exception:
            continue


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model", default=None,
                    help=".cix = NPU, .onnx = CPU onnxruntime. Omit to skip AI checks")
    ap.add_argument("--image", default=None,
                    help="photo to pan as the test stream (required for --expect)")
    ap.add_argument("--expect", default="",
                    help="comma-separated COCO classes the AI must find in --image")
    ap.add_argument("--width", type=int, default=1920, help="P4 live geometry is 1920x1296")
    ap.add_argument("--height", type=int, default=1296)
    ap.add_argument("--fps", type=int, default=30)
    ap.add_argument("--seconds", type=int, default=6, help="fixture length (it loops)")
    ap.add_argument("--enc-qp", type=int, default=0,
                    help="fixed QP for HW re-encode; needed on CIX P1 (we ship 36 there)")
    ap.add_argument("--event-thresh", type=float, default=0.005,
                    help="motion T for the test (below the 0.02 default so YOLO reliably arms)")
    ap.add_argument("--motion-decoder", choices=("auto", "v4l2", "pyav"), default="auto")
    ap.add_argument("--hub-url", default="http://127.0.0.1:8769",
                    help="hub for event upload; unreachable = uploads_failed (reported, not fatal)")
    ap.add_argument("--bitrate", default="24M",
                    help="fixture bitrate cap; ~matches the real camera. Uncapped "
                         "noise runs away to ~250 Mbps and swamps the ingest")
    ap.add_argument("--hls-port", type=int, default=15566,
                    help="HLS loopback port for the test instance; must not collide "
                         "with a live service (production default is 5566)")
    ap.add_argument("--ctrl-port", type=int, default=18899)
    ap.add_argument("--stream-port", type=int, default=18888)
    ap.add_argument("--timeout", type=float, default=90.0, help="per-check wait budget")
    ap.add_argument("--keep", action="store_true", help="keep the temp workdir for debugging")
    ap.add_argument("--attach", default=None,
                    help="do not start a service: run the checks against this camera base URL "
                         "(e.g. http://127.0.0.1:8880/cam/cam4 for the single-process nn-video); "
                         "the service must already be looping a fixture into that camera")
    ap.add_argument("--hls-dir", default=None,
                    help="with --attach: the camera's HLS segment dir to watch")
    ap.add_argument("--log", default=None,
                    help="with --attach: the service log to read the 'detector:' line from "
                         "(default: ask /api/event/status whether a detector is loaded)")
    a = ap.parse_args()

    if a.expect and not a.image:
        ap.error("--expect needs --image (nothing recognisable in the synthetic pattern)")

    c = Checks()
    work = tempfile.mkdtemp(prefix="nn-pipeline-test.")
    base = a.attach.rstrip("/") if a.attach else f"http://127.0.0.1:{a.ctrl_port}"
    proc = None
    print(f"== nn media pipeline acceptance test ==\n  workdir : {work}\n"
          f"  service : {base}\n  fixture : {a.width}x{a.height}@{a.fps} "
          f"{'image=' + os.path.basename(a.image) if a.image else 'synthetic'}\n"
          f"  model   : {a.model or '(none — pipeline checks only)'}\n", flush=True)

    try:
        for tool in ("ffmpeg", "ffprobe"):
            if not shutil.which(tool):
                c.add(f"{tool} available", False, "required to build/verify the fixture")
                return 1

        fixture = os.path.join(work, "fixture.h264")
        try:
            build_fixture(fixture, a.image, a.width, a.height, a.seconds, a.fps, a.bitrate)
            c.add("fixture encoded", True, f"{os.path.getsize(fixture):,} B")
        except subprocess.CalledProcessError as e:
            c.add("fixture encoded", False, "", e.stderr.decode(errors="replace")[-160:])
            return 1

        # The fixture must be heavy enough that access units span SEVERAL 4 KB
        # ingest fragments — that is the only condition under which a false
        # `alignment=au` breaks the decoders.  A light fixture makes every check
        # below pass even with the bug present, so guard the guard.
        mean_au = os.path.getsize(fixture) / max(1, a.seconds * a.fps)
        c.add("fixture AUs span multiple ingest fragments", mean_au > 2 * 4096,
              f"~{mean_au / 1024:.1f} KB/AU ≈ {mean_au / 4096:.1f} fragments",
              f"~{mean_au / 1024:.1f} KB/AU fits one fragment — raise the bitrate or "
              "the alignment regression cannot be detected")

        if a.attach:
            logfile = a.log or ""
            if not a.hls_dir:
                ap.error("--attach needs --hls-dir")
        else:
            proc = start_service(work, fixture, a)
            logfile = os.path.join(work, "service.log")
        if not c.add("service up (control API)",
                     bool(wait_for(lambda: http_json(f"{base}/api/event/live"), a.timeout)),
                     base if a.attach else f"port {a.ctrl_port}",
                     f"no response in {a.timeout:.0f}s — see {logfile or base}"):
            if logfile:
                print(open(logfile, errors="replace").read()[-2000:])
            return 1

        # ── transcode: the HLS branch is the strongest decoder-liveness proof.
        # It is decode → re-encode → ffmpeg segment, and needs zero AI.
        hls = a.hls_dir if a.attach else os.path.join(work, "hls_live")
        segs = wait_for(lambda: (lambda L: L if len(L) >= 2 else None)(
            sorted(f for f in os.listdir(hls) if f.endswith(".ts"))
            if os.path.isdir(hls) else []), a.timeout)
        c.add("HLS: HW decode + re-encode produces segments", bool(segs),
              f"{len(segs)} segments" if segs
              else "no .ts written — decode or re-encode branch is stalled")

        if segs:
            seg = os.path.join(hls, segs[-2])       # -2: the newest may still be growing
            pr = subprocess.run(
                ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                 "stream=codec_name,width,height", "-of", "default=nw=1:nk=1", seg],
                capture_output=True, text=True)
            # MPEG-TS makes ffprobe list the stream once per program; take the first
            got = pr.stdout.split()[:3]
            ok = pr.returncode == 0 and len(got) == 3 and got[0] == "h264"
            c.add("HLS: segment is decodable h264 at source geometry",
                  ok and (int(got[1]), int(got[2])) == (a.width, a.height),
                  " ".join(got) if got else pr.stderr.strip()[:120])
            c.info("HLS bitrate", f"~{os.path.getsize(seg) * 8 / 1e6:.0f} Mbps "
                                  f"(1 s segment, enc-qp={a.enc_qp or 'off'})")
            c.add("HLS: playlist written", os.path.exists(os.path.join(hls, "live.m3u8")))

        # ── snapshot: independent full-frame decode → JPEG
        snap = wait_for(lambda: (lambda b: b if b[:2] == b"\xff\xd8" else None)(
            http_get(f"{base}/snapshot.jpg?fresh=1", timeout=25)), a.timeout)
        c.add("snapshot decodes to a JPEG", bool(snap),
              f"{len(snap):,} B" if snap else "/snapshot.jpg returned no valid JPEG")

        # ── AI: detection branch delivers frames AND inference runs
        if a.model:
            log = open(logfile, errors="replace").read() if logfile else ""
            backend = ("NPU" if "detector: NPU" in log
                       else "CPU" if "detector: CPU" in log
                       else "nn-inferd" if "detector: nn-inferd" in log else None)
            if backend is None and a.attach and not logfile:
                # attached: the service reports whether its detector is loaded
                _st = http_json(f"{base}/api/event/status") or {}
                backend = "loaded" if _st.get("detector") else None
            c.add("AI: detector loaded", bool(backend),
                  f"backend={backend}" if backend
                  else "no 'detector:' line — model failed to load, see service.log")

            # yolo_runs > 0 proves BOTH that the detection branch decoded a frame and
            # that motion armed the detector — the exact pair the alignment bug broke.
            st = wait_for(lambda: (lambda d: d if d.get("yolo_runs") else None)(
                http_json(f"{base}/api/event/status")), a.timeout)
            c.add("AI: motion armed the detector and inference ran", bool(st),
                  f"yolo_runs={st['yolo_runs']} yolo_ms={st.get('yolo_ms')}" if st
                  else f"yolo_runs stayed 0 (motion T={a.event_thresh})")

            if st:
                live = http_json(f"{base}/api/event/live")
                c.add("AI: detection frames at source geometry",
                      (live.get("w"), live.get("h")) == (a.width, a.height),
                      f"{live.get('w')}x{live.get('h')} (expected {a.width}x{a.height})")
                c.info("AI latency", f"{st.get('yolo_ms')} ms/frame on {backend}")

            want = {w.strip() for w in a.expect.split(",") if w.strip()}
            if want:
                found: set[str] = set()

                def _poll():
                    for d in http_json(f"{base}/api/event/live").get("detections", []):
                        if d.get("cls"):
                            found.add(d["cls"])
                    return want <= found

                wait_for(_poll, a.timeout, interval=2.0)
                c.add("AI: finds the expected classes", want <= found,
                      f"want={sorted(want)} found={sorted(found) or 'nothing'}")

        # ── events: mp4+jsonl assembly from the ring, then upload to the hub.
        # A rising `events` only proves the trigger fired; uploads_ok proves the
        # AU reassembly and mp4 mux actually produced a file the hub accepted.
        try:
            before = http_json(f"{base}/api/event/status")
            http_post(f"{base}/api/event/test")
            after = wait_for(lambda: (lambda d: d if d.get("events", 0) >
                                      before.get("events", 0) else None)(
                http_json(f"{base}/api/event/status")), a.timeout)
            c.add("event pipeline assembles an event", bool(after),
                  f"events {before.get('events')} → {after['events']}" if after else "",
                  "event count never increased")

            if hub_reachable(a.hub_url):
                got = wait_for(lambda: (lambda d: d if d.get("uploads_ok", 0) >
                                        before.get("uploads_ok", 0) else None)(
                    http_json(f"{base}/api/event/status")), a.timeout)
                st = got or http_json(f"{base}/api/event/status")
                c.add("event mp4 muxed and uploaded to the hub", bool(got),
                      f"uploads_ok={st.get('uploads_ok')}",
                      f"uploads_ok={st.get('uploads_ok')} failed={st.get('uploads_failed')}"
                      " — check 'upload failed' in service.log")
            else:
                c.info("event upload", f"skipped — no hub at {a.hub_url}")
        except Exception as e:
            c.add("event pipeline assembles an event", False, "", repr(e)[:120])

    finally:
        if proc:
            stop_service(proc)
        if a.keep:
            print(f"\n  workdir kept: {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)

    print("\n== summary ==")
    if c.failed:
        print(f"  FAILED {len(c.failed)}/{len(c.rows)}: {', '.join(c.failed)}")
        return 1
    print(f"  all {len(c.rows)} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
