#!/usr/bin/env python3
"""Drive the hub webapp in headless Chrome: load a page, run steps, capture
screenshots, console errors and JS values.  For checking webapp changes the
way a user sees them (after deploying the static files to the hub).

    python3 tools/webcheck.py URL [--size 1100x1200] [--wait 4] \\
        [--step 'js:<expression>'] [--step 'click:<css selector>'] \\
        [--step 'wait:<seconds>'] [--step 'reload'] [--step 'shot:<file.png>'] \\
        [--shot out.png]

click: takes the FIRST match of the selector.  When a page repeats the same
control (sensor cards carry two identical switches per card), select it in a
js: step instead, e.g. find the card by its .name text, then the row by its
.slabel text, and call .click() on that element.

Asset versions: every webapp change bumps ?v= in hub/static/devices.html to
a NEW string — two sessions reusing one string can leave a browser on the
older file (the hub serves statics with max-age=300).

Every js: step prints its value (JSON).  Console errors and uncaught
exceptions are printed at the end and make the exit code 1.
Needs: google-chrome, python websocket-client.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

import websocket  # websocket-client


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class CDP:
    def __init__(self, ws_url: str):
        self.ws = websocket.create_connection(ws_url, timeout=30, suppress_origin=True)
        self.n = 0
        self.events: list[dict] = []

    def call(self, method: str, **params):
        self.n += 1
        mid = self.n
        self.ws.send(json.dumps({"id": mid, "method": method, "params": params}))
        while True:
            msg = json.loads(self.ws.recv())
            if msg.get("id") == mid:
                if "error" in msg:
                    raise RuntimeError(f"{method}: {msg['error']}")
                return msg.get("result", {})
            self.events.append(msg)

    def drain(self, seconds: float) -> None:
        end = time.time() + seconds
        self.ws.settimeout(0.2)
        while time.time() < end:
            try:
                self.events.append(json.loads(self.ws.recv()))
            except websocket.WebSocketTimeoutException:
                pass
        self.ws.settimeout(30)

    def js(self, expr: str):
        r = self.call("Runtime.evaluate", expression=expr, returnByValue=True, awaitPromise=True)
        if r.get("exceptionDetails"):
            raise RuntimeError(f"js error: {r['exceptionDetails'].get('text')} in {expr[:80]}")
        return r.get("result", {}).get("value")

    def shot(self, path: str) -> None:
        data = self.call("Page.captureScreenshot", format="png", captureBeyondViewport=True)["data"]
        with open(path, "wb") as f:
            f.write(base64.b64decode(data))
        print(f"screenshot: {path}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("url")
    ap.add_argument("--size", default="1100x1200")
    ap.add_argument("--wait", type=float, default=4.0, help="seconds after load before the steps")
    ap.add_argument("--step", action="append", default=[])
    ap.add_argument("--shot", default=None, help="final full-page screenshot")
    a = ap.parse_args()
    w, h = (int(v) for v in a.size.split("x"))
    port = free_port()
    profile = tempfile.mkdtemp(prefix="webcheck-")          # fresh profile = fresh localStorage each run
    chrome = subprocess.Popen(
        ["google-chrome", "--headless=new", "--disable-gpu", "--no-sandbox", "--no-first-run",
         f"--remote-debugging-port={port}", f"--user-data-dir={profile}", f"--window-size={w},{h}", "about:blank"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    errors = []
    try:
        for _ in range(100):
            try:
                tabs = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/json"))
                page = next(t for t in tabs if t.get("type") == "page")
                break
            except Exception:
                time.sleep(0.1)
        else:
            print("chrome did not start"); return 2
        c = CDP(page["webSocketDebuggerUrl"])
        for dom in ("Page", "Runtime", "Log"):
            c.call(f"{dom}.enable")
        c.call("Emulation.setDeviceMetricsOverride", width=w, height=h, deviceScaleFactor=1, mobile=False)
        c.call("Page.navigate", url=a.url)
        c.drain(a.wait)
        for st in a.step:
            kind, _, arg = st.partition(":")
            if kind == "js":
                print(f"js {arg[:70]!r} -> {json.dumps(c.js(arg))[:400]}")
            elif kind == "click":
                ok = c.js(f"(function(){{var e=document.querySelector({json.dumps(arg)}); if(!e) return false; e.click(); return true;}})()")
                print(f"click {arg!r} -> {'ok' if ok else 'NOT FOUND'}")
                c.drain(0.5)
            elif kind == "wait":
                c.drain(float(arg))
            elif kind == "reload":
                c.call("Page.reload", ignoreCache=True)
                c.drain(a.wait)
                print("reloaded")
            elif kind == "shot":
                c.shot(arg)
            else:
                print(f"unknown step {st!r}"); return 2
        if a.shot:
            c.shot(a.shot)
        c.drain(0.3)
        for ev in c.events:
            m = ev.get("method")
            if m == "Runtime.exceptionThrown":
                d = ev["params"]["exceptionDetails"]
                errors.append(f"exception: {d.get('text')} {(d.get('exception') or {}).get('description', '')[:200]}")
            elif m == "Runtime.consoleAPICalled" and ev["params"]["type"] in ("error", "assert"):
                errors.append("console.error: " + " ".join(str(x.get("value", x.get("description", ""))) for x in ev["params"]["args"])[:200])
            elif m == "Log.entryAdded" and ev["params"]["entry"]["level"] == "error":
                e = ev["params"]["entry"]
                if (e.get("url") or "").endswith("/favicon.ico"):
                    continue                        # the hub has no favicon; not an app error
                errors.append(f"log: {e.get('text', '')[:160]} {e.get('url', '')}")
    finally:
        chrome.terminate()
        try:
            chrome.wait(5)
        except Exception:
            chrome.kill()
        shutil.rmtree(profile, ignore_errors=True)
    for e in errors:
        print("ERROR", e)
    print(f"{len(errors)} error(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
