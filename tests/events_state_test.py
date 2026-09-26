"""Verify the item state machine exactly as specified.

Drives the LIVE webapp with a real browser, so it needs a reachable hub and a
camera with at least 3 recorded events (the select checks index rows[2]).

    NN_EVENTS_HUB=<hub address> NN_EVENTS_CAM=cam1 python3 events_state_test.py

The camera is a parameter because events are motion-driven and per-camera-life:
re-provisioning a camera resets its life-start, so whichever camera was
hardcoded can legitimately have zero events.  It defaults to cam0 for
compatibility.

NB this file runs its work at IMPORT (it is a script, not a pytest module), and
it is named *_test.py, so `pytest tests/` collects it.  Anything raising here —
no Chrome, no hub, too few events — used to abort collection of the WHOLE suite
with an IndexError.  Bail out cleanly instead, and skip under pytest.
"""
import os
import sys
import time
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.chrome.options import Options
except ImportError:                      # no selenium on this host
    _no_selenium = True
else:
    _no_selenium = False

def bail(msg):
    """Stop without failing.  Under pytest a bare SystemExit at import time is
    an INTERNALERROR that kills the whole run, so skip properly there."""
    if "pytest" in sys.modules:
        import pytest
        pytest.skip(msg, allow_module_level=True)
    raise SystemExit(msg)


HUB = os.environ.get("NN_EVENTS_HUB", "")
CAM = os.environ.get("NN_EVENTS_CAM", "cam0")
if not HUB:
    bail("set NN_EVENTS_HUB to the hub address to run this live test")
if _no_selenium:
    bail("selenium not installed")

o = Options()
for f in ("--headless=new","--no-sandbox","--disable-dev-shm-usage","--window-size=900,1000"):
    o.add_argument(f)
d = webdriver.Chrome(options=o)
ok = fail = 0
def chk(name, cond):
    global ok, fail
    if cond: ok += 1; print(f"  PASS  {name}")
    else: fail += 1; print(f"  FAIL  {name}")
def rows(): return d.find_elements(By.CSS_SELECTOR, ".ev")
def sw(r, dx):
    ActionChains(d).move_to_element_with_offset(r, 60, 10).click_and_hold()\
        .move_by_offset(dx, 0).pause(0.15).release().perform(); time.sleep(0.6)
def tap(r):
    r.click(); time.sleep(0.6)
def tray(r): return "actions-open" in r.get_attribute("class")
def sel(r): return "sel" in r.get_attribute("class")
def selmode(): return "selmode" in d.find_element(By.TAG_NAME,"body").get_attribute("class")
def playing(): return len(d.find_elements(By.CSS_SELECTOR, ".ev-body"))
def reload():
    d.get(f"http://{HUB}:8769/devices#/events/{CAM}"); time.sleep(5)
try:
    reload()
    # Too few events is a MISSING FIXTURE, not a failing state machine — say so
    # plainly instead of dying on an IndexError three lines later.
    if len(rows()) < 3:
        bail(f"{CAM} has {len(rows())} events, need >=3 (events are "
             f"motion-driven and reset when a camera is re-provisioned; set "
             f"NN_EVENTS_CAM to a camera that has some)")
    print(f"NORMAL:  (hub={HUB} cam={CAM})")
    r = rows()[0]
    sw(r, -120); chk("← -> option", tray(r) and playing()==0)
    sw(r, -120); chk("option ← -> stays option", tray(r))
    sw(r, 120);  chk("option → -> normal (not select!)", (not tray(r)) and (not selmode()))
    tap(r);      chk("normal tap -> playback", playing()==1)
    tap(r);      chk("playback tap -> normal", playing()==0)

    print("PLAYBACK:")
    tap(r); time.sleep(0.3)
    sw(r, -120); chk("playback ← -> option, keeps playing", tray(r) and playing()==1)
    sw(r, 120);  chk("option → -> normal, still playing", (not tray(r)) and playing()==1)
    sw(r, 120);  chk("playback → -> select + collapses", selmode() and sel(r) and playing()==0)

    print("SELECT:")
    rs = rows()
    sw(rs[1], -120); chk("select ← -> selected (no tray)", sel(rs[1]) and not tray(rs[1]))
    sw(rs[1], -120); chk("select ← again -> stays selected", sel(rs[1]))
    sw(rs[1], 120);  chk("select → -> deselected", not sel(rs[1]))
    tap(rs[2]);      chk("select tap -> inverse (selects)", sel(rs[2]))
    tap(rs[2]);      chk("select tap -> inverse (deselects)", not sel(rs[2]))
    chk("still in select mode (row0 selected)", selmode() and sel(rows()[0]))
    sw(rows()[0], 120)
    chk("last deselect -> normal mode", (not selmode()) and len(d.find_elements(By.ID,"bulkbar"))==0)
    chk("no playback opened during select", playing()==0)
finally:
    print(f"\n{ok} passed, {fail} failed")
    d.quit()
