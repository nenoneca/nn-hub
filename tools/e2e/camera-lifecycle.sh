#!/bin/bash
# Camera lifecycle E2E — drives the SAME endpoints the webapp's buttons do,
# so a pass here means the UI flow works, not just some parallel API.
#
#   camera-lifecycle.sh --ssid <wifi> --pass <secret> [--cam-a cam0] [--cam-b cam1]
#
# Phases (each asserts VIDEO IS FLOWING at the end — a camera that
# registers but does not stream is a failure):
#   A  unregister one camera, re-provision it under a NEW name
#   B  unregister BOTH cameras, then provision them back in REVERSE order
#   C  delete both archive entries
#
# Why reverse order in B: it is the case that breaks slot inference.  With
# two slots free at once the hub cannot guess, so the wizard must carry an
# explicit target — this proves a device never lands in the wrong slot.
#
# Credentials come from the command line or NN_E2E_WIFI_SSID / _PASS; the
# password is never written to disk or into the hub.
set -u

HUB=${HUB:?set HUB to the hub address, e.g. HUB=nn-hub.local}
API="http://$HUB:8769/api/v1"
CAM_A=${CAM_A:-cam0}
CAM_B=${CAM_B:-cam1}
SSID=${NN_E2E_WIFI_SSID:-}
PASS=${NN_E2E_WIFI_PASS:-}
PASS_N=0; FAIL_N=0

while [ $# -gt 0 ]; do
  case "$1" in
    --ssid)  SSID=$2; shift 2 ;;
    --pass)  PASS=$2; shift 2 ;;
    --cam-a) CAM_A=$2; shift 2 ;;
    --cam-b) CAM_B=$2; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
[ -n "$SSID" ] && [ -n "$PASS" ] || {
  echo "need --ssid and --pass (the cameras must rejoin Wi-Fi)" >&2; exit 2; }

# This test FACTORY-RESETS real cameras.  Wrong credentials leave them
# unprovisioned and off the network until someone re-provisions them by
# hand — which is exactly what a careless dry run did on 2026-08-21.
# Refuse to start without an explicit acknowledgement.
if [ "${NN_E2E_DESTRUCTIVE:-}" != "yes" ]; then
  cat >&2 <<'WARN'
REFUSING TO RUN: this test unregisters (factory-resets) live cameras and
re-provisions them.  If the Wi-Fi credentials are wrong the cameras stay
offline until re-provisioned by hand.

Re-run with:  NN_E2E_DESTRUCTIVE=yes camera-lifecycle.sh --ssid ... --pass ...
WARN
  exit 2
fi

ck() {  # ck <name> <rc> <evidence>
  if [ "$2" = 0 ]; then PASS_N=$((PASS_N+1)); printf 'PASS  %-46s %s\n' "$1" "$3"
  else FAIL_N=$((FAIL_N+1)); printf 'FAIL  %-46s %s\n' "$1" "$3"; fi
}
jqv() { python3 -c "import sys,json;
try: print(json.load(sys.stdin)$1)
except Exception: print('')" 2>/dev/null; }

cam_names() { curl -sf -m 15 "$API/cameras" | jqv "" ; }
cam_listed() { curl -sf -m 15 "$API/cameras" | grep -q "\"$1\""; }
cam_name_of() { curl -sf -m 15 "$API/cameras" | python3 -c "
import sys,json
for c in json.load(sys.stdin):
    if c['id']=='$1': print(c.get('name') or ''); break" 2>/dev/null; }

# Video is the only proof that matters: the media sequence advances ONLY
# when new segments are muxed, so a cached playlist cannot fake it.
video_flowing() {  # video_flowing <cam> <deadline_s>
  local cam=$1 deadline=$2 t0 a b
  t0=$(date +%s)
  while [ $(( $(date +%s) - t0 )) -lt "$deadline" ]; do
    a=$(curl -sf -m 15 "$API/cameras/$cam/hls/live.m3u8" 2>/dev/null | sed -n 's/#EXT-X-MEDIA-SEQUENCE:\([0-9]*\)/\1/p')
    sleep 8
    b=$(curl -sf -m 15 "$API/cameras/$cam/hls/live.m3u8" 2>/dev/null | sed -n 's/#EXT-X-MEDIA-SEQUENCE:\([0-9]*\)/\1/p')
    if [ -n "$a" ] && [ -n "$b" ] && [ "$b" -gt "$a" ]; then
      EVID="seq $a→$b"; return 0
    fi
    sleep 4
  done
  EVID="no segment advance in ${deadline}s (last: ${a:-none}→${b:-none})"
  return 1
}

wait_listed() {  # wait_listed <cam> <deadline_s>
  local t0; t0=$(date +%s)
  until cam_listed "$1"; do
    [ $(( $(date +%s) - t0 )) -gt "$2" ] && return 1
    sleep 5
  done
  return 0
}

scan_cams() {  # scan_cams → every camera BLE addr currently in setup mode
  curl -sf -m 60 -X POST "$API/provision/scan" \
       -H 'Content-Type: application/json' -d '{"scan_time": 12}' \
    | python3 -c "
import sys,json
for c in (json.load(sys.stdin).get('candidates') or []):
    if c.get('kind')=='camera_esp': print(c['addr'])" 2>/dev/null
}

# A cleared camera reboots into setup mode; on the ESP boards that takes
# 30-60 s, so a single scan straight after the reset finds nothing and the
# test reports a failure that is really just impatience (2026-08-21).
# 420 s, not 150: a cleared camera that reboots cleanly advertises within
# ~30 s, but one that stays up runs its netstream retry loop to exhaustion
# first and only then falls back to BLE — measured at ~6 min on 2026-08-21.
find_addr() {  # find_addr [want_addr] [deadline_s] → addr, or "" on timeout
  local want=${1:-} deadline=${2:-420} t0 a
  t0=$(date +%s)
  while [ $(( $(date +%s) - t0 )) -lt "$deadline" ]; do
    for a in $(scan_cams); do
      if [ -z "$want" ] || [ "${a^^}" = "${want^^}" ]; then echo "$a"; return 0; fi
    done
    sleep 5
  done
  return 1
}

# Which board owns a slot right now.  Recorded by the hub at provisioning
# time; the test needs it so it can put each camera back where it came from
# instead of into whichever slot it reached first — the bug that swapped two
# cameras and left one dark on 2026-08-21.
cam_addr_of() {  # cam_addr_of <cam>
  curl -sf -m 15 "$API/cameras" | python3 -c "
import sys,json
for c in json.load(sys.stdin):
    if c['id']=='$1': print(c.get('addr') or ''); break" 2>/dev/null
}

unregister() {  # unregister <cam> — non-force: a real device reset
  curl -sf -m 60 -X POST "$API/cameras/$1/unregister" \
       -H 'Content-Type: application/json' -d '{}' 2>/dev/null
}

provision() {  # provision <name> <addr> [slot] → job outcome
  local body
  body=$(python3 -c "
import json,sys
b={'kind':'camera_esp','name':sys.argv[1],'addr':sys.argv[2],
   'ssid':sys.argv[3],'password':sys.argv[4]}
if len(sys.argv)>5 and sys.argv[5]: b['target_cam']=sys.argv[5]
print(json.dumps(b))" "$1" "$2" "$SSID" "$PASS" "${3:-}")
  local jid t0 st
  jid=$(curl -sf -m 30 -X POST "$API/provision/jobs" \
        -H 'Content-Type: application/json' -d "$body" | jqv "['id']")
  [ -n "$jid" ] || { JOB_STATE="rejected"; return 1; }
  t0=$(date +%s)
  while [ $(( $(date +%s) - t0 )) -lt 180 ]; do
    st=$(curl -sf -m 15 "$API/provision/jobs/$jid" | jqv "['state']")
    case "$st" in
      done)        JOB_STATE=done; return 0 ;;
      # "unconfirmed" = writes landed but the device never sent its status
      # notify.  That was the norm until the NimBLE conn-handle-0 bug was
      # fixed (nn-modules bd9fee1); it is now a REGRESSION, so fail on it.
      # The camera may still come up — the video check reports that
      # separately — but the operator was not told the truth, and the
      # webapp would show a red "device did not confirm".
      unconfirmed) JOB_STATE="unconfirmed (status notify missing)"; return 1 ;;
      error)       JOB_STATE="error: $(curl -sf -m 15 "$API/provision/jobs/$jid" | jqv "['error']")"; return 1 ;;
    esac
    sleep 4
  done
  JOB_STATE="timeout"; return 1
}

archive_key() {  # archive_key <cam> → newest archive entry for that slot
  curl -sf -m 15 "$API/archive" | python3 -c "
import sys,json
ks=[a['device_id'] for a in json.load(sys.stdin)
    if a['device_id'].split('@')[0]=='$1']
print(sorted(ks)[-1] if ks else '')" 2>/dev/null
}

echo "── camera lifecycle E2E · hub $HUB · $(date -u +%FT%TZ) ──"
echo "   cameras under test: $CAM_A, $CAM_B"

# baseline: both must be streaming, else the test proves nothing
for c in "$CAM_A" "$CAM_B"; do
  video_flowing "$c" 90; ck "baseline $c streaming" $? "$EVID"
done
[ $FAIL_N -gt 0 ] && { echo "── aborting: baseline not healthy ──"; exit 1; }

# ── PHASE A: unregister one, re-provision under a NEW name ───────────────
echo "── A: unregister $CAM_A, re-provision as a new name ──"
NEW_NAME="e2e-$(date +%H%M%S)"
ADDR_A=$(cam_addr_of "$CAM_A"); ADDR_B=$(cam_addr_of "$CAM_B")
echo "   slot owners before: $CAM_A=${ADDR_A:-unknown} $CAM_B=${ADDR_B:-unknown}"
R=$(unregister "$CAM_A")
echo "$R" | grep -q '"ok": true'; ck "A1 $CAM_A unregistered" $? "$(echo "$R" | head -c 80)"
echo "$R" | grep -q '"cleared": true'
ck "A2 device really cleared (not force)" $? "cleared=$(echo "$R" | jqv "['cleared']")"
cam_listed "$CAM_A"; [ $? -ne 0 ]; ck "A3 hidden from camera list" $? "gone while archived"

ADDR=$(find_addr "$ADDR_A")
[ -n "$ADDR" ]; ck "A4 advertises for provisioning" $? "${ADDR:-not found within 420s}"
if [ -n "$ADDR" ]; then
  provision "$NEW_NAME" "$ADDR" "$CAM_A"; rc=$?
  ck "A5 provisioning job" $rc "$JOB_STATE"
  wait_listed "$CAM_A" 150; ck "A6 re-registered" $? "slot $CAM_A"
  video_flowing "$CAM_A" 180; ck "A7 video flowing again" $? "$EVID"
  [ "$(cam_name_of "$CAM_A")" = "$NEW_NAME" ]
  ck "A8 shows the NEW name" $? "want '$NEW_NAME', got '$(cam_name_of "$CAM_A")'"
fi

# ── PHASE B: unregister BOTH, provision back in REVERSE order ────────────
echo "── B: unregister $CAM_A + $CAM_B, re-provision in reverse ──"
unregister "$CAM_A" >/dev/null; unregister "$CAM_B" >/dev/null
sleep 20
cam_listed "$CAM_A" || cam_listed "$CAM_B"; [ $? -ne 0 ]
ck "B1 both unregistered" $? "neither listed"

# Reverse order: B first, then A — and each names BOTH its slot and the
# board that belongs in it.  With two slots free the hub cannot infer either,
# and taking "whoever advertises first" is what silently swapped the two
# cameras.  Waiting for the specific address is the whole point of the phase.
for slot in "$CAM_B" "$CAM_A"; do
  [ "$slot" = "$CAM_A" ] && want=$ADDR_A || want=$ADDR_B
  ADDR=$(find_addr "$want")
  if [ -z "$ADDR" ]; then
    ck "B2 $slot: device advertising" 1 "${want:-camera} never advertised in 420s"
    continue
  fi
  provision "e2e-$slot" "$ADDR" "$slot"; rc=$?
  ck "B2 $slot: provisioning job" $rc "$JOB_STATE (addr $ADDR)"
  [ -z "$want" ] || [ "${ADDR^^}" = "${want^^}" ]
  ck "B2b $slot: got ITS OWN board back" $? "want ${want:-any}, used $ADDR"
  wait_listed "$slot" 150; ck "B3 $slot: re-registered" $? "back in list"
  video_flowing "$slot" 180; ck "B4 $slot: video flowing" $? "$EVID"
done

# ── PHASE C: delete the archive entries ─────────────────────────────────
echo "── C: delete archive entries ──"
for slot in "$CAM_A" "$CAM_B"; do
  k=$(archive_key "$slot")
  if [ -z "$k" ]; then ck "C1 $slot: archive entry present" 1 "none found"; continue; fi
  D=$(curl -sf -m 30 -X DELETE "$API/archive/$k")
  echo "$D" | grep -q '"ok": true'
  ck "C1 $slot: archive deleted" $? "$(echo "$D" | head -c 70)"
  # deleting history must NOT disturb the live camera
  cam_listed "$slot"; ck "C2 $slot: still live after delete" $? "history-only delete"
done

for c in "$CAM_A" "$CAM_B"; do
  video_flowing "$c" 90; ck "C3 $c still streaming at the end" $? "$EVID"
done

# The fleet must come out of the test exactly as it went in: same board in
# same slot.  A test that leaves two cameras swapped has not proven the
# lifecycle works, it has just hidden the damage behind passing checks.
for c in "$CAM_A" "$CAM_B"; do
  [ "$c" = "$CAM_A" ] && want=$ADDR_A || want=$ADDR_B
  now=$(cam_addr_of "$c")
  [ -z "$want" ] || [ "${now^^}" = "${want^^}" ]
  ck "C4 $c: same board as before the test" $? "was ${want:-unknown}, now ${now:-unknown}"
done

echo "── result: $PASS_N pass, $FAIL_N fail ──"
[ $FAIL_N = 0 ]
