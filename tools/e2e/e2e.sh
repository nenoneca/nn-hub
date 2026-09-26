#!/bin/bash
# nn fleet end-to-end test.
#
#   e2e.sh            fast, non-destructive: sessions, field ops,
#                     cascade, caches, camera, catalog (~2 min)
#   e2e.sh --ota V    additionally runs a REAL sensor OTA cycle to
#                     version V on one device (~10 min, reboots it)
#
# Exit 0 = all PASS.  Each check prints PASS/FAIL with its evidence.
set -u
HUB=${HUB:?set HUB to the hub address, e.g. HUB=nn-hub.local}
API="http://$HUB:8769/api/v1"
SENSORS=(c6-s1 c6-s2 c6-s3)
CAM=cam3
PASS=0; FAIL=0; SKIP=0

ck() {  # ck <name> <ok:0|1> <evidence>
  if [ "$2" = 0 ]; then PASS=$((PASS+1)); printf "PASS  %-34s %s\n" "$1" "$3"
  else FAIL=$((FAIL+1)); printf "FAIL  %-34s %s\n" "$1" "$3"; fi
}
sk() {  # sk <name> <why> — a device busy with OTA isn't a failure
  SKIP=$((SKIP+1)); printf "SKIP  %-34s %s\n" "$1" "$2"
}
busy() {  # busy <dev> → 0 when the device is mid-OTA
  local s
  s=$(curl -sf -m 15 "$API/devices/$1/ota" 2>/dev/null | python3 -c "
import sys,json
try: print(json.load(sys.stdin).get('state') or '')
except Exception: print('')" 2>/dev/null)
  case "$s" in downloading*|applying*|armed*) return 0 ;; *) return 1 ;; esac
}

echo "── nn fleet E2E · hub $HUB · $(date -u +%FT%TZ) ──"
# Let any prior mesh traffic drain: a cascade storm (this test's own,
# or a previous run's) delays field replies by seconds — measuring
# through it says nothing about steady-state health.
sleep "${SETTLE:-20}"

# 1. hub alive
H=$(curl -sf -m 10 "$API/healthz" 2>/dev/null)
ck "hub healthz" $? "$H"

# 2. per-sensor: field read (sealed path) latency + write round-trip
for d in "${SENSORS[@]}"; do
  if busy "$d"; then sk "$d sealed read <5s" "mid-OTA"; continue; fi
  read_once() {
    local t0 t1 R
    t0=$(date +%s%3N)
    R=$(curl -sf -m 20 "$API/devices/$1/field/led" 2>/dev/null)
    t1=$(date +%s%3N); RD_MS=$((t1-t0))
    echo "$R" | grep -q '"value"' && [ $RD_MS -le 5000 ]
  }
  if read_once "$d"; then ck "$d sealed read <5s" 0 "${RD_MS}ms"
  else
    first=$RD_MS; sleep 12          # one retry after a brief quiet period
    read_once "$d"
    ck "$d sealed read <5s" $? "${RD_MS}ms (first ${first}ms)"
  fi
done
W=$(curl -sf -m 25 -X PUT "$API/devices/c6-s2/field/led" \
     -H 'Content-Type: application/json' -d '{"value":1}' 2>/dev/null)
echo "$W" | grep -q '"value": 1'; ck "c6-s2 write round-trip" $? "$W"

# 3. cascade: toggle s1 button_v, expect all three led caches fresh
TGT=1; PRE=0        # drive LOW first so the tested edge is a REAL change
curl -sf -m 25 -X PUT "$API/devices/c6-s1/field/button_v" \
     -H 'Content-Type: application/json' -d "{\"value\":$PRE}" >/dev/null 2>&1
sleep 14
NOW=$(date +%s)
curl -sf -m 25 -X PUT "$API/devices/c6-s1/field/button_v" \
     -H 'Content-Type: application/json' -d "{\"value\":$TGT}" >/dev/null 2>&1
sleep 18
cascade_check() {  # cascade_check <dev> <since> → 0 when led==TGT and fresh
  local C V val ts
  C=$(curl -sf -m 15 "$API/devices/$1/fields/cache" 2>/dev/null)
  V=$(echo "$C" | python3 -c "
import sys,json
f=json.load(sys.stdin)['fields'].get('led',{})
print(int(f.get('v',-1)), f.get('ts',0))" 2>/dev/null)
  val=${V%% *}; ts=${V##* }
  CC_EVID="v=$val +$(( ts - $2 ))s"
  [ "$val" = "$TGT" ] && [ $(( ts - $2 )) -ge 0 ] && [ $(( ts - $2 )) -lt 90 ]
}

RETRY=()
for d in "${SENSORS[@]}"; do
  if busy "$d"; then sk "$d cascade led=$TGT fresh" "mid-OTA"; continue; fi
  if cascade_check "$d" "$NOW"; then ck "$d cascade led=$TGT fresh" 0 "$CC_EVID"
  else RETRY+=("$d"); fi
done

# One retry round for devices that missed the edge: a single lost D2D
# frame is expected on a weak link (the system retries by design); a
# device that misses TWO consecutive cascades is a real failure.
if [ ${#RETRY[@]} -gt 0 ]; then
  echo "      (retrying cascade for: ${RETRY[*]})"
  curl -sf -m 25 -X PUT "$API/devices/c6-s1/field/button_v" \
       -H 'Content-Type: application/json' -d "{\"value\":$PRE}" >/dev/null 2>&1
  sleep 16
  NOW2=$(date +%s)
  curl -sf -m 25 -X PUT "$API/devices/c6-s1/field/button_v" \
       -H 'Content-Type: application/json' -d "{\"value\":$TGT}" >/dev/null 2>&1
  sleep 20
  for d in "${RETRY[@]}"; do
    cascade_check "$d" "$NOW2"
    ck "$d cascade led=$TGT fresh (retry)" $? "$CC_EVID"
  done
fi

# 4. image identity + catalog reachability
for d in "${SENSORS[@]}"; do
  IMG=$(curl -sf -m 15 "$API/devices/$d" 2>/dev/null \
        | sed -n 's/.*"image": "\([^"]*\)".*/\1/p')
  # renamed fw 0.0.31 (nn-modules 272aa5b); the old key is accepted so this
  # suite still passes against a pre-rename fleet mid-migration.
  [ "$IMG" = "nn-app-mdns-ot-esp32c6" ] || [ "$IMG" = "mdns_ot_esp32c6" ]
  ck "$d image identity" $? "$IMG"
done
N=$(curl -sf -m 15 "$API/firmware/catalog?device_type=mdns_ot_esp32c6" 2>/dev/null \
    | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null)
[ "${N:-0}" -ge 1 ]; ck "sensor catalog non-empty" $? "$N entries"

# 5. camera: bundle+platform report fresh, stream serving
B=$(curl -sf -m 15 "$API/cameras/$CAM/bundle" 2>/dev/null)
AGE=$(echo "$B" | python3 -c "
import sys,json,time
j=json.load(sys.stdin); print(int(time.time()-j.get('reported_at',0)), j.get('version'), j.get('platform'))" 2>/dev/null)
age=${AGE%% *}
[ -n "$age" ] && [ "$age" -lt 1200 ]; ck "cam3 report fresh (<20min)" $? "$AGE"
live_snapshot() {  # live_snapshot <cam> → 0 when video is actually FLOWING
  # A snapshot that merely returns bytes proves nothing: the endpoint
  # serves a cached frame, so a camera whose sensor died still answers
  # with a stale JPEG (exactly how cam0/cam1 looked "healthy" for 15
  # hours after they stopped producing video).  The HLS media sequence
  # only advances when new segments are muxed, so it is the honest
  # liveness signal.
  local seq_a seq_b
  SNAP_SZ=$(curl -sf -m 20 -o /dev/null -w '%{size_download}' "$API/cameras/$1/snapshot.jpg" 2>/dev/null)
  seq_a=$(curl -sf -m 15 "$API/cameras/$1/hls/live.m3u8" 2>/dev/null \
          | sed -n 's/#EXT-X-MEDIA-SEQUENCE:\([0-9]*\)/\1/p' | head -1)
  sleep 6
  seq_b=$(curl -sf -m 15 "$API/cameras/$1/hls/live.m3u8" 2>/dev/null \
          | sed -n 's/#EXT-X-MEDIA-SEQUENCE:\([0-9]*\)/\1/p' | head -1)
  if [ -n "$seq_a" ] && [ -n "$seq_b" ] && [ "$seq_b" -gt "$seq_a" ]; then
    SNAP_NOTE="${SNAP_SZ}B, +$(( seq_b - seq_a )) segments/6s"
    return 0
  fi
  SNAP_NOTE="${SNAP_SZ:-0}B but NO NEW SEGMENTS (seq stuck at ${seq_a:-?}) — video stopped"
  return 1
}

live_snapshot "$CAM"; ck "cam3 stream live" $? "$SNAP_NOTE"

# 5b. ESP cameras: stream always, firmware report once they carry it
for c in ${ESP_CAMS:-cam0 cam1}; do
  live_snapshot "$c"; ck "$c stream live" $? "$SNAP_NOTE"
  B=$(curl -sf -m 15 "$API/cameras/$c/bundle" 2>/dev/null)
  V=$(echo "$B" | python3 -c "
import sys,json
try: print(json.load(sys.stdin).get('version') or '')
except Exception: print('')" 2>/dev/null)
  if [ -z "$V" ]; then
    # firmware that predates fw/img reporting — expected until flashed
    sk "$c firmware report" "not reporting yet (pre-flash)"
  else
    AGE=$(echo "$B" | python3 -c "
import sys,json,time
print(int(time.time()-json.load(sys.stdin).get('reported_at',0)))" 2>/dev/null)
    [ "${AGE:-99999}" -lt 3600 ]; ck "$c firmware report" $? "$V, ${AGE}s ago"
  fi
done

# 6. optional destructive: real OTA cycle on one sensor
if [ "${1:-}" = "--ota" ]; then
  V=${2:?--ota needs a version}
  D=c6-s3
  echo "── OTA cycle: $D -> $V ──"
  curl -sf -m 60 -X POST "$API/firmware/catalog/mdns_ot_esp32c6/$V/promote" \
       -H 'Content-Type: application/json' -d '{}' >/dev/null
  curl -sf -m 30 -X POST "$API/devices/$D/ota" >/dev/null
  T0=$(date +%s); ok=1
  while [ $(( $(date +%s) - T0 )) -lt 900 ]; do
    ST=$(curl -sf -m 15 "$API/devices/$D/ota" 2>/dev/null | python3 -c "
import sys,json; j=json.load(sys.stdin); print(j.get('state'),j.get('running_version'))" 2>/dev/null)
    echo "$ST" | grep -q "$V" && { ok=0; break; }
    echo "$ST" | grep -q "^armed" && \
      curl -sf -m 30 -X POST "$API/devices/$D/ota/apply" >/dev/null 2>&1
    sleep 30
  done
  ck "OTA $D -> $V" $ok "$(( $(date +%s) - T0 ))s"
fi

echo "── result: $PASS pass, $FAIL fail, $SKIP skipped ──"
[ $FAIL = 0 ]
