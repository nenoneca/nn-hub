#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Live hardware E2E test for the D2G DEVICE_THREAD_STATE auto-refresh
# of provision_info.ml_eid.  Requires a running nn-hub + nn-gw and at
# least one sensor that's currently sending D2H frames (so the gateway's
# proto_router cache has an entry to replay).
#
# Algorithm:
#   1. Pick a target device that has a non-empty ml_eid in the hub.
#   2. Snapshot its current ml_eid as `original`.
#   3. Overwrite ml_eid with a sentinel via POST /devices/{dev}/provision.
#   4. Wait for ≥2 gateway 5-second heartbeat ticks (~12 s).
#   5. Re-read ml_eid; it must match `original` again (auto-refreshed).
#
# Exit codes: 0 = passed, 1 = failed, 2 = no eligible device found.
#
# Usage:
#   HUB_URL=http://<hub-address>:8769 ./test_ml_eid_autorefresh.sh [device-name]
# If device-name is omitted, the first device with a non-empty ml_eid is used.

set -euo pipefail

HUB_URL=${HUB_URL:-http://localhost:8769}
TARGET=${1:-}
SENTINEL=${SENTINEL:-fd00:dead:beef::1}
WAIT_SECONDS=${WAIT_SECONDS:-12}

# ── helpers ──────────────────────────────────────────────────────────────

j() { python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get(sys.argv[1], ""))' "$1"; }

list_devices() {
    curl -fsS -m 5 "$HUB_URL/api/v1/devices" \
        | python3 -c 'import json,sys; [print(d["name"]) for d in json.load(sys.stdin)]'
}

device_ml_eid() {
    curl -fsS -m 5 "$HUB_URL/api/v1/devices/$1" | j ml_eid
}

set_ml_eid() {
    local dev=$1 ml_eid=$2
    curl -fsS -m 5 -X POST -H 'Content-Type: application/json' \
        -d "{\"ml_eid\":\"$ml_eid\"}" \
        "$HUB_URL/api/v1/devices/$dev/provision" > /dev/null
}

# ── pick target ──────────────────────────────────────────────────────────

if [[ -z "$TARGET" ]]; then
    for d in $(list_devices); do
        ml=$(device_ml_eid "$d" || echo "")
        if [[ -n "$ml" ]]; then
            TARGET=$d
            break
        fi
    done
fi
if [[ -z "$TARGET" ]]; then
    echo "no device with a non-empty ml_eid found in $HUB_URL" >&2
    exit 2
fi

ORIGINAL=$(device_ml_eid "$TARGET")
if [[ -z "$ORIGINAL" ]]; then
    echo "$TARGET has empty ml_eid — cannot run test" >&2
    exit 2
fi
echo "target: $TARGET"
echo "  original ml_eid: $ORIGINAL"
echo "  sentinel:        $SENTINEL"

# ── perturb ──────────────────────────────────────────────────────────────

set_ml_eid "$TARGET" "$SENTINEL"
STALED=$(device_ml_eid "$TARGET")
if [[ "$STALED" != "$SENTINEL" ]]; then
    echo "FAIL: set_ml_eid didn't take (got $STALED)" >&2
    exit 1
fi
echo "  staled, waiting ${WAIT_SECONDS}s for next heartbeat..."

sleep "$WAIT_SECONDS"

# ── verify ───────────────────────────────────────────────────────────────

NOW=$(device_ml_eid "$TARGET")
if [[ "$NOW" == "$ORIGINAL" ]]; then
    echo "PASS: ml_eid auto-refreshed to $NOW"
    exit 0
fi
if [[ "$NOW" == "$SENTINEL" ]]; then
    echo "FAIL: ml_eid still stale after ${WAIT_SECONDS}s" >&2
    echo "  gateway probably isn't running the DEVICE_THREAD_STATE patch" >&2
    echo "  OR target device hasn't sent any D2H for the gateway to cache" >&2
    # Restore original to leave hub in a clean state.
    set_ml_eid "$TARGET" "$ORIGINAL"
    exit 1
fi
echo "FAIL: ml_eid is $NOW (neither original nor sentinel)" >&2
set_ml_eid "$TARGET" "$ORIGINAL"
exit 1
