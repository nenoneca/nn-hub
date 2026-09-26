#!/bin/bash
# nn-ota agent — BeagleY camera updater (bundle + platform layers).
#
# Runs as a ONESHOT on the host (timer → nn-ota-launcher → this script
# via the `current` symlink), so each tick executes the newest version
# of itself: updating the bundle updates the agent.
#
# Order is deliberate: BUNDLE first, then PLATFORM.  An agent fix must
# be able to land before the agent performs the riskier platform work.
#
# Both layers use the same two-tick contract:
#   tick N   : stage → verify → switch → write `pending` → restart
#   tick N+1 : health-check → confirm, or roll back + blacklist
# Platform health additionally requires onnxruntime mapped in-process
# (a broken platform streams happily with inference dead).
set -u
# The hub is whatever this camera was PROVISIONED with (the same kv entry
# nn-gw-agent reads); NN_HUB overrides it for bench runs.  No address is
# baked in: an unprovisioned camera has no hub to update from.
KV=${NN_KV:-/var/lib/nn/kv/nnprov}
HUB=${NN_HUB:-$(cat "$KV/hub_host" 2>/dev/null || true)}
if [ -z "$HUB" ]; then echo "[nn-ota] no hub_host in $KV (not provisioned) -- idle"; exit 0; fi
API="http://$HUB:8769/api/v1"
FW="http://$HUB:8770/gw_firmware"
TYPE=byai_camera
PTYPE=byai_platform
EDGE=/opt/edgeai
NN=/opt/nn-app          # app layer on the HOST, bind-mounted to /opt/nn
REL=$NN/releases
CUR=$NN/current
STATE=/var/lib/nn-ota
mkdir -p "$REL" "$STATE" 2>/dev/null || true

log() { echo "[nn-ota] $*"; }

cur_ver()  { sed -n 's/.*"version"[: ]*"\([^"]*\)".*/\1/p' "$CUR/manifest.json" 2>/dev/null | head -1; }
plat_cur() { cat $EDGE/root/etc/nn-platform-version 2>/dev/null; }

report() {
    local v pv; v=$(cur_ver); pv=$(plat_cur)
    [ -n "$v" ] && curl -sf -m 8 -X PUT "$API/cameras/cam3/bundle" \
        -H "Content-Type: application/json" \
        -d "{\"version\":\"$v\",\"device_type\":\"$TYPE\",\"platform\":\"$pv\"}" >/dev/null 2>&1
}

healthy() {
    systemctl is-active --quiet nn-camera || return 1
    local pid; pid=$(pgrep -f "nn/current/nn-camera" | head -1)
    [ -n "$pid" ] || return 1
    local age; age=$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d " ")
    [ "${age:-0}" -ge 25 ] || return 1
    return 0
}

platform_health() {
    healthy || return 1
    local pid; pid=$(pgrep -f "nn/current/nn-camera" | head -1)
    [ "$(grep -cE "libonnxruntime" /proc/$pid/maps 2>/dev/null)" -ge 1 ] || return 1
    return 0
}

stop_stack() {
    systemctl stop nn-camera 2>/dev/null
    systemctl stop nn-npu 2>/dev/null
    lxc-stop -n edgeai -t 15 2>/dev/null
    umount $EDGE/root 2>/dev/null
    return 0
}
start_stack() { systemctl start nn-npu; sleep 8; systemctl start nn-camera; }

# ── power-loss recovery ────────────────────────────────────────────────
# The platform switch is two renames.  A power cut between them leaves
# the image file missing and the camera dead until someone intervenes —
# unless the intent is journalled first and replayed here.  Runs before
# anything else, every tick, and is a no-op in the normal case.
JRN=$STATE/plat-swap-journal
recover_swap() {
    [ -f "$JRN" ] || return 0
    local want; want=$(cat "$JRN")
    log "swap journal found (target $want) — recovering"
    if [ -f $EDGE/edgeai-rootfs.ext4 ]; then
        log "recovery: image present, nothing to replay"
    elif [ -f $EDGE/edgeai-rootfs-new.ext4 ]; then
        mv $EDGE/edgeai-rootfs-new.ext4 $EDGE/edgeai-rootfs.ext4
        log "recovery: completed the swap to $want"
    elif [ -f $EDGE/edgeai-rootfs-prev.ext4 ]; then
        mv $EDGE/edgeai-rootfs-prev.ext4 $EDGE/edgeai-rootfs.ext4
        echo "$want" > "$STATE/plat-blocked"
        log "recovery: restored previous platform (blacklisted $want)"
    else
        log "recovery: NO image available — manual intervention required"
        rm -f "$JRN"; return 1
    fi
    rm -f "$JRN"
    mountpoint -q $EDGE/root || start_stack
    return 0
}

# ── platform swap executor ─────────────────────────────────────────────
# Re-exec'd from /tmp: the swap unmounts the filesystem this script
# normally lives on.
if [ "${1:-}" = "__platform_swap" ]; then
    NEWV="$2"; NEWFILE="$3"; CURV="$(plat_cur)"
    echo "${CURV:-none}" > "$STATE/plat-pending"
    echo "$NEWV" > "$JRN"          # journal BEFORE touching any file
    stop_stack
    mv $EDGE/edgeai-rootfs.ext4 $EDGE/edgeai-rootfs-prev.ext4
    mv "$NEWFILE" $EDGE/edgeai-rootfs.ext4
    rm -f "$JRN"                   # both renames done: intent fulfilled
    start_stack
    log "platform flipped to $NEWV (pending confirm next tick)"
    exit 0
fi

# ── single-instance guard ──────────────────────────────────────────────
# A long platform download can outlast the tick interval; overlapping
# agents would fight over the same files.
exec 9>"$STATE/agent.lock"
flock -n 9 || { log "another tick is still running — skipping"; exit 0; }

recover_swap || exit 1

# ── log rotation (cap from the hub) ────────────────────────────────────
rotate_log() {
    local logf="$NN/log/cam.log"
    [ -f "$logf" ] || return 0
    local cap_kb
    cap_kb=$(curl -sf -m 8 "$API/settings/camera-log-cap" 2>/dev/null \
             | sed -n 's/.*"cap_kb"[: ]*\([0-9]*\).*/\1/p')
    [ -n "$cap_kb" ] || cap_kb=4096
    local sz_kb=$(( $(stat -c%s "$logf" 2>/dev/null || echo 0) / 1024 ))
    if [ "$sz_kb" -gt "$cap_kb" ]; then
        cp "$logf" "$logf.1" && : > "$logf"
        log "rotated cam.log (${sz_kb} KB > cap ${cap_kb} KB)"
    fi
}
rotate_log

# ── LAYER 1: bundle ────────────────────────────────────────────────────
if [ -f "$STATE/pending" ]; then
    NEWV=$(cur_ver); PREV=$(cat "$STATE/pending")
    if healthy; then
        log "CONFIRMED $NEWV (was $PREV)"
        rm -f "$STATE/pending"
    else
        log "health check FAILED on $NEWV — rolling back to $PREV"
        [ -d "$REL/$PREV" ] && { ln -sfn "releases/$PREV" "$CUR.tmp" && mv -T "$CUR.tmp" "$CUR"; systemctl restart nn-camera; }
        echo "$NEWV" > "$STATE/blocked"
        rm -f "$STATE/pending"
    fi
    report; exit 0
fi

TARGET=$(curl -sf -m 10 "$API/firmware" | python3 -c "
import sys, json
for t in json.load(sys.stdin):
    if t.get('device_type') == '$TYPE':
        print(t.get('target_version') or t.get('version') or ''); break" 2>/dev/null)
CURV=$(cur_ver)
if [ -n "$TARGET" ] && [ "$TARGET" != "$CURV" ] && \
   ! { [ -f "$STATE/blocked" ] && [ "$(cat "$STATE/blocked")" = "$TARGET" ]; }; then
    log "update $CURV -> $TARGET"
    META=$(curl -sf -m 10 "$FW/$TYPE/$TARGET/meta") || { log "meta fetch failed"; exit 1; }
    SHA=$(echo "$META" | sed -n 's/.*"sha256"[: ]*"\([^"]*\)".*/\1/p')
    TMP=$(mktemp /tmp/nn-ota.XXXXXX.tgz)
    curl -sf -m 300 -o "$TMP" "$FW/$TYPE/$TARGET" || { log "download failed"; rm -f "$TMP"; exit 1; }
    [ "$(sha256sum "$TMP" | cut -d" " -f1)" = "$SHA" ] || { log "sha mismatch"; rm -f "$TMP"; exit 1; }
    DEST="$REL/$TARGET"; rm -rf "$DEST"; mkdir -p "$DEST"
    tar -xzf "$TMP" -C "$DEST" || { log "unpack failed"; rm -rf "$DEST" "$TMP"; exit 1; }
    rm -f "$TMP"
    for f in nn-camera cam_run.sh agent.sh manifest.json; do
        [ -e "$DEST/$f" ] || { log "bundle missing $f"; rm -rf "$DEST"; exit 1; }
    done
    REQP=$(sed -n 's/.*"requires_platform"[: ]*"\([^"]*\)".*/\1/p' "$DEST/manifest.json")
    if [ -n "$REQP" ]; then
        PC=$(plat_cur)
        LOWEST=$(printf "%s\n%s\n" "$REQP" "${PC:-0}" | sort -V | head -1)
        if [ "$LOWEST" != "$REQP" ]; then
            log "bundle $TARGET needs platform >= $REQP (have ${PC:-none}) — waiting"
            rm -rf "$DEST"; exit 0
        fi
    fi
    chmod +x "$DEST/nn-camera" "$DEST/cam_run.sh" "$DEST/agent.sh"
    echo "${CURV:-none}" > "$STATE/pending"
    ln -sfn "releases/$TARGET" "$CUR.tmp" && mv -T "$CUR.tmp" "$CUR"
    systemctl restart nn-camera
    log "flipped to $TARGET (pending confirm next tick)"
    ls -1t "$REL" | tail -n +4 | while read -r d; do rm -rf "$REL/$d"; done
    report; exit 0
fi

# ── LAYER 2: platform ──────────────────────────────────────────────────
if [ -f "$STATE/plat-pending" ]; then
    PPREV=$(cat "$STATE/plat-pending"); PCUR=$(plat_cur)
    if platform_health; then
        log "PLATFORM CONFIRMED $PCUR (was $PPREV)"
        rm -f "$STATE/plat-pending" $EDGE/edgeai-rootfs-bad.ext4
    else
        log "platform health FAILED on $PCUR — rolling back to $PPREV"
        echo "$PPREV" > "$JRN"
        stop_stack
        mv $EDGE/edgeai-rootfs.ext4 $EDGE/edgeai-rootfs-bad.ext4
        mv $EDGE/edgeai-rootfs-prev.ext4 $EDGE/edgeai-rootfs.ext4
        rm -f "$JRN"
        start_stack
        echo "$PCUR" > "$STATE/plat-blocked"
        rm -f "$STATE/plat-pending"
    fi
    report; exit 0
fi

PTARGET=$(curl -sf -m 10 "$API/firmware" | python3 -c "
import sys, json
for t in json.load(sys.stdin):
    if t.get('device_type') == '$PTYPE':
        print(t.get('target_version') or t.get('version') or ''); break" 2>/dev/null)
PCUR=$(plat_cur)
if [ -n "$PTARGET" ] && [ -n "$PCUR" ] && [ "$PTARGET" != "$PCUR" ]; then
    if [ -f "$STATE/plat-blocked" ] && [ "$(cat "$STATE/plat-blocked")" = "$PTARGET" ]; then
        log "platform $PTARGET previously failed health — skipping"
    else
        log "platform update $PCUR -> $PTARGET"
        PMETA=$(curl -sf -m 10 "$FW/$PTYPE/$PTARGET/meta") || { log "plat meta failed"; exit 1; }
        PSHA=$(echo "$PMETA" | sed -n 's/.*"sha256"[: ]*"\([^"]*\)".*/\1/p')
        PGZ=$EDGE/edgeai-rootfs-dl.gz
        curl -sf -m 1800 -o "$PGZ" "$FW/$PTYPE/$PTARGET" || { log "plat download failed"; rm -f "$PGZ"; exit 1; }
        [ "$(sha256sum "$PGZ" | cut -d" " -f1)" = "$PSHA" ] || { log "plat sha mismatch"; rm -f "$PGZ"; exit 1; }
        PNEW=$EDGE/edgeai-rootfs-new.ext4
        gunzip -c "$PGZ" > "$PNEW" || { log "gunzip failed"; rm -f "$PGZ" "$PNEW"; exit 1; }
        rm -f "$PGZ"
        # settle 4 GB of writeback before restarting the camera: doing
        # this under memory pressure correlated with intermittent early
        # SIGSEGVs during the 2026-08-14 bring-up.
        sync
        echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
        sleep 5
        cp "$0" /tmp/nn-ota-plat-exec.sh
        exec /bin/bash /tmp/nn-ota-plat-exec.sh __platform_swap "$PTARGET" "$PNEW"
    fi
fi

report
exit 0
