#!/bin/bash
# One-command release for cam3 artifacts.
#
#   release.sh bundle   <version>   [--board user@camera-board]
#   release.sh platform <version>   [--src /path/to/edgeai-orig.ext4]
#
# bundle:   packs {nn-camera, cam_run.sh, agent.sh, manifest} from the
#           board's CURRENT release + the kit's agent.sh, publishes to
#           the hub's local firmware source, syncs and promotes.
# platform: builds a slim container image from the SDK original on the
#           HUB HOST (never ship GBs from the board — its WiFi upload is
#           broken), publishes and promotes.  The camera pulls either
#           artifact within its 10-min tick and self-confirms.
#
# Requires: sshpass, and hosts + credentials supplied by the ENVIRONMENT.
#
# Nothing here is defaulted: this script used to carry the bench passwords
# and addresses as literals, which put real credentials in a git history
# that no later edit can unpublish.  Keep them in an untracked env file:
#
#   install -d -m 700 ~/.config/nn
#   cat > ~/.config/nn/release.env <<'EOF'
#   export HUB=10.0.0.5                 # hub address for the REST API
#   export HUB_SSH=user@hub-host        # ssh target for the hub host
#   export HUB_PASS=...                 # hub ssh password
#   export BOARD=user@camera-board      # ssh target for the camera board
#   export BOARD_PASS=...               # camera board ssh password
#   EOF
#   chmod 600 ~/.config/nn/release.env
#
# Override the location with NN_RELEASE_ENV.
set -euo pipefail

_env_file=${NN_RELEASE_ENV:-$HOME/.config/nn/release.env}
# shellcheck source=/dev/null
[ -r "$_env_file" ] && . "$_env_file"

need() {  # need <VAR> <what it is>
  if [ -z "${!1:-}" ]; then
    echo "release.sh: \$$1 is not set — $2." >&2
    echo "  Export it, or put it in $_env_file (see the header)." >&2
    exit 2
  fi
}
need HUB        "hub address for the REST API"
need HUB_SSH    "ssh target for the hub host"
need HUB_PASS   "hub ssh password"
need BOARD      "ssh target for the camera board"
need BOARD_PASS "camera board ssh password"
API="http://$HUB:8769/api/v1"
HERE=$(cd "$(dirname "$0")" && pwd)

kind=${1:?bundle|platform}; ver=${2:?version}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

publish() {  # $1=device_type $2=version $3=artifact $4=note
  local sha size
  sha=$(sha256sum "$3" | cut -d' ' -f1); size=$(stat -c%s "$3")
  sshpass -p "$HUB_PASS" scp -q "$3" "$HUB_SSH:/tmp/nn-rel-artifact"
  sshpass -p "$HUB_PASS" ssh "$HUB_SSH" "
    mkdir -p ~/.nn-hub/firmware-local/$1/$2
    mv /tmp/nn-rel-artifact ~/.nn-hub/firmware-local/$1/$2/image.signed.bin
    printf '{\n \"schema\": 1,\n \"device_type\": \"%s\",\n \"version\": \"%s\",\n \"format\": \"tar.gz\",\n \"sha256\": \"%s\",\n \"size_bytes\": %s,\n \"mcuboot\": {},\n \"build_meta\": {\"note\": \"%s\"}\n}\n' \
      '$1' '$2' '$sha' '$size' '$4' > ~/.nn-hub/firmware-local/$1/$2/manifest.json"
  curl -sf -X POST "$API/firmware/sources/bench-local/sync" >/dev/null
  curl -sf -X POST "$API/firmware/catalog/$1/$2/promote" \
       -H 'Content-Type: application/json' -d '{}' >/dev/null
  echo "published + promoted $1/$2 (sha ${sha:0:12}…)"
}

case "$kind" in
bundle)
  # pull the currently-running binary + run script off the board
  sshpass -p "$BOARD_PASS" ssh "$BOARD" \
    "echo $BOARD_PASS | sudo -S tar -C /opt/nn-app/current -cf /tmp/nn-cur.tar nn-camera cam_run.sh librproc_byname.so" >/dev/null 2>&1
  sshpass -p "$BOARD_PASS" scp -q "$BOARD:/tmp/nn-cur.tar" "$work/"
  tar -C "$work" -xf "$work/nn-cur.tar"
  cp "$HERE/agent.sh" "$work/agent.sh"
  printf '{\n "device_type": "byai_camera",\n "version": "%s",\n "format": "tar.gz",\n "requires_platform": "1.0.0"\n}\n' "$ver" > "$work/manifest.json"
  chmod +x "$work/nn-camera" "$work/cam_run.sh" "$work/agent.sh" "$work/librproc_byname.so"
  # librproc_byname.so MUST ship: cam_run.sh LD_PRELOADs it, and a bundle
  # without it silently drops the remoteproc-by-name protection (ld.so
  # ignores a missing preload — found live on 0.1.8).
  tar -czf "$work/bundle.tgz" -C "$work" nn-camera cam_run.sh agent.sh manifest.json librproc_byname.so
  publish byai_camera "$ver" "$work/bundle.tgz" "release.sh bundle"
  ;;
platform)
  src=${4:-/tmp/edgeai-orig.ext4}   # SDK original, already on the hub host
  sshpass -p "$HUB_PASS" scp -q "$HERE/build-platform-image.sh" "$HUB_SSH:/tmp/"
  sshpass -p "$HUB_PASS" ssh "$HUB_SSH" \
    "echo $HUB_PASS | sudo -S env VER=$ver SRC=$src bash /tmp/build-platform-image.sh" 2>&1 | tail -3
  sshpass -p "$HUB_PASS" ssh "$HUB_SSH" "cp /tmp/plat-$ver.gz /tmp/nn-plat-artifact"
  # already on hub host — publish in place
  sha=$(sshpass -p "$HUB_PASS" ssh "$HUB_SSH" "sha256sum /tmp/plat-$ver.gz | cut -d' ' -f1")
  size=$(sshpass -p "$HUB_PASS" ssh "$HUB_SSH" "stat -c%s /tmp/plat-$ver.gz")
  sshpass -p "$HUB_PASS" ssh "$HUB_SSH" "
    mkdir -p ~/.nn-hub/firmware-local/byai_platform/$ver
    cp /tmp/plat-$ver.gz ~/.nn-hub/firmware-local/byai_platform/$ver/image.signed.bin
    printf '{\n \"schema\": 1,\n \"device_type\": \"byai_platform\",\n \"version\": \"%s\",\n \"format\": \"tar.gz\",\n \"sha256\": \"%s\",\n \"size_bytes\": %s,\n \"mcuboot\": {},\n \"build_meta\": {\"note\": \"release.sh platform\"}\n}\n' \
      '$ver' '$sha' '$size' > ~/.nn-hub/firmware-local/byai_platform/$ver/manifest.json"
  curl -sf -X POST "$API/firmware/sources/bench-local/sync" >/dev/null
  curl -sf -X POST "$API/firmware/catalog/byai_platform/$ver/promote" \
       -H 'Content-Type: application/json' -d '{}' >/dev/null
  echo "published + promoted byai_platform/$ver"
  ;;
*) echo "usage: release.sh bundle|platform <version>"; exit 2 ;;
esac
