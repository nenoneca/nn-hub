#!/usr/bin/env bash
# Media OTA end-to-end runner.
#
# Stages a paired P4+C6 firmware bundle, then drives the split download/swap OTA
# over the encrypted control channel and confirms both chips reach the target.
#
# Prereqs:
#   - Binaries staged under /tmp/media_ota (see build steps in docs/ota_design.md)
#   - C6 provisioned (WiFi + hub control endpoint + keys) — survives the reflash
#     because NVS @0x9000 is preserved (we never erase-flash).
#   - The C6 dials the provisioned control endpoint; run the OTA server there.
#
# Board ports (CH343 UART; verify with udevadm):
#   P4 = /dev/ttyACM10   C6 = /dev/ttyACM11
#
# Usage:
#   media_ota_e2e.sh flash-baseline      # one-time: reflash both -> 0.1.0 OTA layout
#   media_ota_e2e.sh run                 # stage 0.1.1, apply, verify both on target
set -euo pipefail

STAGE=/tmp/media_ota
P4_PORT=${P4_PORT:-/dev/ttyACM10}
C6_PORT=${C6_PORT:-/dev/ttyACM11}
CTRL_PORT=${CTRL_PORT:-8770}
KEYDIR=${KEYDIR:-/tmp/ble_hub_data}
HOST_DIR=$(cd "$(dirname "$0")" && pwd)
IDF=/media/chalos/mx500/nn_project_nowest/media/esp-idf-v6.0.1/export.sh

# NOTE on order: flash P4 FIRST (C6 old fw is steady, not pulsing P4 reset), then
# C6 — the C6's boot power-cycles the P4 into a clean state on the new fw.
# We preserve NVS (no erase-flash) so provisioning/keys survive the relayout.
flash_one() { # <chip> <port> <dir> <appbin> <bl_off>
  local chip=$1 port=$2 dir=$3 app=$4 bl=$5
  echo ">> flashing $chip on $port (preserving NVS), bootloader@$bl ..."
  python -m esptool --chip "$chip" -p "$port" -b 460800 \
    --before default-reset --after hard-reset write-flash \
    "$bl" "$dir/bootloader.bin" \
    0x8000 "$dir/partition-table.bin" \
    0xf000 "$dir/ota_data_initial.bin" \
    0x20000 "$dir/$app"
}

case "${1:-}" in
flash-baseline)
  source "$IDF" >/dev/null 2>&1 || true
  # NOTE: ESP32-P4 2nd-stage bootloader lives at 0x2000; ESP32-C6's at 0x0.
  flash_one esp32p4 "$P4_PORT" "$STAGE/p4_0.1.0" nn-app-media.bin 0x2000
  sleep 3
  flash_one esp32c6 "$C6_PORT" "$STAGE/c6_0.1.0" nn-app-media-network.bin 0x0
  echo ">> baseline 0.1.0 flashed. Watch C6 ($C6_PORT) for link-up + control channel."
  ;;
run)
  echo ">> starting OTA server on :$CTRL_PORT (C6 dials in) ..."
  python3 "$HOST_DIR/media_ota_push.py" \
    --p4-bin "$STAGE/target/nn-app-media_0.1.1.bin" --p4-ver 0.1.1 \
    --c6-bin "$STAGE/target/nn-app-media-network_0.1.1.bin" --c6-ver 0.1.1 \
    --port "$CTRL_PORT" --keydir "$KEYDIR" --apply
  ;;
*)
  echo "usage: $0 {flash-baseline|run}"; exit 1;;
esac
