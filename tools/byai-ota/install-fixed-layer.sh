#!/bin/bash
# Install the immutable OTA layer on a BeagleY camera host (run as root).
# Idempotent.  The updatable layers (bundle, platform) arrive via OTA.
set -e
H=$(dirname "$0")
install -m755 "$H/nn-ota-launcher" /usr/local/sbin/nn-ota-launcher
install -m644 "$H/nn-ota.service" /etc/systemd/system/nn-ota.service
install -m644 "$H/nn-ota.timer" /etc/systemd/system/nn-ota.timer
mkdir -p /etc/systemd/system/nn-camera.service.d
install -m644 "$H/nn-camera-core.conf" /etc/systemd/system/nn-camera.service.d/core.conf
# core dumps land in the host-bound app log dir (survives platform swaps)
sysctl -w kernel.core_pattern=/opt/nn/log/core.%e.%p
grep -q core_pattern /etc/sysctl.d/90-nn-ota.conf 2>/dev/null || \
  echo "kernel.core_pattern=/opt/nn/log/core.%e.%p" > /etc/sysctl.d/90-nn-ota.conf
systemctl daemon-reload
systemctl enable --now nn-ota.timer
echo "fixed layer installed"
