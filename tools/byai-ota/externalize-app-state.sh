#!/bin/bash
# Move the app layer OUT of the platform image onto the host, bind-mounted
# into the container — so platform swaps never touch app state.
set -e
CFG=/var/lib/lxc/edgeai/config

systemctl stop nn-ota.timer nn-camera
systemctl stop nn-npu 2>/dev/null || true
lxc-stop -n edgeai -t 15 2>/dev/null || true
mountpoint -q /opt/edgeai/root || mount -o loop /opt/edgeai/edgeai-rootfs.ext4 /opt/edgeai/root

mkdir -p /opt/nn-app
rsync -a /opt/edgeai/root/opt/nn/ /opt/nn-app/
rm -rf /opt/edgeai/root/opt/nn
mkdir -p /opt/edgeai/root/opt/nn      # empty mountpoint stays in the image
echo "state moved: $(du -sh /opt/nn-app | cut -f1)"

grep -q "opt/nn-app" "$CFG" || \
  echo "lxc.mount.entry = /opt/nn-app opt/nn none bind,create=dir 0 0" >> "$CFG"
echo "bind entry present: $(grep -c nn-app "$CFG")"

# fixed layer now points at the HOST copy of the agent
sed -i 's#/opt/edgeai/root/opt/nn/current/agent.sh#/opt/nn-app/current/agent.sh#' /usr/local/sbin/nn-ota-launcher
grep agent /usr/local/sbin/nn-ota-launcher | head -1

systemctl start nn-npu
sleep 8
lxc-attach -n edgeai -- ls /opt/nn/current/manifest.json >/dev/null && echo "BIND-OK"
systemctl start nn-camera
sleep 22
systemctl is-active nn-camera
systemctl start nn-ota.timer
echo EXTERNALIZE-DONE
