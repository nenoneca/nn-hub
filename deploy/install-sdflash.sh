#!/bin/bash
# Install the whole-disk flash helper for the hub Factory (micro-SD via a
# USB card reader or the board's SD slot).  Run as root ON THE HUB HOST:
#
#   sudo NN_HUB_USER=orangepi deploy/install-sdflash.sh
#
# What it does — and all it does:
#   /usr/local/sbin/nn-sdflash       root:root 0755   (the helper)
#   /etc/sudoers.d/nn-sdflash        root:root 0440   one NOPASSWD line for
#                                                     the hub's service user,
#                                                     that helper only
# No secrets, no network, no config in git beyond these two files.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
HERE=$(cd "$(dirname "$0")" && pwd)
USER_=${NN_HUB_USER:-$(systemctl show nn-hub -p User --value 2>/dev/null || true)}
[ -n "$USER_" ] || { echo "set NN_HUB_USER (the user nn-hub.service runs as)" >&2; exit 1; }
id "$USER_" >/dev/null

install -o root -g root -m 0755 "$HERE/nn-sdflash" /usr/local/sbin/nn-sdflash
tmp=$(mktemp)
sed "s/%NN_HUB_USER%/$USER_/" "$HERE/nn-sdflash.sudoers" > "$tmp"
visudo -cf "$tmp" >/dev/null            # never install a file sudo can't parse
install -o root -g root -m 0440 "$tmp" /etc/sudoers.d/nn-sdflash
rm -f "$tmp"
echo "installed: /usr/local/sbin/nn-sdflash + /etc/sudoers.d/nn-sdflash for $USER_"
sudo -n -u "$USER_" sudo -n /usr/local/sbin/nn-sdflash check /dev/null 2>/dev/null \
    | grep -q '"ok":false' && echo "self-test: helper reachable via sudo, refuses a non-disk (good)"
