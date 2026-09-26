# Raspberry Pi Imager Spec — `nn-hub` + `nn-gw` Bootable Image

Goal: hand a non-technical operator a single `.img` file they flash to an
SD card with the standard Raspberry Pi Imager, boot the Pi, and have a
working hub + gateway pair on the LAN with no further setup.

This is a **spec only** — implementation is future work.

## Hardware

| Item | Notes |
|---|---|
| Raspberry Pi 4B (2 GB RAM minimum) | Reference target; Pi 3B+ may work but is untested |
| ESP32-C6 NCP board (UART-attached) | Provides the 802.15.4 radio for the gateway |
| MicroSD card, 16 GB+ | Holds OS + both LXC rootfs |
| Optional: BLE-capable USB dongle | Pi 4B already has BLE; only needed for Pi Compute Modules without onboard radio |

## Image contents

```
boot/
  config.txt                 # enable UART4 (the NCP link), disable Bluetooth-on-UART0
  cmdline.txt                # standard Pi boot
firstboot/
  generate-host-keys.sh      # regenerate SSH host keys on first boot
  randomize-hostname.sh      # randomize hostname suffix so multiple Pis on the
                             # same LAN don't collide on `nn-hub.local`
  install-tls-cert.sh        # generate self-signed cert for the REST API
opt/
  nn-hub/                    # nn-hub venv pre-installed
    venv/
    src/
  nn-gw/                     # gw_linux binary + supporting scripts
    bin/gw_linux
    lxc/
etc/systemd/system/
  nn-hub.service             # already enabled
  nn-gw.service              # already enabled
  nn-mdns-publish.service    # advertises _nn-hub._tcp + _nn-gw._tcp
var/lib/lxc/
  nn-hub/                    # pre-built rootfs
  nn-gw/                     # pre-built rootfs
```

## First-boot sequence

1. `cloud-init`-style firstboot scripts run once:
   - Regenerate SSH host keys
   - Randomize hostname (`nn-hub-XXXX` where XXXX = last 4 of MAC)
   - Generate hub identity (P-256 + X25519) → `/var/lib/nn-hub/identity.bin`
   - Initialize empty SQLite DB at `/var/lib/nn-hub/devices.sqlite`
2. Start `nn-hub.service` (listens on `:8767` proto, `:8769` REST, `:8770` firmware HTTP)
3. Start `nn-gw.service` (waits for `nn-hub` on `127.0.0.1:8767`, then
   runs `gw_linux provision-net` if no gateway identity is registered,
   else `gw_linux operate /dev/ttyAMA3`)
4. `nn-mdns-publish.service` advertises:
   - `_nn-hub._tcp.local` → `:8769` for the REST API
   - `_nn-gw._tcp.local` → `:8770` for the network-transport provisioning
5. LED indicator pattern (via GPIO PWM) → green pulse when API up, red on failure

## Out-of-the-box operator flow

1. Flash SD with Pi Imager (point at the `.img`).
2. Insert SD into Pi 4B with NCP board attached to UART4.
3. Power on Pi.
4. Operator's laptop or phone (on same LAN):
   - Discovers hub via mDNS → `nn-hub-XXXX.local:8769`
   - `POST /api/v1/network/init` to mint a fresh Thread network (or skip
     — gateway provisioning will mint one on demand)
   - `POST /api/v1/gateways/new {ssid, psk, hub_host: "127.0.0.1", transport: "ble"}`
     (or use `transport: "net"` since the gateway is on the same LAN and
     advertises `_nn-gw._tcp.local`)
   - Provision sensors via the standalone `nn-provisioner` tool (see #3)
     pointing at this hub's API URL.

## Build pipeline

A separate `scripts/build-rpi-image.sh` would:

1. Start from a Raspberry Pi OS Lite (64-bit) base image.
2. `chroot` in and:
   - `apt install lxc python3-pip avahi-daemon` etc.
   - Bootstrap nn-hub: `pip install /opt/nn-hub`
   - Build the gw_linux binary for `aarch64`
   - Build the two LXC rootfs via the existing `host/gw_linux/lxc/build-rootfs.sh`
   - Install systemd units + firstboot scripts
3. Repack the image, shrink, compress to `.img.xz`.
4. Publish to GitHub Releases tied to nn-hub version tags.

Estimated build time: ~30 min on a beefy host. Output image: ~2 GB
compressed.

## Open design questions (to resolve before implementation)

- **Hub identity uniqueness**: every flashed image gets the same hub
  privkey unless firstboot regenerates it. Must regenerate, but then how
  does the operator find the hub's new pubkey to pin? Either
  - print it on Pi UART/LED on first boot, or
  - expose unauthenticated `GET /api/v1/hub/identity` (the same endpoint
    proposed for the standalone provisioner — see `nn-provisioner` spec).
- **Default Thread network**: ship blank (operator runs network/init) or
  ship with a pre-minted demo network (faster to "it works" but every
  shipped Pi shares the same network creds out of the box, which is a
  security smell).
- **Firmware updates of the hub itself**: do we ship an `nn-hub
  self-update` REST endpoint that pulls from GitHub Releases? Or do we
  expect operators to re-flash the SD?

## Not in scope for v1

- Cloud hub variant (the same Python package runs anywhere; image is the
  Pi convenience layer)
- Multi-gateway Pi (current image supports one NCP UART only)
- Touchscreen UI (would be a separate package)
