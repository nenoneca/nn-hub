# nn-hub

Hub for the **nn** Thread/BLE-IoT system.  Manages a single Thread (OT)
network plus the gateways and end-devices that join it.

## What you need

- Linux with a working BLE adapter (BlueZ ≥5.66 recommended).  Tested
  on Debian/Ubuntu.
- Python ≥3.10.
- The user must be in the `bluetooth` group (`sudo usermod -aG
  bluetooth $USER`, then log out / log back in).
- **Disable ModemManager** (or whitelist these devices) — it auto-
  probes serial devices and corrupts our USB-CDC traffic:
    ```sh
    sudo systemctl stop ModemManager
    sudo systemctl disable ModemManager
    ```
  Or, persistent:
    ```sh
    echo 'SUBSYSTEM=="tty", ATTRS{idVendor}=="303a", ATTRS{idProduct}=="1001", ENV{ID_MM_DEVICE_IGNORE}="1"' \
      | sudo tee /etc/udev/rules.d/99-esp-no-mm.rules
    sudo udevadm control --reload-rules && sudo udevadm trigger
    ```

## Install

From a fresh clone:

```sh
git clone <repo> nn_project_nowest
cd nn_project_nowest/hub
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

This puts `nn-hub` on your `$PATH`.  Run `nn-hub --help` to see all
commands.

## Quickstart

The usual onboarding sequence (no firmware flashing — your hardware
arrives with a base image):

```sh
# 1) Power on the gateway.  It advertises BLE service e7f01001-… until
#    provisioned.
nn-hub gateway new --ssid YOUR_WIFI_SSID --psk YOUR_WIFI_PSK \
                   --hub-host <hub-address>

# Hub auto-generates the Thread network on the first call, encrypts the
# dataset, and pushes it to the gateway.  After ~13 s the gateway joins
# WiFi, applies the OT dataset to its NCP, brings Thread up as Leader,
# and reports back to the hub over TCP.

# 2) Inspect:
nn-hub network show
nn-hub gateway list

# 3) Power on a sensor.  Provision it; sensor auto-joins the same
#    Thread network.
nn-hub device new --name sensor-1 --type sample_c6

# 4) Inspect:
nn-hub device list
nn-hub network show     # now shows the device under the network

# 5) Optional — add a second gateway.  It joins as Router on the same
#    Thread network and helps backhaul packets to the hub.
nn-hub gateway new --ssid SAME_WIFI --psk SAME_PSK \
                   --hub-host <hub-address>
```

Run `nn-hub serve` (in a separate terminal) for the long-running
hub server (CoAP + nn_proto-TCP + firmware-HTTP).

## Re-flashing firmware (if needed)

Hardware ships with a base firmware that supports BLE provisioning and
OTA out of the box.  If you need to (re-)flash from your dev PC:

```sh
nn-hub firmware list                    # show currently registered images
nn-hub firmware upload sample_c6 0.2.0 build/sensor.signed.bin
                                        # upload + set as target version;
                                        # devices pull on next OTA check
```

For the rare case where a device's NVS is corrupt and you need to fully
re-flash the base image via USB, see `docs/factory.md`.

## Files

- `hub.db` — SQLite — gateways, devices, configs, telemetry, network.
- `proto_p256_priv.bin` — hub's nn_proto signing key (ECDSA P-256 with
  RFC 6979 deterministic-k on the device side).
- `firmware/` — registered firmware images, fetched by gateways/devices
  during OTA.

All under `~/.nn-hub/` by default (override with `nn-hub --data-dir`).
