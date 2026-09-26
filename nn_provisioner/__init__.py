"""nn-provisioner — standalone BLE provisioner for nn-hub devices.

This package decouples device/gateway BLE provisioning from the hub
process so the hub can live anywhere (cloud, PC, RPi, Docker) while a
small CLI tool — running on a host that has both a BLE radio and LAN
access to the hub — performs the on-prem onboarding.

Why split it out:
  * The end-user's WiFi PSK never has to flow through the hub.
  * The hub doesn't need BlueZ access, so it runs in containers, on
    headless servers, and behind cloud load-balancers.
  * Multiple sites with one shared hub each get their own
    `nn-provisioner` running on whatever box happens to be near the
    sensors.

The CLI entry point is `nn-provisioner`; see `cli.py` for usage.
"""

__version__ = "0.1.0"
