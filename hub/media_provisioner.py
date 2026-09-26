"""
BLE central provisioner for the media camera networking co-processor (the
ESP32-C6 running nn-app-media-network, advertising as "nn-media-net").

Unlike the OT sensor flow, the media C6 has no Thread dataset: it joins Wi-Fi
and streams H.264 to a video service over TCP.  The provisioning service
(same base UUID e7f0xxxx-6b3e-4f6b-9232-3e26d0d5a2f0) exposes:

  DEVICE_PUBKEY e7f00005  READ          → 32B device X25519 public key
  CONFIG        e7f00004  WRITE         → plaintext config blob (below)
  WIFI          e7f00006  WRITE         → ECIES v2 envelope {ssid,pass}
  STATUS        e7f00003  READ + NOTIFY → 0 IDLE / 1 APPLYING / 2 SUCCESS / 3 ERROR

CONFIG blob (plaintext, version 1):
  u8  ver = 1
  u8  name_len ; name[name_len]
  u8  hub_x25519_pub[32]
  u8  stream_x25519_pub[32]
  u8  hub_host_len ; hub_host ; u16 hub_port        (big-endian)
  u8  stream_host_len ; stream_host ; u16 stream_port (big-endian)

WIFI envelope plaintext (encrypted to the device's X25519 pub with the hub's
long-term X25519 key, ECIES v2 "h2d"):
  u8  ssid_len ; ssid ; u8 pass_len ; pass

The device needs the hub's public key (carried plaintext in CONFIG) before it
can decrypt the WIFI envelope, so CONFIG is always written first.
"""
from __future__ import annotations
import asyncio
import dataclasses
import logging
import os
import struct

if os.environ.get("DBUS_SYSTEM_BUS_ADDRESS") is None \
        and os.path.exists("/opt/host_dbus.sock"):
    os.environ["DBUS_SYSTEM_BUS_ADDRESS"] = "unix:path=/opt/host_dbus.sock"

import json
from bleak import BleakClient, BleakScanner

from . import ble_adapter
from bleak.exc import BleakError

from . import crypto

log = logging.getLogger("hub.media")

PROV_SVC_UUID     = "e7f00001-6b3e-4f6b-9232-3e26d0d5a2f0"
STATUS_CHAR_UUID  = "e7f00003-6b3e-4f6b-9232-3e26d0d5a2f0"
CONFIG_CHAR_UUID  = "e7f00004-6b3e-4f6b-9232-3e26d0d5a2f0"
DEVPUB_CHAR_UUID  = "e7f00005-6b3e-4f6b-9232-3e26d0d5a2f0"
WIFI_CHAR_UUID    = "e7f00006-6b3e-4f6b-9232-3e26d0d5a2f0"

STATUS_NAMES   = {0x00: "IDLE", 0x01: "APPLYING", 0x02: "SUCCESS", 0x03: "ERROR"}
STATUS_SUCCESS = 0x02
STATUS_ERROR   = 0x03


@dataclasses.dataclass
class MediaProvisioningResult:
    ble_addr: str
    device_x25519_pub: bytes   # 32 bytes
    # True when the writes were acknowledged but the device never
    # confirmed: provisioning is UNPROVEN until it registers.
    unconfirmed: bool = False


def _enc_endpoint(host: str, port: int) -> bytes:
    h = host.encode()
    if len(h) > 255:
        raise ValueError("host too long")
    return bytes([len(h)]) + h + struct.pack(">H", port)


def encode_config(name: str,
                  hub_x25519_pub: bytes,
                  stream_x25519_pub: bytes,
                  hub_host: str, hub_port: int,
                  stream_host: str, stream_port: int,
                  gw_blob: bytes | None = None) -> bytes:
    """Build the plaintext CONFIG blob (version 1)."""
    name_b = name.encode()
    if len(name_b) > 255:
        raise ValueError("name too long")
    if len(hub_x25519_pub) != 32 or len(stream_x25519_pub) != 32:
        raise ValueError("pubkeys must be 32 bytes")
    # A blank CONTROL endpoint is the bug that silently kills nn_ctrl: the C6
    # loops in the "not provisioned" branch, never dials, and ALL OTA / config-sync
    # fails while the stream still works.  Refuse to provision it.
    if not hub_host or not (0 < hub_port < 65536):
        raise ValueError(f"control endpoint (hub_host:hub_port) must be set — got "
                         f"{hub_host!r}:{hub_port}; an empty control endpoint leaves "
                         f"nn_ctrl dormant (no OTA, no config-sync).")
    if not stream_host or not (0 < stream_port < 65536):
        raise ValueError(f"stream endpoint must be set — got {stream_host!r}:{stream_port}")
    out = (bytes([1, len(name_b)]) + name_b
           + hub_x25519_pub + stream_x25519_pub
           + _enc_endpoint(hub_host, hub_port)
           + _enc_endpoint(stream_host, stream_port))
    # Trailing TLV 'G' 0x47: gateway identity, stored DORMANT by the
    # device (camera-as-gateway: delivered at provision time even to
    # hardware that cannot use it yet, so enabling later needs no
    # re-provision).  Pre-TLV firmware ignores trailing bytes.
    if gw_blob:
        if len(gw_blob) > 512:
            raise ValueError("gw_blob too large for the CONFIG TLV")
        out += bytes([0x47]) + len(gw_blob).to_bytes(2, "big") + gw_blob
    return out


def encode_wifi_plaintext(ssid: str, password: str) -> bytes:
    s = ssid.encode()
    p = password.encode()
    if len(s) > 255 or len(p) > 255:
        raise ValueError("ssid/password too long")
    return bytes([len(s)]) + s + bytes([len(p)]) + p


async def scan(scan_time: float = 12.0, target_addr: str | None = None,
               adapter: str | None = None) -> list:
    found: dict[str, object] = {}
    devs = await BleakScanner.discover(timeout=scan_time, return_adv=True,
                                       **ble_adapter.kwargs(adapter))
    for addr, (device, adv) in devs.items():
        addr_low = addr.lower()
        if target_addr and addr_low != target_addr.lower():
            continue
        uuids = [str(u).lower() for u in (adv.service_uuids or [])]
        if PROV_SVC_UUID.lower() in uuids:
            log.info("[BLE] Found media node: %s name=%r rssi=%s",
                     addr, device.name, getattr(adv, "rssi", "?"))
            found[addr_low] = device
    return list(found.values())


def exc_text(e: BaseException) -> str:
    """Never an empty string: a bare asyncio.TimeoutError or BleakError
    str()s to "" and once left a provisioning job with error="" (2026-09-14)."""
    msg = str(e).strip()
    return f"{type(e).__name__}: {msg}" if msg else type(e).__name__


async def provision(
    addr: str,
    *,
    name: str,
    ssid: str,
    password: str,
    hub_x25519_priv,                 # cryptography X25519PrivateKey
    stream_host: str, stream_port: int,
    hub_host: str, hub_port: int,
    stream_x25519_pub: bytes | None = None,   # defaults to the hub key
    connect_timeout: float = 15.0,
    prov_timeout: float = 30.0,
    adapter: str | None = None,        # BLE adapter, e.g. "hci1" (None = hub setting)
    gw_blob: bytes | None = None,      # dormant gateway identity TLV payload
) -> MediaProvisioningResult:
    """Connect to *addr* and provision Wi-Fi + endpoints + keys over BLE."""
    status_done = asyncio.Event()
    unconfirmed = [False]
    status_ok = [False]
    saw_applying = [False]

    def _status_notify(_, data: bytearray):
        code = data[0] if data else 0xFF
        log.info("[BLE] %s status → %s", addr, STATUS_NAMES.get(code, hex(code)))
        if code == 0x01:
            saw_applying[0] = True
        elif code == STATUS_SUCCESS:
            status_ok[0] = True
            status_done.set()
        elif code == STATUS_ERROR:
            status_done.set()

    hub_pub = crypto.x25519_pubkey_bytes(hub_x25519_priv)
    if stream_x25519_pub is None:
        stream_x25519_pub = hub_pub   # until the stream service has its own key

    # Where in the exchange the link is, so a drop can be reported with its
    # position ("during connect", "reading device pubkey", ...).  A job once
    # ended with error="" and the hub log stopped at "Connecting to ..."
    # while the board saw the central connect and leave 2.8 s later without
    # a CONFIG write (2026-09-14) — nothing said where or why.
    phase = ["connecting (service discovery)"]

    def _on_disconnect(_client):
        log.warning("[BLE] %s disconnected while %s", addr, phase[0])

    log.info("[BLE] Connecting to %s ...", addr)
    try:
        return await _provision_session(
            BleakClient(addr, timeout=connect_timeout,
                        disconnected_callback=_on_disconnect,
                        **ble_adapter.kwargs(adapter)),
            addr, phase, name=name, ssid=ssid, password=password,
            hub_x25519_priv=hub_x25519_priv, hub_pub=hub_pub,
            stream_x25519_pub=stream_x25519_pub, stream_host=stream_host,
            stream_port=stream_port, hub_host=hub_host, hub_port=hub_port,
            gw_blob=gw_blob, prov_timeout=prov_timeout,
            status_notify=_status_notify, status_done=status_done,
            status_ok=status_ok, saw_applying=saw_applying,
            unconfirmed=unconfirmed)
    except (BleakError, asyncio.TimeoutError, OSError) as e:
        # Link-level failures: say which step lost the link and what the
        # stack reported, instead of surfacing str(e) which may be "".
        log.error("[BLE] %s: link failed while %s: %s", addr, phase[0],
                  exc_text(e))
        raise RuntimeError(f"BLE link to {addr} failed while {phase[0]}: "
                           f"{exc_text(e)}") from e


async def _provision_session(client_cm, addr, phase, *, name, ssid, password,
                             hub_x25519_priv, hub_pub, stream_x25519_pub,
                             stream_host, stream_port, hub_host, hub_port,
                             gw_blob, prov_timeout, status_notify, status_done,
                             status_ok, saw_applying, unconfirmed):
    async with client_cm as client:
        try:
            await client._backend._acquire_mtu()
            log.info("[BLE] Connected  MTU=%s", client.mtu_size)
        except Exception as e:
            log.warning("[BLE] _acquire_mtu raised %s — continuing", e)

        # 1. Read the device's X25519 public key.
        phase[0] = "reading the device pubkey"
        device_pub = bytes(await client.read_gatt_char(DEVPUB_CHAR_UUID))
        if len(device_pub) != 32:
            raise RuntimeError(f"bad device pubkey len {len(device_pub)}")
        log.info("[BLE] Device X25519: %s...", device_pub.hex()[:16])

        # 2. Subscribe to status notifications.
        phase[0] = "subscribing to status"
        await client.start_notify(STATUS_CHAR_UUID, status_notify)

        # 3. Write CONFIG (plaintext) first — gives the device our hub pubkey.
        config = encode_config(name, hub_pub, stream_x25519_pub,
                               hub_host, hub_port, stream_host, stream_port,
                               gw_blob=gw_blob)
        log.info("[BLE] Writing CONFIG (%d bytes)...", len(config))
        phase[0] = "writing CONFIG"
        config_acked = False
        try:
            await client.write_gatt_char(CONFIG_CHAR_UUID, config, response=True)
            config_acked = True
        except BleakError as e:
            # A GATT server can apply the value and still fail to answer:
            # the BlueZ-backed Linux camera (nn-setupd) stores the config
            # -- its log shows "CONFIG applied" -- and then returns ATT
            # 0x0e on the very same write (2026-09-10).  Aborting here
            # discards a device that is in fact configured, and the WIFI
            # write below already tolerates the identical case.  Nothing
            # is assumed from tolerating it: with neither write acked and
            # no status notify, the flow still raises, and success still
            # has to be proven by the device registering with the hub.
            log.warning("[BLE] CONFIG write returned %s -- the device may "
                        "have applied it anyway; continuing to WIFI", e)

        # 4. Write WIFI (ECIES envelope encrypted to the device pubkey).
        envelope = crypto.encrypt(
            plaintext=encode_wifi_plaintext(ssid, password),
            recipient_x25519_pub_bytes=device_pub,
            sender_x25519_priv=hub_x25519_priv,
            direction="h2d",
        )
        wire = json.dumps(envelope, separators=(",", ":")).encode()
        log.info("[BLE] Writing encrypted WIFI (%d byte envelope)...", len(wire))
        phase[0] = "writing WIFI"
        writes_acked = False
        try:
            await client.write_gatt_char(WIFI_CHAR_UUID, wire, response=True)
            writes_acked = True
        except BleakError as e:
            log.warning("[BLE] WIFI write returned %s — maybe RF coex (the C6 "
                        "may have applied + dropped BLE while joining Wi-Fi)", e)

        # 5. Wait for SUCCESS (best-effort: coex may drop BLE on Wi-Fi join).
        phase[0] = "waiting for the status notify"
        try:
            await asyncio.wait_for(status_done.wait(), timeout=prov_timeout)
        except asyncio.TimeoutError:
            if saw_applying[0]:
                log.warning("[BLE] no SUCCESS but saw APPLYING — soft success")
                status_ok[0] = True
            elif writes_acked or config_acked:
                # Firmware notifies SUCCESS *before* it touches Wi-Fi
                # (nn_prov.c: notify, then reboot 1.5 s later), so a
                # missing notify is NOT expected and must not be called
                # success — that would hide a device that rejected the
                # credentials.  Report UNCONFIRMED: the writes landed, the
                # outcome is unknown, and the device registering with the
                # hub is the only thing that settles it.
                log.warning("[BLE] writes acked but no status notify — "
                            "UNCONFIRMED; the device must prove it by "
                            "registering")
                unconfirmed[0] = True
            else:
                raise RuntimeError(
                    f"no status from {addr} and the config/Wi-Fi writes were "
                    f"not acknowledged — the device did not receive them")

        if not status_ok[0] and not unconfirmed[0]:
            raise RuntimeError(f"device {addr} reported provisioning ERROR")

    log.info("[BLE] %s provisioned ✓", addr)
    return MediaProvisioningResult(ble_addr=addr,
                                  device_x25519_pub=device_pub,
                                  unconfirmed=unconfirmed[0])


# ── CLI entrypoint ───────────────────────────────────────────────────────────
# Repeatable media-camera provisioning:  python -m hub.media_provisioner ...
# The CONTROL endpoint (--hub-host/--hub-port) defaults to the beagle media
# control server (:8772) so it is ALWAYS set — the omission that previously
# left nn_ctrl dormant and blocked OTA.  Re-provision flow on the device first:
# console `prov reset` + reboot → C6 advertises BLE → run this.
def _cli() -> int:
    import argparse
    from pathlib import Path

    ap = argparse.ArgumentParser(
        description="Provision a media camera (C6) over BLE: WiFi + control + "
                    "stream endpoints + hub key.")
    ap.add_argument("--name", default="media-1", help="device name (mDNS)")
    ap.add_argument("--ssid", required=True, help="WiFi SSID")
    ap.add_argument("--psk",  required=True, help="WiFi password")
    ap.add_argument("--hub-host",   default=os.environ.get("NN_HUB_HOST"),
                    help="CONTROL endpoint host the C6 dials (nn_ctrl / OTA); "
                         "default: $NN_HUB_HOST")
    ap.add_argument("--hub-port",   type=int, default=8772,
                    help="CONTROL endpoint port (media_ctrl_server)")
    ap.add_argument("--stream-host", default=os.environ.get("NN_STREAM_HOST"),
                    help="video uplink host (nn_netstream); default: $NN_STREAM_HOST, "
                         "else --hub-host")
    ap.add_argument("--stream-port", type=int, default=8888)
    ap.add_argument("--addr", default=None,
                    help="BLE address; if omitted, scans for one media peripheral")
    ap.add_argument("--keydir", default="/var/lib/nn-media/keys",
                    help="hub X25519 key dir (must match media_ctrl_server)")
    ap.add_argument("--scan-time", type=float, default=10.0)
    ap.add_argument("--ble-adapter", default=None,
                    help="local BLE adapter to use, e.g. hci1 (default: the\n"
                         "NN_BLE_ADAPTER env setting, else BlueZ's first). "
                         "Use --list-adapters to see the choices.")
    ap.add_argument("--list-adapters", action="store_true",
                    help="print the local BLE adapters and exit")
    a = ap.parse_args()
    if a.list_adapters:
        print("Local Bluetooth adapters:")
        print(ble_adapter.describe(a.ble_adapter))
        print("\nSelect with --ble-adapter hciN, or set %s=hciN"
              % ble_adapter.ENV_VAR)
        return 0
    # no built-in address: the hub is whatever this deployment says it is
    if not a.hub_host:
        ap.error("--hub-host (or $NN_HUB_HOST) is required: the address the camera dials")
    a.stream_host = a.stream_host or a.hub_host


    logging.basicConfig(level=logging.INFO, format="%(message)s")
    priv = crypto.load_or_generate_enc_key(Path(a.keydir))
    log.info("hub X25519 pub: %s", crypto.x25519_pubkey_bytes(priv).hex())

    async def run():
        addr = a.addr
        if not addr:
            devs = await scan(scan_time=a.scan_time, adapter=a.ble_adapter)
            if not devs:
                raise SystemExit("no media BLE peripherals found (device must be "
                                 "unprovisioned: run `prov reset` on its console + reboot)")
            if len(devs) > 1:
                raise SystemExit("multiple peripherals — pass --addr:\n  " +
                                 "\n  ".join(f"{d.address} {d.name}" for d in devs))
            addr = devs[0].address
            log.info("using %s", addr)
        res = await provision(
            addr, name=a.name, ssid=a.ssid, password=a.psk,
            hub_x25519_priv=priv,
            hub_host=a.hub_host, hub_port=a.hub_port,          # ← control (was missing)
            stream_host=a.stream_host, stream_port=a.stream_port,
            adapter=a.ble_adapter)
        log.info("provisioned: device_pub=%s  control=%s:%d  stream=%s:%d",
                 res.device_x25519_pub.hex()[:16], a.hub_host, a.hub_port,
                 a.stream_host, a.stream_port)

    try:
        asyncio.run(run())
        return 0
    except Exception as e:
        log.error("provisioning failed: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(_cli())
