"""
BLE central provisioner — extends the existing OT provisioning protocol
with hub-specific characteristics.

Existing OT Provisioning Service (e7f00001-...):
  DATASET_CHAR   e7f00002  READ|WRITE  — OT dataset TLV bytes
  STATUS_CHAR    e7f00003  READ|NOTIFY — 0x00 IDLE, 0x01 APPLYING,
                                         0x02 SUCCESS, 0x03 ERROR

New Hub Provisioning characteristics (same service, new UUIDs):
  HUB_CONFIG_CHAR     e7f00004  WRITE  — [1B name_len][name][32B X25519]
  DEVICE_PUBKEYS_CHAR e7f00005  READ   — [32B X25519]  (32 bytes fixed)

Provisioning flow
-----------------
1. Scan for devices advertising PROV_SVC_UUID
2. Connect + MTU exchange
3. Discover service + characteristics
4. Read DEVICE_PUBKEYS_CHAR (64 B)  → device key material
5. Write DATASET_CHAR (OT dataset bytes)
6. Write HUB_CONFIG_CHAR (hub config blob)
7. Wait for STATUS_CHAR notify == 0x02 SUCCESS
8. Disconnect
9. Return ProvisioningResult

Device-side implementation notes
---------------------------------
The Zephyr device firmware must be updated to expose the two new
characteristics.  The corresponding NVS keys are:
  "prov/dev_x25519"   (32 bytes) — X25519 public key
  "prov/hub_x25519"   (32 bytes) — stored from HUB_CONFIG_CHAR
  "prov/name"         (≤32 bytes) — stored from HUB_CONFIG_CHAR

The device generates its X25519 key pair on first boot via Zephyr PSA crypto:
  - X25519:  psa_generate_key(PSA_ALG_ECDH, PSA_ECC_FAMILY_MONTGOMERY, 255-bit)
"""

from __future__ import annotations
import asyncio
import dataclasses
import logging
import os

# When running inside the nn-hub LXC container the host's BlueZ daemon
# is reached over a bind-mounted DBus socket at /opt/host_dbus.sock
# (see /var/lib/lxc/nn-hub/config).  The systemd unit sets
# DBUS_SYSTEM_BUS_ADDRESS so the long-running server points bleak at
# the right socket, but `lxc-attach -- nn-hub …` and other one-shot
# CLI invocations bypass that environment.  Detect the socket and set
# the address ourselves before bleak does its first DBus connection.
if os.environ.get("DBUS_SYSTEM_BUS_ADDRESS") is None \
        and os.path.exists("/opt/host_dbus.sock"):
    os.environ["DBUS_SYSTEM_BUS_ADDRESS"] = "unix:path=/opt/host_dbus.sock"

from bleak import BleakClient, BleakScanner

from . import ble_adapter
from bleak.exc import BleakError

from .crypto import decode_device_pubkeys, x25519_pubkey_bytes

log = logging.getLogger("hub.ble")

# ── UUIDs ────────────────────────────────────────────────────────────────────

PROV_SVC_UUID           = "e7f00001-6b3e-4f6b-9232-3e26d0d5a2f0"
DATASET_CHAR_UUID       = "e7f00002-6b3e-4f6b-9232-3e26d0d5a2f0"
STATUS_CHAR_UUID        = "e7f00003-6b3e-4f6b-9232-3e26d0d5a2f0"
HUB_CONFIG_CHAR_UUID    = "e7f00004-6b3e-4f6b-9232-3e26d0d5a2f0"
DEVICE_PUBKEYS_CHAR_UUID = "e7f00005-6b3e-4f6b-9232-3e26d0d5a2f0"

STATUS_NAMES   = {0x00: "IDLE", 0x01: "APPLYING", 0x02: "SUCCESS", 0x03: "ERROR"}
STATUS_SUCCESS = 0x02
STATUS_ERROR   = 0x03


@dataclasses.dataclass
class ProvisioningResult:
    ble_addr: str
    device_x25519_pub: bytes    # 32 bytes
    has_hub_chars: bool         # False if device runs old firmware (no hub chars)


# ── Scanning ─────────────────────────────────────────────────────────────────

async def scan(scan_time: float = 12.0,
               target_addr: str | None = None,
               adapter: str | None = None) -> list:
    """
    Scan for devices advertising the OT Provisioning service.
    If *target_addr* is given, return as soon as that address is seen.

    Uses BleakScanner.discover() instead of detection_callback because
    BlueZ does duplicate filtering and the callback path silently
    drops adverts for any device it has already seen recently — a
    fresh-but-recently-probed peripheral comes back empty.
    """
    found: dict[str, object] = {}

    devs = await BleakScanner.discover(timeout=scan_time, return_adv=True,
                                       **ble_adapter.kwargs(adapter))
    for addr, (device, adv) in devs.items():
        addr_low = addr.lower()
        if target_addr and addr_low != target_addr.lower():
            continue
        uuids = [str(u).lower() for u in (adv.service_uuids or [])]
        if PROV_SVC_UUID.lower() in uuids:
            rssi = getattr(adv, "rssi", "?")
            log.info("[BLE] Found: %s  name=%r  rssi=%s",
                     addr, device.name, rssi)
            found[addr_low] = device

    return list(found.values())


# ── Provisioning session ──────────────────────────────────────────────────────

async def provision(
    addr: str,
    dataset_bytes: bytes,
    hub_config_bytes: bytes,
    hub_x25519_priv=None,   # cryptography.X25519PrivateKey, required for M6
    connect_timeout: float = 15.0,
    prov_timeout: float = 30.0,
    adapter: str | None = None,        # BLE adapter, e.g. "hci1" (None = hub setting)
) -> ProvisioningResult:
    """
    Connect to *addr*, provision it with *dataset_bytes* and *hub_config_bytes*,
    wait for SUCCESS, and return the device's public keys.

    *hub_config_bytes*: output of crypto.encode_hub_config()
    *hub_x25519_priv*: hub's long-term X25519 private key (cryptography
        X25519PrivateKey).  Required when the device firmware exposes
        DEVICE_PUBKEYS_CHAR — we encrypt the OT dataset to the device's
        X25519 pub before writing DATASET_CHAR (M6).  If None or the
        device runs old firmware without DEVICE_PUBKEYS, we fall back to
        plaintext (M5 behaviour) for backwards compat.
    """
    status_done = asyncio.Event()
    writes_acked = [False]
    status_ok   = [False]
    saw_applying = [False]   # device acknowledged + started apply work

    def _status_notify(_, data: bytearray):
        code = data[0] if data else 0xFF
        name = STATUS_NAMES.get(code, f"0x{code:02x}")
        log.info("[BLE] %s  status → %s", addr, name)
        if code == 0x01:  # APPLYING
            saw_applying[0] = True
        if code == STATUS_SUCCESS:
            status_ok[0] = True
            status_done.set()
        elif code == STATUS_ERROR:
            status_done.set()

    log.info("[BLE] Connecting to %s ...", addr)
    try:
        async with BleakClient(addr, timeout=connect_timeout,
                               **ble_adapter.kwargs(adapter)) as client:
            # Acquire MTU BEFORE start_notify — BlueZ refuses
            # AcquireWrite once a notify is open ("Notify acquired").
            # M6 envelope writes (~200B) need MTU > 23 to avoid Write
            # Long fragmentation.
            try:
                await client._backend._acquire_mtu()
                log.info("[BLE] Connected  MTU=%s", client.mtu_size)
            except Exception as e:
                log.warning("[BLE] _acquire_mtu raised %s — continuing", e)
            # Trigger service discovery
            try:
                _ = client.services
            except Exception:
                pass

            # Discover which characteristics are present
            svc = next(
                (s for s in client.services
                 if s.uuid.lower() == PROV_SVC_UUID.lower()),
                None,
            )
            if svc is None:
                raise RuntimeError(f"OT Provisioning service not found on {addr}")

            char_uuids = {c.uuid.lower() for c in svc.characteristics}
            has_hub = DEVICE_PUBKEYS_CHAR_UUID.lower() in char_uuids

            # ── Read device pubkeys (new chars only) ──────────────────────
            device_x_pub = b"\x00" * 32

            if has_hub:
                log.info("[BLE] Reading device pubkeys...")
                raw = bytes(await client.read_gatt_char(DEVICE_PUBKEYS_CHAR_UUID))
                (device_x_pub,) = decode_device_pubkeys(raw)
                log.info("[BLE] Device X25519: %s...", device_x_pub.hex()[:16])
            else:
                log.warning("[BLE] Device does not expose hub provisioning chars "
                            "(old firmware) — skipping key exchange")

            # ── Subscribe to status notifications ─────────────────────────
            await client.start_notify(STATUS_CHAR_UUID, _status_notify)
            log.info("[BLE] Subscribed to status notifications")

            # ── Write hub config FIRST (before dataset) ─────────────────────
            # Order matters on ESP32-C6: applying the Thread dataset re-enables
            # the 802.15.4 PHY which is *the same radio* as the BLE PHY, so
            # BLE drops mid-apply.  If we write the dataset first, the hub
            # config write that comes next races the disconnect and usually
            # fails.  Writing hub config first guarantees both writes land.
            if has_hub and HUB_CONFIG_CHAR_UUID.lower() in char_uuids:
                log.info("[BLE] Writing hub config (%d bytes)...",
                         len(hub_config_bytes))
                await client.write_gatt_char(
                    HUB_CONFIG_CHAR_UUID, hub_config_bytes, response=True
                )
                log.info("[BLE] Hub config write acknowledged")

            # ── Write OT dataset ──────────────────────────────────────────
            # M6: when the device supports the hub-config protocol, encrypt
            # the dataset (which contains the Thread network key) using
            # ECIES before sending.  HUB_CONFIG was just written above with
            # our X25519 pubkey, so the device already has everything it
            # needs to derive the shared key.  Without this, the network
            # key would travel as plaintext over BLE — anyone in radio
            # range during the brief provisioning window could capture it.
            if has_hub and hub_x25519_priv is not None:
                from . import crypto
                envelope = crypto.encrypt(
                    plaintext=dataset_bytes,
                    recipient_x25519_pub_bytes=device_x_pub,
                    sender_x25519_priv=hub_x25519_priv,
                    direction="h2d",
                )
                # Device side parses NUL-terminated JSON via Zephyr's
                # json_obj_parse — no whitespace, no trailing newline.
                import json
                wire = json.dumps(envelope, separators=(",", ":")).encode()
                log.info("[BLE] Writing encrypted OT dataset (%d bytes plain "
                         "→ %d bytes envelope)...",
                         len(dataset_bytes), len(wire))
                try:
                    await client.write_gatt_char(
                        DATASET_CHAR_UUID, wire, response=True
                    )
                    log.info("[BLE] Encrypted dataset write acknowledged")
                    writes_acked[0] = True
                except BleakError as e:
                    log.warning("[BLE] dataset write returned %s — likely RF "
                                "coex (device may have applied successfully)", e)
            else:
                log.warning("[BLE] Sending dataset PLAINTEXT (legacy device "
                            "without DEVICE_PUBKEYS or no hub key supplied)")
                log.info("[BLE] Writing OT dataset (%d bytes)...",
                         len(dataset_bytes))
                try:
                    await client.write_gatt_char(
                        DATASET_CHAR_UUID, dataset_bytes, response=True
                    )
                    log.info("[BLE] OT dataset write acknowledged")
                except BleakError as e:
                    log.warning("[BLE] dataset write returned %s — likely RF "
                                "coex (device may have applied successfully)", e)

            # ── Wait for provisioning result (best-effort) ─────────────────
            # SUCCESS notification may not reach us if BLE drops mid-apply.
            # If we saw APPLYING, that's enough — caller verifies via Thread
            # state (mDNS/CoAP) post-flight.
            log.info("[BLE] Waiting for provisioning result (%.0fs)...",
                     prov_timeout)
            try:
                await asyncio.wait_for(status_done.wait(), timeout=prov_timeout)
            except asyncio.TimeoutError:
                if saw_applying[0]:
                    log.warning("[BLE] Timed out waiting for SUCCESS, but device "
                                "fired APPLYING — treating as soft success "
                                "(verify via mDNS/CoAP post-flight)")
                    status_ok[0] = True
                elif writes_acked[0]:
                    # Both writes were ATT-acknowledged and then every
                    # status notify was lost — measured live 2026-08-20:
                    # the device applied the dataset, attached to the
                    # mesh, and the hub had declared failure and never
                    # registered it.  With acked writes the mesh arrival
                    # check (post-flight) is the true verdict, so treat
                    # this as soft success rather than stranding a
                    # provisioned-but-unregistered device.
                    log.warning("[BLE] status notifies lost after acked "
                                "writes — soft success (verify by arrival)")
                    status_ok[0] = True
                else:
                    raise RuntimeError(
                        f"Timed out waiting for status from {addr} "
                        f"(no APPLYING seen — device may not have received dataset)"
                    )

            if not status_ok[0]:
                raise RuntimeError(f"Device {addr} reported provisioning ERROR")

            try:
                await client.stop_notify(STATUS_CHAR_UUID)
            except BleakError:
                pass  # connection may already be torn down — fine

    except (BleakError, OSError) as exc:
        # Connection errors after a successful APPLYING notify are expected
        # (RF coex tears BLE down).  Only re-raise if no APPLYING was seen.
        if saw_applying[0]:
            log.warning("[BLE] post-APPLYING transport error (%s) — treating as "
                        "soft success", exc)
        else:
            raise RuntimeError(f"BLE error with {addr}: {exc}") from exc
    except asyncio.TimeoutError as exc:
        raise RuntimeError(f"BLE error with {addr}: {exc}") from exc

    log.info("[BLE] %s provisioned successfully ✓", addr)
    return ProvisioningResult(
        ble_addr=addr,
        device_x25519_pub=device_x_pub,
        has_hub_chars=has_hub,
    )
