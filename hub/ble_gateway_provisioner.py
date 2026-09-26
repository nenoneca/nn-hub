"""
BLE central provisioner — pushes WiFi + hub credentials to a fresh
gateway over BLE.

Service e7f01001-6b3e-4f6b-9232-3e26d0d5a2f0:
  WIFI_CRED    e7f01002  WRITE   [1B ssid_len][ssid][1B psk_len][psk]
  HUB_HOST     e7f01003  WRITE   [1B host_len][host]
  HUB_IDENTITY e7f01004  WRITE   [8B hub_id][65B hub_p256_pub]
  GW_INFO      e7f01005  READ    [1B schema=2][8B gw_id][65B p256_pub][32B x25519_pub]
  STATUS       e7f01006  READ|NOTIFY  see _STATUS_*
  COMMIT       e7f01007  WRITE   [1B] 0x01 apply, 0x00 clear staged

Flow
----
1. Scan for devices advertising the service UUID (or use --addr).
2. Connect.
3. Read GW_INFO → derive gateway_id + pubkey for hub-DB registration.
4. Write WIFI_CRED, expect STATUS notify 0x10 (STAGED_WIFI).
5. Write HUB_HOST,  expect STATUS notify 0x11 (STAGED_HUB_HOST).
6. Write HUB_IDENTITY, expect STATUS notify 0x12 (STAGED_HUB_ID).
7. Write COMMIT 0x01.  Status sequence: 0x01 APPLYING → 0x02 SUCCESS,
   then gateway reboots ~1 s later (we expect a disconnect).
8. Return ProvisioningResult(gateway_id, pubkey_b64).
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import logging
import os
from pathlib import Path
from typing import Optional

from bleak import BleakClient, BleakScanner

from . import ble_adapter
from bleak.exc import BleakError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from . import crypto  # shared X25519 ECIES (also used by sensors)

log = logging.getLogger("hub.ble_gw")

# ── UUIDs ────────────────────────────────────────────────────────────────
GW_PROV_SVC      = "e7f01001-6b3e-4f6b-9232-3e26d0d5a2f0"
GW_WIFI_CRED     = "e7f01002-6b3e-4f6b-9232-3e26d0d5a2f0"
GW_HUB_HOST      = "e7f01003-6b3e-4f6b-9232-3e26d0d5a2f0"
GW_HUB_IDENTITY  = "e7f01004-6b3e-4f6b-9232-3e26d0d5a2f0"
GW_INFO          = "e7f01005-6b3e-4f6b-9232-3e26d0d5a2f0"
GW_STATUS        = "e7f01006-6b3e-4f6b-9232-3e26d0d5a2f0"
GW_COMMIT        = "e7f01007-6b3e-4f6b-9232-3e26d0d5a2f0"
GW_HUB_X25519    = "e7f01008-6b3e-4f6b-9232-3e26d0d5a2f0"  # M6 unified bootstrap
GW_OT_DATASET    = "e7f01009-6b3e-4f6b-9232-3e26d0d5a2f0"  # Phase 1: hub-pushed OT dataset

# Status byte values (must stay in sync with gw_ble_provision.c)
_STATUS_IDLE              = 0x00
_STATUS_APPLYING          = 0x01
_STATUS_SUCCESS           = 0x02
_STATUS_ERROR_VALIDATION  = 0x03
_STATUS_ERROR_INCOMPLETE  = 0x04
_STATUS_ERROR_NVS         = 0x05
_STATUS_STAGED_WIFI       = 0x10
_STATUS_STAGED_HUB_HOST   = 0x11
_STATUS_STAGED_HUB_ID     = 0x12
_STATUS_STAGED_OT_DATASET = 0x13

# Limits (must stay in sync with gw_provision.h)
SSID_MAX     = 32
PSK_MAX      = 64
HUB_HOST_MAX = 64
HUB_ID_LEN   = 8
HUB_PUB_LEN  = 65   # P-256 uncompressed (nn_proto signing pub)


@dataclasses.dataclass
class ProvisioningResult:
    """Result of a successful BLE provisioning."""
    gateway_id:      str   # hex (8 bytes → 16 chars)
    gateway_pubkey:  bytes # 65 bytes uncompressed P-256
    address:         str   # BLE MAC of the provisioned device

    @property
    def pubkey_b64(self) -> str:
        import base64
        return base64.b64encode(self.gateway_pubkey).decode()


class BleGatewayProvisionError(Exception):
    """Raised on any provisioning failure."""


# ── hub identity loader ─────────────────────────────────────────────────

def load_hub_identity(privkey_path: Optional[Path] = None) -> tuple[bytes, bytes]:
    """Load the hub's nn_proto P-256 keypair from disk.  Returns
    (hub_id_8B, hub_pubkey_65B_uncompressed). """
    path = privkey_path or (Path.home() / ".nn-hub" / "proto_p256_priv.bin")
    raw = path.read_bytes()
    if len(raw) != 32:
        raise BleGatewayProvisionError(
            f"{path}: bad private-key length {len(raw)} (expected 32)")
    priv = ec.derive_private_key(int.from_bytes(raw, "big"), ec.SECP256R1())
    pub65 = priv.public_key().public_bytes(
        encoding=serialization.Encoding.X962,
        format=serialization.PublicFormat.UncompressedPoint)
    if len(pub65) != HUB_PUB_LEN or pub65[0] != 0x04:
        raise BleGatewayProvisionError(
            f"unexpected pubkey shape: len={len(pub65)} prefix={pub65[:1].hex()}")
    hub_id = hashlib.sha256(pub65).digest()[:HUB_ID_LEN]
    return hub_id, pub65


# ── scan ────────────────────────────────────────────────────────────────

async def find_gateway(scan_time: float = 10.0,
                       address: Optional[str] = None,
                       adapter: Optional[str] = None) -> Optional[str]:
    """Scan for a gateway advertising GW_PROV_SVC.  Returns the
    BLE address of the first match, or `address` if explicitly provided."""
    if address:
        log.info("BLE: using explicit address %s (no scan)", address)
        return address

    log.info("BLE: scanning for gateway service %s (%.0fs)...",
             GW_PROV_SVC, scan_time)
    devices = await BleakScanner.discover(timeout=scan_time, return_adv=True,
                                          **ble_adapter.kwargs(adapter))
    for addr, (_d, adv) in devices.items():
        uuids = [u.lower() for u in (adv.service_uuids or [])]
        if GW_PROV_SVC in uuids:
            log.info("BLE: found gateway at %s (name=%r)", addr, _d.name)
            return addr
    return None


# ── ECIES encryptor (uses the shared hub_crypto X25519 path) ─────────────

def _encrypt_for_gateway(plaintext: bytes,
                         gw_x25519_pub: bytes,
                         hub_x25519_priv) -> bytes:
    """
    Encrypt `plaintext` for the gateway using the same X25519 ECIES v2
    path that hub↔device messaging uses.  Produces a NUL-terminated
    JSON string suitable for direct GATT write; the device-side
    `hub_crypto_decrypt` parses it back.
    """
    import json
    env = crypto.encrypt(
        plaintext=plaintext,
        recipient_x25519_pub_bytes=gw_x25519_pub,
        sender_x25519_priv=hub_x25519_priv,
        direction="h2d",
    )
    return json.dumps(env, separators=(",", ":")).encode()


# ── BlueZ helpers ───────────────────────────────────────────────────────

def _bluez_remove_device(addr: str) -> None:
    """Best-effort: drop any stale BlueZ pairing for `addr` so a fresh
    SMP exchange runs.  Our gateway has CONFIG_BT_BONDABLE=n — it
    forgets the LTK across connections, so a BlueZ-cached bond against
    a device that has lost its LTK causes encrypted attribute access to
    fail with 'ATT Insufficient Authentication' / 'Unlikely Error'.
    """
    import shutil
    import subprocess
    bctl = shutil.which("bluetoothctl")
    if not bctl:
        log.debug("bluetoothctl not found — skipping stale-bond cleanup")
        return
    try:
        subprocess.run(
            [bctl, "remove", addr],
            check=False,
            capture_output=True,
            timeout=5,
        )
        log.debug("BlueZ: removed any cached bond for %s", addr)
    except (subprocess.SubprocessError, OSError) as e:
        log.debug("BlueZ remove failed (%s) — continuing", e)


# ── core flow ───────────────────────────────────────────────────────────

async def _wait_for_status(events: asyncio.Queue,
                           expected: int,
                           label: str,
                           timeout: float = 15.0) -> None:
    try:
        st = await asyncio.wait_for(events.get(), timeout=timeout)
    except asyncio.TimeoutError:
        raise BleGatewayProvisionError(
            f"timed out waiting for STATUS=0x{expected:02x} ({label})")
    if st != expected:
        raise BleGatewayProvisionError(
            f"expected STATUS=0x{expected:02x} ({label}), got 0x{st:02x}")


async def provision_gateway(
        ssid: str,
        psk: str,
        hub_host: str,
        hub_x25519_priv,                 # cryptography X25519PrivateKey
        ot_dataset_tlvs: bytes,          # Thread §8.10 TLV blob, ≤254 B
        *,
        address: Optional[str] = None,
        scan_time: float = 10.0,
        connect_timeout: float = 15.0,
        privkey_path: Optional[Path] = None,
        adapter: Optional[str] = None,   # BLE adapter, e.g. "hci1"
) -> ProvisioningResult:
    """Provision a fresh gateway over BLE.  Raises BleGatewayProvisionError
    on any failure; returns a ProvisioningResult on success.

    Per-step pre-conditions (validated on the device side; this client
    also checks before sending so we get cleaner local error messages):
      - len(ssid)   in 1..SSID_MAX
      - len(psk)    in 0..PSK_MAX
      - len(hub_host) in 1..HUB_HOST_MAX
    """
    # ── validate ────────────────────────────────────────────────────────
    if not (0 < len(ssid) <= SSID_MAX):
        raise BleGatewayProvisionError(f"ssid len {len(ssid)} not in 1..{SSID_MAX}")
    if not (0 <= len(psk) <= PSK_MAX):
        raise BleGatewayProvisionError(f"psk len {len(psk)} not in 0..{PSK_MAX}")
    if not (0 < len(hub_host) <= HUB_HOST_MAX):
        raise BleGatewayProvisionError(
            f"hub_host len {len(hub_host)} not in 1..{HUB_HOST_MAX}")

    hub_id, hub_pub = load_hub_identity(privkey_path)

    # ── scan ────────────────────────────────────────────────────────────
    addr = await find_gateway(scan_time=scan_time, address=address,
                              adapter=adapter)
    if not addr:
        raise BleGatewayProvisionError(
            "no gateway found advertising " + GW_PROV_SVC)

    # ── connect + write ────────────────────────────────────────────────
    status_events: asyncio.Queue = asyncio.Queue()

    def on_status(_handle, data: bytearray) -> None:
        if not data:
            return
        log.debug("STATUS notify: 0x%02x", data[0])
        status_events.put_nowait(data[0])

    log.info("BLE: connecting to %s...", addr)
    async with BleakClient(addr, timeout=connect_timeout,
                           **ble_adapter.kwargs(adapter)) as client:
        # Acquire MTU FIRST, before start_notify — BlueZ refuses
        # `AcquireWrite` once a notify is open ("Notify acquired") on
        # the same connection.
        try:
            backend = client._backend
            await backend._acquire_mtu()
            log.info("BLE: ATT MTU acquired: %s", client.mtu_size)
        except Exception as e:
            log.warning("BLE: _acquire_mtu raised %s — continuing", e)

        await client.start_notify(GW_STATUS, on_status)

        info = await client.read_gatt_char(GW_INFO)
        # GW_INFO schema 2: [1B schema=2][8B gw_id][65B p256_pub][32B x25519_pub]
        EXPECTED_LEN = 1 + HUB_ID_LEN + HUB_PUB_LEN + 32
        if len(info) != EXPECTED_LEN:
            raise BleGatewayProvisionError(
                f"GW_INFO bad length {len(info)} (expected {EXPECTED_LEN})")
        if info[0] != 2:
            raise BleGatewayProvisionError(
                f"GW_INFO schema {info[0]} (expected 2 — gateway "
                f"firmware predates M6 unification?)")
        gw_id          = bytes(info[1:1 + HUB_ID_LEN])
        gw_p256_pub    = bytes(info[1 + HUB_ID_LEN:1 + HUB_ID_LEN + HUB_PUB_LEN])
        gw_x25519_pub  = bytes(info[1 + HUB_ID_LEN + HUB_PUB_LEN:])
        if gw_p256_pub[0] != 0x04:
            raise BleGatewayProvisionError(
                f"GW_INFO p256 prefix 0x{gw_p256_pub[0]:02x} (expected 0x04)")
        log.info("BLE: gateway id=%s  x25519=%s...",
                 gw_id.hex(), gw_x25519_pub.hex()[:16])

        # M6 unified: bootstrap step — write the hub's static X25519 pub
        # PLAINTEXT.  Device's hub_crypto needs both its own static priv
        # and the hub's static pub for static-static DH.  Subsequent
        # WIFI/HUB_HOST/HUB_IDENTITY writes are JSON ECIES envelopes,
        # same path used by sensor provisioning.
        hub_x25519_pub = crypto.x25519_pubkey_bytes(hub_x25519_priv)
        await client.write_gatt_char(GW_HUB_X25519, hub_x25519_pub,
                                     response=True)
        log.info("BLE: hub X25519 pub written (%d bytes)",
                 len(hub_x25519_pub))

        # WIFI_CRED  — encrypted JSON envelope
        wifi_pt = (bytes([len(ssid)]) + ssid.encode() +
                   bytes([len(psk)])  + psk.encode())
        wifi_env = _encrypt_for_gateway(wifi_pt, gw_x25519_pub,
                                        hub_x25519_priv)
        await client.write_gatt_char(GW_WIFI_CRED, wifi_env, response=True)
        await _wait_for_status(status_events, _STATUS_STAGED_WIFI,
                               "STAGED_WIFI")

        # HUB_HOST
        host_pt = bytes([len(hub_host)]) + hub_host.encode()
        host_env = _encrypt_for_gateway(host_pt, gw_x25519_pub,
                                        hub_x25519_priv)
        await client.write_gatt_char(GW_HUB_HOST, host_env, response=True)
        await _wait_for_status(status_events, _STATUS_STAGED_HUB_HOST,
                               "STAGED_HUB_HOST")

        # HUB_IDENTITY  — carries hub_id + hub's nn_proto P-256 pub for
        # signature verification (different from the X25519 used above).
        ident_pt = hub_id + hub_pub
        ident_env = _encrypt_for_gateway(ident_pt, gw_x25519_pub,
                                         hub_x25519_priv)
        await client.write_gatt_char(GW_HUB_IDENTITY, ident_env, response=True)
        await _wait_for_status(status_events, _STATUS_STAGED_HUB_ID,
                               "STAGED_HUB_ID")

        # OT_DATASET  — Phase 1: hub pushes the active Thread network
        # identity (TLV blob, ≤254 B).  Encrypted because the network
        # key is in there.
        if not ot_dataset_tlvs or len(ot_dataset_tlvs) > 254:
            raise BleGatewayProvisionError(
                f"ot_dataset_tlvs len {len(ot_dataset_tlvs)} not in 1..254")
        ds_env = _encrypt_for_gateway(ot_dataset_tlvs, gw_x25519_pub,
                                      hub_x25519_priv)
        log.info("BLE: writing OT_DATASET (%d B plain → %d B envelope)",
                 len(ot_dataset_tlvs), len(ds_env))
        await client.write_gatt_char(GW_OT_DATASET, ds_env, response=False)
        await _wait_for_status(status_events, _STATUS_STAGED_OT_DATASET,
                               "STAGED_OT_DATASET")

        # COMMIT
        try:
            await client.write_gatt_char(GW_COMMIT, b"\x01", response=True)
        except BleakError as e:
            # Gateway may have rebooted after the SUCCESS notify, racing
            # the ATT response.  We treat this as soft-success only IF
            # we already saw APPLYING/SUCCESS.
            log.debug("COMMIT write raised %s — checking status queue", e)

        # Wait for APPLYING then SUCCESS.
        await _wait_for_status(status_events, _STATUS_APPLYING, "APPLYING",
                               timeout=8.0)
        st = None
        try:
            st = await asyncio.wait_for(status_events.get(), timeout=10.0)
        except asyncio.TimeoutError:
            pass
        if st != _STATUS_SUCCESS:
            # Gateway may have already rebooted — try one more time to
            # see if the disconnect arrives quickly (good signal).
            await asyncio.sleep(2.0)
            if not client.is_connected:
                log.info("BLE: client disconnected after APPLYING — "
                         "treating as soft-success (gateway rebooted)")
            else:
                raise BleGatewayProvisionError(
                    f"COMMIT did not reach SUCCESS (last status: "
                    f"0x{st:02x})" if st is not None else
                    "COMMIT did not reach SUCCESS (no further status)")
        else:
            log.info("BLE: STATUS=SUCCESS (gateway will reboot)")

    return ProvisioningResult(
        gateway_id=gw_id.hex(),
        gateway_pubkey=gw_p256_pub,   # nn_proto signing pubkey (P-256)
        address=addr,
    )
