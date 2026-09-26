"""Hub-side OT (Thread) network identity.

The hub manages a single Thread network shared by all registered
gateways and devices.  On first gateway provisioning the dataset is
generated automatically (random network key + extpanid + panid; default
channel and name); subsequent provisionings re-use it.

Dataset bytes are stored on disk in hub.db `network` table as canonical
OT operational-dataset TLVs (per Thread 1.x spec).  Same wire format
that `ot dataset active -x` produces, so the bytes can be passed
straight to:

  - the sensor's `nm_thread_apply_dataset(tlvs, len)` (already TLV-aware)
  - the gateway's new `gw_ot_apply_dataset(tlvs, len)` which decomposes
    the TLVs into Spinel SET ops to drive its NCP.
"""

from __future__ import annotations

import dataclasses
import os
import secrets
import struct
import time
from typing import Optional

from .db import DB


# ── TLV types (per Thread 1.x Mesh-Commissioning spec) ───────────────────────

TLV_CHANNEL          = 0
TLV_PANID            = 1
TLV_EXT_PANID        = 2
TLV_NETWORK_NAME     = 3
TLV_PSKC             = 4
TLV_NETWORK_KEY      = 5
TLV_MESH_LOCAL_PREFIX = 7
TLV_SECURITY_POLICY  = 12
TLV_ACTIVE_TIMESTAMP = 14
TLV_CHANNEL_MASK     = 53


# ── defaults ─────────────────────────────────────────────────────────────────

DEFAULT_CHANNEL  = 15        # 2.4 GHz channel — 11..26 valid for 802.15.4
DEFAULT_NAME     = "nn-hub"  # 1..16 ASCII chars
# Mesh-local prefix must be a /64 in fdXX:XXXX:XXXX:XXXX (ULA).  We pick
# `fd00:9a72:6d00:1::/64` by convention; the lower 24 bits of the second
# group are randomised once at hub init so two hubs in the same building
# don't collide.
DEFAULT_MLPREFIX_TEMPLATE = "fd{rand:06x}:6d00:0001::"

# Security Policy TLV body: 2-byte rotation period (hours, BE), 1-byte flags.
# 0x07 = O+N+R: native commissioning + NWK key obtain + routers eligible
DEFAULT_ROTATION_HOURS = 672  # 28 days
DEFAULT_SEC_FLAGS      = 0xFF  # all flags set (Thread 1.3 default, "ORrcCnNbB")

# Channel mask: 32-bit page descriptor + 4-byte channel mask.
# Page 0, channels 11..26 = 0x07FFF800 BE.
DEFAULT_CHANNEL_MASK_PAGE = 0
DEFAULT_CHANNEL_MASK_BITS = 0x07FFF800


# ── dataclass ────────────────────────────────────────────────────────────────


@dataclasses.dataclass
class Network:
    channel:        int
    panid:          int          # 16-bit
    extpanid:       bytes        # 8 bytes
    name:           str          # 1..16 ASCII
    network_key:    bytes        # 16 bytes
    mesh_local_prefix: bytes     # 8 bytes (the /64 prefix)
    pskc:           bytes        # 16 bytes
    rotation_hours: int = DEFAULT_ROTATION_HOURS
    sec_flags:      int = DEFAULT_SEC_FLAGS

    @property
    def extpanid_hex(self) -> str:
        return self.extpanid.hex()

    @property
    def network_key_hex(self) -> str:
        return self.network_key.hex()

    @property
    def mesh_local_prefix_hex(self) -> str:
        return self.mesh_local_prefix.hex()

    @property
    def pskc_hex(self) -> str:
        return self.pskc.hex()


# ── generators ───────────────────────────────────────────────────────────────


def _random_panid() -> int:
    """Random 16-bit PAN ID, avoiding 0x0000 and 0xFFFF (reserved)."""
    while True:
        p = secrets.randbits(16)
        if 0 < p < 0xFFFF:
            return p


def _random_mesh_local_prefix() -> bytes:
    """ULA /64 prefix `fd?? ?? ??: 6d00: 0001:: /64`.  Random middle 24 bits."""
    # 8 bytes total: fd, three random bytes, then 6d 00 00 01.
    return bytes([0xFD]) + secrets.token_bytes(3) + bytes([0x6D, 0x00, 0x00, 0x01])


def generate_network(
    channel: int = DEFAULT_CHANNEL,
    name: str = DEFAULT_NAME,
) -> Network:
    """Generate a fresh Thread network with random secrets."""
    if not (11 <= channel <= 26):
        raise ValueError(f"channel {channel} not in 11..26 (2.4 GHz)")
    if not (1 <= len(name) <= 16):
        raise ValueError(f"name length {len(name)} not in 1..16")
    return Network(
        channel=channel,
        panid=_random_panid(),
        extpanid=secrets.token_bytes(8),
        name=name,
        network_key=secrets.token_bytes(16),
        mesh_local_prefix=_random_mesh_local_prefix(),
        pskc=secrets.token_bytes(16),
    )


# ── TLV encoder ──────────────────────────────────────────────────────────────


def _tlv(t: int, v: bytes) -> bytes:
    """One T-L-V record.  Length is a single byte (max 254 per Thread spec)."""
    if len(v) > 254:
        raise ValueError(f"TLV {t} value too long: {len(v)}")
    return bytes([t, len(v)]) + v


def encode_dataset_tlvs(net: Network) -> bytes:
    """Produce the canonical OT operational-dataset TLV blob.

    Order matches what `ot dataset active -x` emits (see Thread 1.3
    spec §8.10 "Operational Dataset"):
      ActiveTimestamp, Channel, ChannelMask, ExtPanID, MeshLocalPrefix,
      NetworkKey, NetworkName, PanID, PSKc, SecurityPolicy.
    """
    parts: list[bytes] = []

    # Active Timestamp (14): 8 bytes — seconds[48] + ticks[15] + Authoritative[1]
    # Use a coarse epoch-second timestamp; ticks=0; auth=1.
    secs = int(time.time())
    ts = (secs << 16) | 0x0001  # ticks=0, U-bit (authoritative)=1
    parts.append(_tlv(TLV_ACTIVE_TIMESTAMP, ts.to_bytes(8, "big")))

    # Channel (0): page byte + 16-bit channel BE = 3 bytes
    parts.append(_tlv(TLV_CHANNEL,
                      bytes([0]) + net.channel.to_bytes(2, "big")))

    # Channel Mask (53): one entry — 1B page + 1B mask len(=4) + 4B mask BE
    cm_entry = (bytes([DEFAULT_CHANNEL_MASK_PAGE, 4]) +
                DEFAULT_CHANNEL_MASK_BITS.to_bytes(4, "big"))
    parts.append(_tlv(TLV_CHANNEL_MASK, cm_entry))

    # Ext PAN ID (2): 8 bytes
    parts.append(_tlv(TLV_EXT_PANID, net.extpanid))

    # Mesh-Local Prefix (7): 8 bytes
    parts.append(_tlv(TLV_MESH_LOCAL_PREFIX, net.mesh_local_prefix))

    # Network Key (5): 16 bytes
    parts.append(_tlv(TLV_NETWORK_KEY, net.network_key))

    # Network Name (3): UTF-8, no NUL terminator
    parts.append(_tlv(TLV_NETWORK_NAME, net.name.encode("ascii")))

    # PAN ID (1): 2 bytes BE
    parts.append(_tlv(TLV_PANID, net.panid.to_bytes(2, "big")))

    # PSKc (4): 16 bytes
    parts.append(_tlv(TLV_PSKC, net.pskc))

    # Security Policy (12): 2B rotation hours BE + 1B flags
    sp = net.rotation_hours.to_bytes(2, "big") + bytes([net.sec_flags])
    parts.append(_tlv(TLV_SECURITY_POLICY, sp))

    return b"".join(parts)


# ── DB layer ─────────────────────────────────────────────────────────────────


def get_network(db: DB) -> Optional[Network]:
    """Return the active network, or None if not initialised."""
    row = db._conn.execute("SELECT channel, panid, extpanid_hex, "
                           "network_name, network_key_hex, "
                           "mesh_local_prefix_hex, pskc_hex "
                           "FROM network WHERE id = 1").fetchone()
    if not row:
        return None
    return Network(
        channel=row[0],
        panid=row[1],
        extpanid=bytes.fromhex(row[2]),
        name=row[3],
        network_key=bytes.fromhex(row[4]),
        mesh_local_prefix=bytes.fromhex(row[5]),
        pskc=bytes.fromhex(row[6]),
    )


def get_dataset_tlvs(db: DB) -> Optional[bytes]:
    row = db._conn.execute(
        "SELECT dataset_tlvs_hex FROM network WHERE id = 1").fetchone()
    return bytes.fromhex(row[0]) if row else None


def get_or_create_network(db: DB,
                          channel: int = DEFAULT_CHANNEL,
                          name: str = DEFAULT_NAME) -> Network:
    """Return the active network, creating + persisting one if absent."""
    net = get_network(db)
    if net is not None:
        return net
    net = generate_network(channel=channel, name=name)
    tlvs = encode_dataset_tlvs(net)
    db._conn.execute(
        "INSERT INTO network "
        "(id, channel, panid, extpanid_hex, network_name, "
        "network_key_hex, mesh_local_prefix_hex, pskc_hex, "
        "dataset_tlvs_hex, created_at) "
        "VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (net.channel, net.panid, net.extpanid_hex, net.name,
         net.network_key_hex, net.mesh_local_prefix_hex, net.pskc_hex,
         tlvs.hex(), int(time.time())),
    )
    db._conn.commit()
    return net


def reset_network(db: DB,
                  channel: int = DEFAULT_CHANNEL,
                  name: str = DEFAULT_NAME) -> Network:
    """Wipe the existing network and create a fresh one (--force flow)."""
    db._conn.execute("DELETE FROM network")
    db._conn.commit()
    return get_or_create_network(db, channel=channel, name=name)
