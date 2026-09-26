"""nn_proto — wire format for hub ↔ gateway ↔ device.

See docs/protocol/nn_proto.md for the full spec.

Frame layout (little-endian throughout):

  | magic[2]=0x4E 0x4E | type[2] | pkt_size[4] | device_id_size[2] |
  | device_id[N] | payload[M] | sig[64] |

  pkt_size = bytes from device_id_size through end of sig
  sig      = ECDSA P-256 (secp256r1) raw R || S over SHA-256 of
             [magic .. payload].  64 bytes total.  Embedded sign side
             uses RFC 6979 deterministic-k derivation; verify side
             accepts any valid (R, S) so signatures from random-k
             signers (the hub) interoperate.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
    encode_dss_signature,
)


# ── constants ────────────────────────────────────────────────────────────────

MAGIC = b"\x4e\x4e"  # 'NN'
HEADER_FIXED = 10
SIG_LEN = 64           # ECDSA P-256 raw R||S (32 + 32 BE)
PUBKEY_LEN = 65        # uncompressed: 0x04 || X[32] || Y[32]
PRIV_LEN = 32          # P-256 scalar
OVERHEAD = HEADER_FIXED + SIG_LEN
CURVE = ec.SECP256R1()


class FrameType(IntEnum):
    D2H = 0x0000
    H2D = 0x0001
    D2G = 0x0002
    G2D = 0x0003
    D2D = 0x0004
    # hub -> gateway command, handled by the gateway (not forwarded); signed
    # by the hub; inner [cmd:2][tid:4][u64 epoch ms][body]; reply is D2G
    # [cmd+1][tid:4][status:i8][body]
    H2G = 0x0005


class Cmd(IntEnum):
    """Inner cmd codes for D2G/G2D payloads."""
    RESERVED            = 0x0000
    HUB_STATUS_QUERY    = 0x0001  # D2G, no args
    HUB_STATUS_ANNOUNCE = 0x0002  # G2D, 1B online flag
    GATEWAY_HELLO       = 0x0003  # G2D mcast, 16B addr + 2B interval + 1B online
    GATEWAY_THREAD_STATE = 0x0004 # D2G, 1B role + 2B rloc16 LE + 16B mleid
    DEVICE_HEARTBEAT     = 0x0005 # D2H, 4B uptime_ms LE
    DEVICE_THREAD_STATE  = 0x0006 # D2G, 2B did_size LE + did + 16B device ml_eid
    # 0x0010..0x001F — Phase 6 hub↔device R/W relay.
    # Inner payload: [cmd:2 LE | tid:4 LE | ECIES envelope JSON]
    FIELD_OP        = 0x0010  # H2D — get/set request, opaque envelope
    FIELD_REPLY     = 0x0011  # D2H — paired reply, same tid
    FIELD_REPLY_ACK = 0x0012  # H2D — hub acks receipt of FIELD_REPLY so
                              #       sensor's reliable-send slot retires.
                              #       Same tid as the FIELD_REPLY.  Body empty.

    # 0x0020..0x002F — sensor↔hub control plane (replaces legacy CoAP
    # paths: time/log/auto/info/commission/ota).  Same inner shape as
    # FIELD_OP: [cmd:2 LE | tid:4 LE | body...].  Reply matched by tid.
    TIME_QUERY     = 0x0020  # D2H — body empty
    TIME_REPLY     = 0x0021  # H2D — body: u64 LE epoch ms
    LOG_LINE       = 0x0022  # D2H — body: utf-8 text (no reply)
    AUTO_EVENT     = 0x0023  # D2H — body: JSON event (no reply)
    INFO_QUERY     = 0x0024  # H2D — body empty
    INFO_REPLY     = 0x0025  # D2H — body: JSON
    AUTO_PUSH      = 0x0026  # H2D — body: ECIES envelope
    AUTO_ACK       = 0x0027  # D2H — body: 1B status
    COMMISSION_ADD = 0x0028  # H2D — body: ECIES envelope
    COMMISSION_ACK = 0x0029  # D2H — body: 1B status
    OTA_CHECK      = 0x002A  # D2H — body: JSON {"type":..,"version":..}
    OTA_MANIFEST   = 0x002B  # H2D — body: JSON manifest
    OTA_BLOCK_REQ  = 0x002C  # D2H — body: u32 LE block_num + u16 LE size
    OTA_BLOCK      = 0x002D  # H2D — body: u32 LE block_num + raw bytes
    OTA_HINT       = 0x002E  # H2D — body empty; tells sensor to run check+download+apply
    OTA_HINT_ACK   = 0x002F  # D2H — body: 1B status (0=accepted, errno on reject)
    # 0x0030/0x0031 are D2D AUTO_NOTIFY / AUTO_NOTIFY_ACK (device-side only).
    OTA_CHUNKSUMS_REQ = 0x0032  # D2H — body: u32 LE first_block + u16 LE count
    OTA_CHUNKSUMS     = 0x0033  # H2D — body: u32 LE first_block + n×8B trunc-sha256
    OTA_READY         = 0x0034  # D2H — body: JSON {"version":..,"sha256":..} — armed, awaiting apply
    OTA_APPLY         = 0x0035  # H2D — body empty; device runs request_upgrade + reboot
    OTA_PATCH_REQ     = 0x0036  # D2H — body: u32 LE offset + u16 LE size (detools patch bytes)
    OTA_PATCH         = 0x0037  # H2D — body: u32 LE offset + raw patch bytes
    SESS_INIT         = 0x0038  # H2D — body: 8B hub session salt (Phase 2)
    SESS_PROBE        = 0x0039  # D2H — body: nn_session sealed probe (verify)
    FIELD_OP_S        = 0x003A  # H2D — session-sealed field op (zero frame sig)
    FIELD_REPLY_S     = 0x003B  # D2H — session-sealed field reply (zero frame sig)
    SESS_HELLO        = 0x003C  # D2H — 8B device session salt (small-frame carrier)
    SESS_PROBE_ACK    = 0x003D  # H2D — empty; acks a verified SESS_PROBE
    SESS_GROUP_KEY    = 0x003E  # H2D — session-sealed [epoch:4 LE][key:32] (Phase 4)
    # 0x003F is D2D AUTO_NOTIFY_S (group-keyed cascade, device-side only)
    AUTO_EVENT_ACK    = 0x0040  # H2D — empty; acks a reliable AUTO_EVENT
    REBOOT            = 0x0041  # H2D — body empty; device ACKs then warm-reboots
                                #       ~500 ms later.  Same trust surface as
                                #       OTA_APPLY, which already reboots devices.
    REBOOT_ACK        = 0x0042  # D2H — 1B status (0=rebooting)
    CLEAR_USER_DATA     = 0x0043  # H2D — session-SEALED [01 01]: arm
                                  #       clear-on-next-boot.  Idempotent —
                                  #       retries are safe (unlike REBOOT).
    CLEAR_USER_DATA_ACK = 0x0044  # D2H — session-SEALED [status:1]
    RADIO_STATS         = 0x0045  # D2H — unsigned, tid 0: cumulative radio/mesh
                                  #       counters (hub/radio_health.py, v1 59 B)
    CHANNEL_SCAN_REQ    = 0x0046  # H2D — u16 LE ms/channel: device energy-scans 11..26
    CHANNEL_SCAN_REPLY  = 0x0047  # D2H — same tid: [status:i8][channel:u8][16 x i8 dBm]
    GW_CHANNEL_SCAN        = 0x0050  # H2G — u16 LE ms/channel
    GW_CHANNEL_SCAN_RESULT = 0x0051  # D2G — [status][channel][16 x i8 dBm]
    GW_CHANNEL_SET         = 0x0052  # H2G — u8 channel + u16 LE delay s (MGMT_PENDING_SET)
    GW_CHANNEL_SET_RESULT  = 0x0053  # D2G — [status]
    GW_DATASET_GET         = 0x0054  # H2G — empty
    GW_DATASET             = 0x0055  # D2G — [status][active dataset TLVs]
    GW_H2D_UNDELIVERABLE   = 0x0056  # D2G — [reason:i8][did_size:u16][did][cmd:u16][tid:u32]
                                     #       the gateway could not forward an H2D frame
    GW_ROUTE_SET           = 0x0058  # H2G — [did_size:u16][did][16 B mesh addr]
    GW_ROUTE_SET_RESULT    = 0x0059  # D2G — [status]
    # 0x8000+ reserved for application-defined extensions.


# ── exceptions ───────────────────────────────────────────────────────────────


class ProtoError(ValueError):
    """Base class for nn_proto parsing/encoding errors."""


class TruncatedFrame(ProtoError):
    """Buffer is shorter than the declared pkt_size."""


class BadMagic(ProtoError):
    """Frame doesn't start with the 'NN' magic."""


class BadSignature(ProtoError):
    """ECDSA signature didn't verify."""


# ── DSS ⇄ raw R||S helpers ──────────────────────────────────────────────────


def _dss_to_raw(dss_sig: bytes) -> bytes:
    """ASN.1 DER (cryptography's default) → 64-byte raw R||S."""
    r, s = decode_dss_signature(dss_sig)
    return r.to_bytes(32, "big") + s.to_bytes(32, "big")


def _raw_to_dss(raw_sig: bytes) -> bytes:
    """64-byte raw R||S → ASN.1 DER (what cryptography's verify expects)."""
    if len(raw_sig) != SIG_LEN:
        raise ProtoError(f"raw sig must be {SIG_LEN} bytes, got {len(raw_sig)}")
    r = int.from_bytes(raw_sig[:32], "big")
    s = int.from_bytes(raw_sig[32:], "big")
    return encode_dss_signature(r, s)


def pubkey_to_uncompressed(pub: ec.EllipticCurvePublicKey) -> bytes:
    """Serialize a P-256 public key as 65-byte uncompressed: 0x04 || X || Y."""
    nums = pub.public_numbers()
    return b"\x04" + nums.x.to_bytes(32, "big") + nums.y.to_bytes(32, "big")


def pubkey_from_uncompressed(blob: bytes) -> ec.EllipticCurvePublicKey:
    """Parse a 65-byte uncompressed P-256 public key."""
    if len(blob) != PUBKEY_LEN or blob[0] != 0x04:
        raise ProtoError(
            f"expected 65-byte uncompressed pubkey starting with 0x04, "
            f"got len={len(blob)} first={blob[:1]!r}"
        )
    x = int.from_bytes(blob[1:33], "big")
    y = int.from_bytes(blob[33:65], "big")
    return ec.EllipticCurvePublicNumbers(x, y, CURVE).public_key()


# ── frame ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Frame:
    """A parsed frame.  Use encode()/decode() to convert to/from bytes."""

    type: int
    device_id: bytes
    payload: bytes
    sig: bytes

    @property
    def device_id_size(self) -> int:
        return len(self.device_id)

    @property
    def pkt_size(self) -> int:
        return 2 + len(self.device_id) + len(self.payload) + SIG_LEN


def frame_size(device_id_size: int, payload_size: int) -> int:
    """Total bytes on the wire for these field sizes."""
    return HEADER_FIXED + device_id_size + payload_size + SIG_LEN


# ── encode / decode ─────────────────────────────────────────────────────────


def _signed_blob(type_: int, device_id: bytes, payload: bytes) -> bytes:
    """Bytes the ECDSA signature covers."""
    pkt_size = 2 + len(device_id) + len(payload) + SIG_LEN
    return (
        MAGIC
        + struct.pack("<HIH", type_, pkt_size, len(device_id))
        + device_id
        + payload
    )


def encode(
    type_: int,
    device_id: bytes,
    payload: bytes,
    signing_key: ec.EllipticCurvePrivateKey,
) -> bytes:
    """Encode a frame, signing it with the given P-256 private key.

    The signature is ECDSA over SHA-256 of the framed bytes (magic..payload),
    serialized as raw R||S big-endian (64 bytes).
    """
    if len(device_id) > 0xFFFF:
        raise ProtoError(f"device_id too long: {len(device_id)} (max 65535)")
    if not isinstance(type_, int) or type_ < 0 or type_ > 0xFFFF:
        raise ProtoError(f"type out of range: {type_!r}")
    if not isinstance(signing_key.curve, ec.SECP256R1):
        raise ProtoError(f"signing key must be P-256, got {signing_key.curve!r}")

    blob = _signed_blob(type_, device_id, payload)
    dss = signing_key.sign(blob, ec.ECDSA(hashes.SHA256()))
    raw = _dss_to_raw(dss)
    if len(raw) != SIG_LEN:
        raise ProtoError(f"unexpected raw sig length: {len(raw)}")
    return blob + raw


def decode(buf: bytes) -> tuple[Frame, int]:
    """Parse one frame from `buf`.  Returns (Frame, bytes_consumed).

    Does NOT verify the signature.  Call verify_sig() separately.
    Raises BadMagic, TruncatedFrame, or ProtoError on malformed input.
    """
    if len(buf) < HEADER_FIXED:
        raise TruncatedFrame(
            f"need ≥{HEADER_FIXED} bytes for header, got {len(buf)}"
        )
    if buf[:2] != MAGIC:
        raise BadMagic(f"bad magic: {buf[:2]!r}")

    type_, pkt_size, device_id_size = struct.unpack("<HIH", buf[2:10])

    min_pkt_size = 2 + device_id_size + SIG_LEN
    if pkt_size < min_pkt_size:
        raise ProtoError(
            f"pkt_size {pkt_size} < minimum {min_pkt_size} "
            f"for device_id_size={device_id_size}"
        )

    total_frame_len = 8 + pkt_size
    if len(buf) < total_frame_len:
        raise TruncatedFrame(
            f"need {total_frame_len} bytes for frame, got {len(buf)}"
        )

    payload_size = pkt_size - 2 - device_id_size - SIG_LEN
    device_id = buf[10 : 10 + device_id_size]
    payload   = buf[10 + device_id_size : 10 + device_id_size + payload_size]
    sig       = buf[total_frame_len - SIG_LEN : total_frame_len]

    return (
        Frame(type=type_, device_id=device_id, payload=payload, sig=sig),
        total_frame_len,
    )


def verify_sig(frame: Frame, pubkey: ec.EllipticCurvePublicKey) -> None:
    """Raise BadSignature if the frame's ECDSA signature doesn't verify."""
    blob = _signed_blob(frame.type, frame.device_id, frame.payload)
    dss = _raw_to_dss(frame.sig)
    try:
        pubkey.verify(dss, blob, ec.ECDSA(hashes.SHA256()))
    except InvalidSignature as e:
        raise BadSignature(str(e)) from e


def parse_stream(buf: bytes):
    """Iterate frames from a TCP-style byte stream.

    Yields Frame objects.  Stops on truncated trailing frame
    (caller can append more bytes and re-call).
    """
    pos = 0
    while pos < len(buf):
        try:
            frame, consumed = decode(buf[pos:])
        except TruncatedFrame:
            return
        yield frame
        pos += consumed


# ── inner-cmd helpers (D2G/G2D payloads) ────────────────────────────────────


def encode_inner(cmd: int, args: bytes = b"") -> bytes:
    """Build the inner payload for a D2G/G2D frame."""
    return struct.pack("<H", cmd) + args


def decode_inner(payload: bytes) -> tuple[int, bytes]:
    """Split a D2G/G2D inner payload into (cmd, args)."""
    if len(payload) < 2:
        raise ProtoError(f"inner payload too short: {len(payload)}")
    cmd = struct.unpack("<H", payload[:2])[0]
    return cmd, payload[2:]
