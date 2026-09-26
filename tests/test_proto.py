"""Round-trip + golden-vector tests for hub.proto."""

from __future__ import annotations

import os
import struct

import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from hub.proto import (
    CURVE,
    MAGIC,
    OVERHEAD,
    PUBKEY_LEN,
    SIG_LEN,
    BadMagic,
    BadSignature,
    Cmd,
    Frame,
    FrameType,
    ProtoError,
    TruncatedFrame,
    decode,
    decode_inner,
    encode,
    encode_inner,
    frame_size,
    pubkey_from_uncompressed,
    pubkey_to_uncompressed,
    verify_sig,
)


@pytest.fixture
def signing_key() -> ec.EllipticCurvePrivateKey:
    """Deterministic P-256 keypair from a fixed scalar.  The pubkey is
    therefore stable across runs even though ECDSA signatures are not.
    """
    # Fixed private scalar within the P-256 group order.
    priv_int = 0x42424242_42424242_42424242_42424242_42424242_42424242_42424242_42424242
    return ec.derive_private_key(priv_int, CURVE)


@pytest.fixture
def random_key() -> ec.EllipticCurvePrivateKey:
    return ec.generate_private_key(CURVE)


# ── basic round-trip ────────────────────────────────────────────────────────


def test_round_trip_empty_payload(random_key):
    device_id = b"deadbeef"
    raw = encode(FrameType.D2H, device_id, b"", random_key)
    assert len(raw) == frame_size(len(device_id), 0)

    frame, consumed = decode(raw)
    assert consumed == len(raw)
    assert frame.type == FrameType.D2H
    assert frame.device_id == device_id
    assert frame.payload == b""
    assert len(frame.sig) == SIG_LEN
    verify_sig(frame, random_key.public_key())


def test_round_trip_with_payload(random_key):
    device_id = b"x" * 16
    payload = os.urandom(123)
    raw = encode(FrameType.H2D, device_id, payload, random_key)

    frame, consumed = decode(raw)
    assert consumed == len(raw)
    assert frame.type == FrameType.H2D
    assert frame.device_id == device_id
    assert frame.payload == payload
    verify_sig(frame, random_key.public_key())


def test_round_trip_empty_device_id(random_key):
    """Multicast frames carry no device_id."""
    payload = b"\x01\x00hello"
    raw = encode(FrameType.G2D, b"", payload, random_key)

    frame, consumed = decode(raw)
    assert frame.device_id == b""
    assert frame.payload == payload
    verify_sig(frame, random_key.public_key())


@pytest.mark.parametrize("did_size", [0, 1, 8, 64, 255, 1024])
@pytest.mark.parametrize("payload_size", [0, 1, 64, 256, 1024])
def test_round_trip_size_matrix(random_key, did_size, payload_size):
    device_id = os.urandom(did_size)
    payload = os.urandom(payload_size)
    raw = encode(FrameType.D2H, device_id, payload, random_key)
    assert len(raw) == frame_size(did_size, payload_size)

    frame, consumed = decode(raw)
    assert consumed == len(raw)
    assert frame.device_id == device_id
    assert frame.payload == payload
    verify_sig(frame, random_key.public_key())


# ── streaming (back-to-back frames) ────────────────────────────────────────


def test_back_to_back_frames(random_key):
    a = encode(FrameType.D2H, b"id1", b"hello", random_key)
    b = encode(FrameType.D2H, b"id2x" * 4, b"world!" * 5, random_key)
    stream = a + b

    f1, n1 = decode(stream)
    assert n1 == len(a)
    f2, n2 = decode(stream[n1:])
    assert n2 == len(b)
    assert f1.payload == b"hello"
    assert f2.payload == b"world!" * 5


# ── sig verification ───────────────────────────────────────────────────────


def test_bad_signature_raises(random_key):
    raw = encode(FrameType.D2H, b"id", b"payload", random_key)
    tampered = bytearray(raw)
    tampered[12] ^= 0x01  # flip a payload bit
    frame, _ = decode(bytes(tampered))
    with pytest.raises(BadSignature):
        verify_sig(frame, random_key.public_key())


def test_wrong_pubkey_raises(random_key):
    other = ec.generate_private_key(CURVE)
    raw = encode(FrameType.D2H, b"id", b"payload", random_key)
    frame, _ = decode(raw)
    with pytest.raises(BadSignature):
        verify_sig(frame, other.public_key())


# ── malformed input ────────────────────────────────────────────────────────


def test_bad_magic():
    raw = b"XX" + b"\x00" * 100
    with pytest.raises(BadMagic):
        decode(raw)


def test_truncated_header():
    with pytest.raises(TruncatedFrame):
        decode(b"\x4e\x4e\x00\x00")


def test_truncated_body(random_key):
    raw = encode(FrameType.D2H, b"id", b"payload", random_key)
    with pytest.raises(TruncatedFrame):
        decode(raw[:-5])


def test_pkt_size_below_minimum():
    pkt_size = 10  # less than 2 + 0 + 64
    raw = MAGIC + struct.pack("<HIH", 0, pkt_size, 0)
    with pytest.raises(ProtoError):
        decode(raw + b"\x00" * 100)


# ── inner cmd helpers ──────────────────────────────────────────────────────


def test_inner_cmd_round_trip():
    args = b"\x01"
    inner = encode_inner(Cmd.HUB_STATUS_ANNOUNCE, args)
    cmd, parsed_args = decode_inner(inner)
    assert cmd == Cmd.HUB_STATUS_ANNOUNCE
    assert parsed_args == args


def test_inner_cmd_no_args():
    inner = encode_inner(Cmd.HUB_STATUS_QUERY)
    cmd, parsed_args = decode_inner(inner)
    assert cmd == Cmd.HUB_STATUS_QUERY
    assert parsed_args == b""


def test_inner_cmd_truncated():
    with pytest.raises(ProtoError):
        decode_inner(b"\x01")


# ── pubkey serialisation ───────────────────────────────────────────────────


def test_pubkey_round_trip(signing_key):
    pub = signing_key.public_key()
    blob = pubkey_to_uncompressed(pub)
    assert len(blob) == PUBKEY_LEN
    assert blob[0] == 0x04
    pub2 = pubkey_from_uncompressed(blob)
    nums1 = pub.public_numbers()
    nums2 = pub2.public_numbers()
    assert nums1.x == nums2.x and nums1.y == nums2.y


def test_pubkey_bad_prefix():
    with pytest.raises(ProtoError):
        pubkey_from_uncompressed(b"\x02" + b"\x00" * 64)


# ── header determinism (used as the cross-language reference) ──────────────


def test_header_determinism(signing_key):
    """Header bytes are fully deterministic given the inputs.  Payload and
    pubkey are deterministic too — only the ECDSA signature varies between
    runs.  This is what the C-side test relies on for cross-language
    framing parity (C parses Python's frame, then verifies the sig).
    """
    device_id = bytes([1, 2, 3, 4])
    payload = struct.pack("<H", Cmd.HUB_STATUS_QUERY)
    raw = encode(FrameType.D2G, device_id, payload, signing_key)

    assert raw[:2] == MAGIC
    assert struct.unpack("<H", raw[2:4])[0] == FrameType.D2G
    pkt_size = struct.unpack("<I", raw[4:8])[0]
    assert pkt_size == 2 + len(device_id) + len(payload) + SIG_LEN
    assert struct.unpack("<H", raw[8:10])[0] == len(device_id)
    assert raw[10 : 10 + 4] == device_id
    assert raw[14 : 16] == payload

    assert len(raw) == OVERHEAD + len(device_id) + len(payload)

    frame, _ = decode(raw)
    verify_sig(frame, signing_key.public_key())

    # Hex dump for cross-language verification (only printed with `-s`).
    print()
    print(f"FRAME_HEX  = {raw.hex()}")
    print(f"PUBKEY_HEX = {pubkey_to_uncompressed(signing_key.public_key()).hex()}")
