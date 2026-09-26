"""
Hub ↔ device encrypted messaging  (protocol v2).

Protocol v2 — Noise-IK-style authenticated ECIES:
  No Ed25519 signing.  Authentication comes from the static-static X25519
  exchange which binds each message to the long-term key pair of both parties.

ECIES envelope v2 (JSON-serialisable dict):
  {
    "v":     2,
    "epk":   "<base64>",   # ephemeral X25519 public key (32 B)
    "nonce": "<base64>",   # AES-GCM nonce (12 B)
    "ct":    "<base64>",   # AES-GCM ciphertext (includes 16-B tag)
  }

Key derivation:
  shared_e = ECDH(ephem_priv, peer_static_pub)     # ephemeral
  shared_s = ECDH(own_static_priv, peer_static_pub) # static–static
  aes_key  = HKDF-SHA256(shared_e || shared_s,
                          salt=ephem_pub, info=direction_tag, length=32)

  direction_tag:
    hub→device:  b"nn-hub-v2-h2d"
    device→hub:  b"nn-hub-v2-d2h"

On-wire bytes for BLE hub-config characteristic (WRITE to device):
  [1B name_len][name UTF-8][32B hub X25519 pub]
  max = 1 + 32 + 32 = 65 bytes

On-wire bytes for BLE device-pubkeys characteristic (READ from device):
  [32B device X25519 pub]  = 32 bytes fixed

Legacy v1 note:
  hub_key.pem (Ed25519) is loaded by the hub but no longer used for CoAP
  messaging.  It is kept for potential future use.
"""

from __future__ import annotations
import base64
import os
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey, X25519PublicKey,
)
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes, serialization


_INFO_H2D = b"nn-hub-v2-h2d"
_INFO_D2H = b"nn-hub-v2-d2h"


# ── X25519 key lifecycle ─────────────────────────────────────────────────────

def generate_x25519() -> X25519PrivateKey:
    return X25519PrivateKey.generate()


def load_or_generate_enc_key(data_dir: Path) -> X25519PrivateKey:
    """Load the hub's X25519 encryption key, generating on first run."""
    path = data_dir / "hub_enc_key.raw"
    if path.exists():
        raw = path.read_bytes()
        return X25519PrivateKey.from_private_bytes(raw)
    key = X25519PrivateKey.generate()
    raw = key.private_bytes(
        serialization.Encoding.Raw,
        serialization.PrivateFormat.Raw,
        serialization.NoEncryption(),
    )
    path.write_bytes(raw)
    return key


def x25519_pubkey_bytes(key: X25519PrivateKey) -> bytes:
    return key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )


def ed25519_pubkey_bytes(key: Ed25519PrivateKey) -> bytes:
    return key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )


# ── BLE wire format helpers ──────────────────────────────────────────────────

def encode_hub_config(name: str, hub_x25519_priv: X25519PrivateKey) -> bytes:
    """
    Build the binary blob written to the HUB_CONFIG_CHAR BLE characteristic.
    Format: [1B name_len][name UTF-8][32B hub X25519 pub]
    """
    name_bytes = name.encode("utf-8")[:32]
    x25519_pub = x25519_pubkey_bytes(hub_x25519_priv)
    return bytes([len(name_bytes)]) + name_bytes + x25519_pub


def decode_device_pubkeys(raw: bytes) -> tuple[bytes]:
    """
    Parse the 32-byte blob read from DEVICE_PUBKEYS_CHAR.
    Returns (x25519_pub_32,).
    """
    if len(raw) < 32:
        raise ValueError(f"Expected 32 bytes, got {len(raw)}")
    return (raw[:32],)


# ── Internal key derivation ──────────────────────────────────────────────────

def _derive_aes_key(ephem_priv: X25519PrivateKey,
                    ephem_pub_bytes: bytes,
                    own_static_priv: X25519PrivateKey,
                    peer_static_pub_bytes: bytes,
                    info: bytes) -> bytes:
    """
    Derive a 32-byte AES key using ephemeral + static ECDH with HKDF-SHA256.
    """
    peer_pub = X25519PublicKey.from_public_bytes(peer_static_pub_bytes)
    shared_e = ephem_priv.exchange(peer_pub)
    shared_s = own_static_priv.exchange(peer_pub)

    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=ephem_pub_bytes,
        info=info,
    ).derive(shared_e + shared_s)


# ── ECIES encrypt ────────────────────────────────────────────────────────────

def encrypt(
    plaintext: bytes,
    recipient_x25519_pub_bytes: bytes,
    sender_x25519_priv: X25519PrivateKey,
    direction: str = "h2d",
) -> dict:
    """
    ECIES v2 encrypt *plaintext* for *recipient_x25519_pub_bytes*.

    *sender_x25519_priv*: hub's long-term X25519 key (for static-static DH)
    *direction*: "h2d" (hub→device) or "d2h" (device→hub)

    Returns a JSON-serialisable dict (the ECIES envelope).
    """
    info = _INFO_H2D if direction == "h2d" else _INFO_D2H

    # Ephemeral X25519 key pair for this message
    ephem_priv = X25519PrivateKey.generate()
    ephem_pub  = ephem_priv.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )

    aes_key = _derive_aes_key(ephem_priv, ephem_pub,
                               sender_x25519_priv, recipient_x25519_pub_bytes,
                               info)

    nonce = os.urandom(12)
    ct    = AESGCM(aes_key).encrypt(nonce, plaintext, None)

    return {
        "v":     2,
        "epk":   base64.b64encode(ephem_pub).decode(),
        "nonce": base64.b64encode(nonce).decode(),
        "ct":    base64.b64encode(ct).decode(),
    }


# ── ECIES decrypt ────────────────────────────────────────────────────────────

def decrypt(
    envelope: dict,
    recipient_x25519_priv: X25519PrivateKey,
    sender_x25519_pub_bytes: bytes,
    direction: str = "h2d",
) -> bytes:
    """
    Decrypt an ECIES v2 *envelope* using *recipient_x25519_priv*.

    *sender_x25519_pub_bytes*: sender's long-term X25519 pub (for static-static DH)
    *direction*: "h2d" (hub→device) or "d2h" (device→hub)

    Raises ValueError on bad format/version, cryptography.exceptions on auth fail.
    """
    if envelope.get("v") != 2:
        raise ValueError(f"Unknown envelope version {envelope.get('v')}")

    info = _INFO_H2D if direction == "h2d" else _INFO_D2H

    ephem_pub_bytes = base64.b64decode(envelope["epk"])
    nonce           = base64.b64decode(envelope["nonce"])
    ct              = base64.b64decode(envelope["ct"])

    ephem_pub_key = X25519PublicKey.from_public_bytes(ephem_pub_bytes)
    shared_e = recipient_x25519_priv.exchange(ephem_pub_key)

    sender_pub = X25519PublicKey.from_public_bytes(sender_x25519_pub_bytes)
    shared_s   = recipient_x25519_priv.exchange(sender_pub)

    aes_key = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=ephem_pub_bytes,
        info=info,
    ).derive(shared_e + shared_s)

    return AESGCM(aes_key).decrypt(nonce, ct, None)
