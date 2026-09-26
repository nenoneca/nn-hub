"""
Ed25519 identity for hub.

On first run hub generates a keypair and writes hub_key.pem to --data-dir.
Hub ID is the first 16 hex characters of the raw public key — short enough
to display in CLI output, unique enough for a single-owner deployment.

Gateway keypairs are generated on the device side (not here).  Hub only
stores gateway public keys in the database for verification.
"""

from __future__ import annotations
import base64
import os
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey, Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding, PublicFormat, PrivateFormat, NoEncryption,
    load_pem_private_key,
)
from cryptography.exceptions import InvalidSignature


# ── key lifecycle ────────────────────────────────────────────────────────────

def load_or_generate(data_dir: Path) -> Ed25519PrivateKey:
    key_path = data_dir / "hub_key.pem"
    if key_path.exists():
        return load_pem_private_key(key_path.read_bytes(), password=None)
    key = Ed25519PrivateKey.generate()
    key_path.write_bytes(
        key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    )
    return key


# ── helpers ──────────────────────────────────────────────────────────────────

def hub_id(privkey: Ed25519PrivateKey) -> str:
    """Stable 16-char hex ID derived from the public key."""
    raw = privkey.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return raw.hex()[:16]


def pubkey_b64(privkey: Ed25519PrivateKey) -> str:
    raw = privkey.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return base64.b64encode(raw).decode()


def random_nonce() -> str:
    return base64.b64encode(os.urandom(32)).decode()


def sign(privkey: Ed25519PrivateKey, data: bytes) -> str:
    return base64.b64encode(privkey.sign(data)).decode()


def verify(pubkey_b64_str: str, data: bytes, sig_b64: str) -> bool:
    try:
        raw    = base64.b64decode(pubkey_b64_str)
        pubkey = Ed25519PublicKey.from_public_bytes(raw)
        pubkey.verify(base64.b64decode(sig_b64), data)
        return True
    except (InvalidSignature, Exception):
        return False
