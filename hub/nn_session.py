"""nn_session — hub side of the symmetric AEAD session.

Byte-exact mirror of device/modules/libs/fw_common/src/nn_session.c so
device.tx == hub.rx and vice versa:

  ecdh  = X25519(hub_x25519_priv, device_x25519_pub)   # == device's X25519(dev_priv, hub_pub)
  salt  = dev_salt(8) || hub_salt(8)
  k_d2h = HKDF-SHA256(ecdh, salt, info=b"nn-sess-v1/d2h", 32)
  k_h2d = HKDF-SHA256(ecdh, salt, info=b"nn-sess-v1/h2d", 32)
  # device: tx=k_d2h, rx=k_h2d ;  hub: tx=k_h2d, rx=k_d2h
  record = ctr(8, BE) || AES-256-GCM(k, nonce, aad, plaintext)   # ct includes 16B tag
  nonce  = b"\\x00\\x00\\x00\\x00" || ctr(8, BE)
"""
from __future__ import annotations

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey, X25519PublicKey,
)

CTR_LEN = 8
TAG_LEN = 16
OVERHEAD = CTR_LEN + TAG_LEN


def static_ecdh(hub_priv: X25519PrivateKey, device_pub: bytes) -> bytes:
    return hub_priv.exchange(X25519PublicKey.from_public_bytes(device_pub))


def _hkdf(ecdh: bytes, salt: bytes, info: bytes) -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=32,
                salt=salt, info=info).derive(ecdh)


class Session:
    __slots__ = ("k_tx", "k_rx", "ctr_tx", "ctr_rx_hi", "rx_window", "rx_any")

    def __init__(self, ecdh: bytes, dev_salt: bytes, hub_salt: bytes,
                 is_device: bool = False):
        salt = dev_salt + hub_salt
        k_d2h = _hkdf(ecdh, salt, b"nn-sess-v1/d2h")
        k_h2d = _hkdf(ecdh, salt, b"nn-sess-v1/h2d")
        if is_device:
            self.k_tx, self.k_rx = k_d2h, k_h2d
        else:
            self.k_tx, self.k_rx = k_h2d, k_d2h
        self.ctr_tx = 0
        self.ctr_rx_hi = 0     # highest accepted counter
        self.rx_window = 0     # bitmap: bit i = (ctr_rx_hi - i) seen
        self.rx_any = False

    @staticmethod
    def _nonce(ctr: int) -> bytes:
        return b"\x00\x00\x00\x00" + ctr.to_bytes(8, "big")

    def seal(self, aad: bytes, pt: bytes) -> bytes:
        ctr = self.ctr_tx
        ct = AESGCM(self.k_tx).encrypt(self._nonce(ctr), pt, aad)
        self.ctr_tx += 1
        return ctr.to_bytes(8, "big") + ct

    def open(self, aad: bytes, record: bytes) -> bytes:
        # 64-deep sliding-window anti-replay (mirror of the C impl):
        # accept newer-than-highest, or an unseen counter within the
        # window; advance state only after the tag verifies.
        if len(record) < OVERHEAD:
            raise ValueError("short record")
        ctr = int.from_bytes(record[:CTR_LEN], "big")
        if self.rx_any and ctr <= self.ctr_rx_hi:
            off = self.ctr_rx_hi - ctr
            if off >= 64 or (self.rx_window >> off) & 1:
                raise ValueError("replay")
        pt = AESGCM(self.k_rx).decrypt(self._nonce(ctr), record[CTR_LEN:], aad)
        if not self.rx_any or ctr > self.ctr_rx_hi:
            shift = (ctr - self.ctr_rx_hi) if self.rx_any else 0
            self.rx_window = 0 if shift >= 64 else (self.rx_window << shift) & ((1 << 64) - 1)
            self.rx_window |= 1
            self.ctr_rx_hi = ctr
            self.rx_any = True
        else:
            self.rx_window |= 1 << (self.ctr_rx_hi - ctr)
        return pt
