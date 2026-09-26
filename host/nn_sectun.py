"""
Host side of the nn_sectun secure session (the "server"/responder).

Mirrors media/components/nn_sectun (device, the client/initiator).  The device
connects, sends a HELLO, and both derive a session key from the provisioned
X25519 identities.  Thereafter the wire carries length-prefixed AES-256-GCM
records.  See nn_sectun.h for the exact protocol.

Usage:
    from nn_sectun import SecureSession
    sess = SecureSession.accept(conn, server_x25519_priv)  # reads HELLO
    data = sess.recv()           # one decrypted record (client->server)
    sess.send(b"...")            # encrypt a record (server->client)
"""
from __future__ import annotations
import struct

from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey, X25519PublicKey,
)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes

MAGIC = b"NNS1"
VERSION = 1
INFO = b"nn-sectun-v1"
RECORD_MAX = 4096


def _recv_exact(sock, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed")
        buf += chunk
    return bytes(buf)


def _nonce(ctr: int) -> bytes:
    return b"\x00\x00\x00\x00" + struct.pack(">Q", ctr)


class SecureSession:
    def __init__(self, sock, k_c2s: bytes, k_s2c: bytes, device_pub: bytes):
        self.sock = sock
        self._dec = AESGCM(k_c2s)     # client -> server (we decrypt)
        self._enc = AESGCM(k_s2c)     # server -> client (we encrypt)
        self._ctr_rx = 0
        self._ctr_tx = 0
        self.device_pub = device_pub  # client's static X25519 pub (32B)

    @classmethod
    def accept(cls, sock, server_priv: X25519PrivateKey) -> "SecureSession":
        hello = _recv_exact(sock, 4 + 2 + 32 + 32)
        if hello[:4] != MAGIC:
            raise ValueError(f"bad magic {hello[:4]!r}")
        if hello[4] != VERSION:
            raise ValueError(f"unsupported version {hello[4]}")
        eph_pub = hello[6:38]
        dev_pub = hello[38:70]

        eph = X25519PublicKey.from_public_bytes(eph_pub)
        dev = X25519PublicKey.from_public_bytes(dev_pub)
        ss_e = server_priv.exchange(eph)
        ss_s = server_priv.exchange(dev)
        okm = HKDF(algorithm=hashes.SHA256(), length=64,
                   salt=eph_pub, info=INFO).derive(ss_e + ss_s)
        return cls(sock, okm[:32], okm[32:], dev_pub)

    def recv(self) -> bytes:
        (ct_len,) = struct.unpack(">I", _recv_exact(self.sock, 4))
        if ct_len < 16 or ct_len > RECORD_MAX + 16:
            raise ValueError(f"bad record len {ct_len}")
        ct = _recv_exact(self.sock, ct_len)
        pt = self._dec.decrypt(_nonce(self._ctr_rx), ct, None)
        self._ctr_rx += 1
        return pt

    def send(self, data: bytes) -> None:
        for i in range(0, max(len(data), 1), RECORD_MAX):
            chunk = data[i:i + RECORD_MAX]
            ct = self._enc.encrypt(_nonce(self._ctr_tx), chunk, None)
            self._ctr_tx += 1
            self.sock.sendall(struct.pack(">I", len(ct)) + ct)
