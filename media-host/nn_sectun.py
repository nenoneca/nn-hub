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
import threading

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


HELLO_LEN = 4 + 2 + 32 + 32


def read_hello(sock) -> bytes:
    hello = _recv_exact(sock, HELLO_LEN)
    hello_parts(hello)                       # validates
    return hello


def hello_parts(hello: bytes) -> tuple:
    """(ephemeral_pub, device_pub) of a HELLO, validating magic/version."""
    if len(hello) != HELLO_LEN or hello[:4] != MAGIC:
        raise ValueError(f"bad magic {hello[:4]!r}")
    if hello[4] != VERSION:
        raise ValueError(f"unsupported version {hello[4]}")
    return hello[6:38], hello[38:70]


def read_record_ct(sock) -> bytes:
    """One raw ciphertext record (length-prefixed), not decrypted."""
    (ct_len,) = struct.unpack(">I", _recv_exact(sock, 4))
    if ct_len < 16 or ct_len > RECORD_MAX + 16:
        raise ValueError(f"bad record len {ct_len}")
    return _recv_exact(sock, ct_len)


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
        # One writer at a time.  The adapt controller's thread and the event
        # loop (config push) both send on a camera's session; a thread switch
        # between encrypt and sendall put two records on the wire in the
        # wrong nonce order, the camera's AEAD check failed on the next record
        # and it closed the socket — cam3's four "uplink lost" drops on
        # 2026-09-22 fell on the exact second of a config push.
        self._send_lock = threading.Lock()

    @classmethod
    def accept(cls, sock, server_priv: X25519PrivateKey) -> "SecureSession":
        return cls.from_hello(sock, read_hello(sock), server_priv)

    @classmethod
    def from_hello(cls, sock, hello: bytes, server_priv: X25519PrivateKey) -> "SecureSession":
        """Derive the session from an already-read HELLO.  Lets a single
        listener identify the device (its static pub is in the HELLO, in the
        clear) and pick the host key it was provisioned with BEFORE deriving."""
        eph_pub, dev_pub = hello_parts(hello)
        eph = X25519PublicKey.from_public_bytes(eph_pub)
        dev = X25519PublicKey.from_public_bytes(dev_pub)
        ss_e = server_priv.exchange(eph)
        ss_s = server_priv.exchange(dev)
        okm = HKDF(algorithm=hashes.SHA256(), length=64,
                   salt=eph_pub, info=INFO).derive(ss_e + ss_s)
        return cls(sock, okm[:32], okm[32:], dev_pub)

    @classmethod
    def connect(cls, sock, dev_priv: X25519PrivateKey, server_pub: X25519PublicKey) -> "SecureSession":
        """Device side (initiator), mirroring nn_sectun.c — for tests and
        loopback fixtures.  Sends the HELLO and derives the mirror: the client
        ENCRYPTS with the key the server DECRYPTS with (k_c2s) and vice versa."""
        eph_priv = X25519PrivateKey.generate()
        eph_pub = eph_priv.public_key().public_bytes_raw()
        dev_pub = dev_priv.public_key().public_bytes_raw()
        sock.sendall(MAGIC + bytes([VERSION, 0]) + eph_pub + dev_pub)
        ss_e = eph_priv.exchange(server_pub)
        ss_s = dev_priv.exchange(server_pub)
        okm = HKDF(algorithm=hashes.SHA256(), length=64,
                   salt=eph_pub, info=INFO).derive(ss_e + ss_s)
        return cls(sock, okm[32:], okm[:32], dev_pub)      # swapped roles

    def try_open_first(self, ct: bytes):
        """Decrypt a first ciphertext record under this session, advancing
        the counter only on success — a listener can try candidate host keys
        against the first record a device sends (legacy per-slot host keys)."""
        try:
            pt = self._dec.decrypt(_nonce(self._ctr_rx), ct, None)
        except Exception:
            return None
        self._ctr_rx += 1
        return pt

    def recv(self) -> bytes:
        (ct_len,) = struct.unpack(">I", _recv_exact(self.sock, 4))
        if ct_len < 16 or ct_len > RECORD_MAX + 16:
            raise ValueError(f"bad record len {ct_len}")
        ct = _recv_exact(self.sock, ct_len)
        try:
            pt = self._dec.decrypt(_nonce(self._ctr_rx), ct, None)
        except Exception:
            import sys
            print(f'>> sectun InvalidTag: ctr_rx={self._ctr_rx} ct_len={ct_len} '
                  f'head={ct[:24].hex()}', file=sys.stderr, flush=True)
            raise
        self._ctr_rx += 1
        return pt

    def send(self, data: bytes) -> None:
        with self._send_lock:
            for i in range(0, max(len(data), 1), RECORD_MAX):
                chunk = data[i:i + RECORD_MAX]
                ct = self._enc.encrypt(_nonce(self._ctr_tx), chunk, None)
                self._ctr_tx += 1
                self.sock.sendall(struct.pack(">I", len(ct)) + ct)
