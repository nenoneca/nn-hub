#!/usr/bin/env python3
"""
Decrypting video receiver for the nn media camera (encrypted uplink).

The C6 now wraps its H.264 uplink in an nn_sectun session, so the plain
`tcpserversrc ! filesink` receiver can no longer read it.  This server accepts
the C6's connection, completes the handshake with the streaming service's
X25519 private key, decrypts the record stream, and writes the raw H.264
(Annex-B) elementary stream to a file (or stdout) — so ffmpeg / GStreamer can
consume it exactly as before.

  C6 (encrypt) ── TCP ──▶ secure_receiver.py (decrypt) ──▶ raw .h264

Usage:
  secure_receiver.py [--port 8888] [--out FILE|-] [--keydir DIR]

--keydir holds the streaming service's X25519 key (hub_enc_key.*).  For the
bench test the stream pubkey was provisioned equal to the hub pubkey, so point
this at the hub key dir used during provisioning (e.g. /tmp/ble_hub_data).
"""
import argparse
import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, "/home/chalos/ext/mx500/nn_project_nowest/hub")

from nn_sectun import SecureSession
from hub import crypto


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8888)
    ap.add_argument("--out", default="captures/secure_cam.h264")
    ap.add_argument("--keydir", default="/tmp/ble_hub_data")
    ap.add_argument("--max-bytes", type=int, default=0, help="stop after N bytes (0=unlimited)")
    args = ap.parse_args()

    priv = crypto.load_or_generate_enc_key(Path(args.keydir))
    print(f">> stream service X25519 pub: {crypto.x25519_pubkey_bytes(priv).hex()}")

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", args.port))
    srv.listen(1)
    print(f">> listening (encrypted) on 0.0.0.0:{args.port}")

    out = sys.stdout.buffer if args.out == "-" else open(args.out, "wb")
    while True:
        conn, addr = srv.accept()
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        print(f">> C6 connected from {addr}")
        try:
            sess = SecureSession.accept(conn, priv)
            print(f">> handshake OK; device pub {sess.device_pub.hex()[:16]}...")
            total = 0
            while True:
                data = sess.recv()
                out.write(data)
                out.flush()
                total += len(data)
                if total % (256 * 1024) < len(data):
                    print(f"   decrypted {total//1024} KB")
                if args.max_bytes and total >= args.max_bytes:
                    print(f">> reached {total} bytes; done")
                    return
        except (ConnectionError, ValueError) as e:
            print(f">> session ended: {e} — waiting for reconnect")
        finally:
            conn.close()


if __name__ == "__main__":
    main()
