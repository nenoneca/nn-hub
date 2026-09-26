#!/usr/bin/env python3
"""
Host side of the encrypted C6↔hub control channel (the hub control server).

The C6 dials this server (provisioned hub_host:hub_port), completes the
nn_sectun handshake using the hub's X25519 key, then serves a simple
request/reply protocol.  This server drives that channel: it sends commands and
prints the replies, demonstrating an authenticated, encrypted control link
independent of the video uplink.

  C6 (nn_ctrl, client) ── encrypted TCP ──▶ media_ctrl_server.py (hub)

Opcodes:  0x01 PING → "pong";  0x02 STATUS → status string;
          0x20 SET_TIME(u64 epoch_ms) → C6 sets its absolute clock (and syncs the
          P4).  Pushed on connect and every --time-sync-hours (default 1h).

Usage:
  media_ctrl_server.py [--port 8770] [--keydir /tmp/ble_hub_data] [--poll 5]
                       [--time-sync-hours 1]
"""
import argparse
import socket
import threading
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, "/home/chalos/ext/mx500/nn_project_nowest/hub")

from nn_sectun import SecureSession
from hub import crypto

OP_PING = 0x01
OP_STATUS = 0x02
OP_SET_TIME = 0x20
OP_TIME_STATUS = 0x21


def push_time(sess):
    """Send the current wall clock (epoch ms) so the C6 sets its absolute clock
    and (re)syncs the P4's timestamp offset."""
    epoch_ms = int(time.time() * 1000)
    sess.send(bytes([OP_SET_TIME]) + epoch_ms.to_bytes(8, "little"))
    rep = sess.recv()
    print(f"   SET_TIME({epoch_ms}) → op=0x{rep[0]:02x} status={rep[1] if len(rep) > 1 else '?'}", flush=True)


def serve_one(conn, priv, poll: float, rounds: int, time_sync_hours: float):
    sess = SecureSession.accept(conn, priv)
    print(f">> handshake OK; device pub {sess.device_pub.hex()[:16]}...", flush=True)
    push_time(sess)                       # sync time immediately on connect
    last_time_sync = time.time()
    sync_interval = time_sync_hours * 3600.0
    n = 0
    while rounds == 0 or n < rounds:
        if time.time() - last_time_sync >= sync_interval:
            push_time(sess)
            last_time_sync = time.time()
        sess.send(bytes([OP_PING]))
        rep = sess.recv()
        print(f"   PING  → op=0x{rep[0]:02x} {rep[1:].decode(errors='replace')!r}", flush=True)
        sess.send(bytes([OP_STATUS]))
        rep = sess.recv()
        print(f"   STATUS→ op=0x{rep[0]:02x} {rep[1:].decode(errors='replace')!r}", flush=True)
        sess.send(bytes([OP_TIME_STATUS]))
        rep = sess.recv()
        print(f"   TIME  → op=0x{rep[0]:02x} {rep[1:].decode(errors='replace')!r}", flush=True)
        n += 1
        if rounds and n >= rounds:
            break
        time.sleep(poll)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8770)
    ap.add_argument("--keydir", default="/tmp/ble_hub_data")
    ap.add_argument("--poll", type=float, default=5.0)
    ap.add_argument("--rounds", type=int, default=0, help="exit after N poll rounds (0=forever)")
    ap.add_argument("--time-sync-hours", type=float, default=1.0,
                    help="push wall-clock time to the C6 this often (default 1h)")
    args = ap.parse_args()

    priv = crypto.load_or_generate_enc_key(Path(args.keydir))
    print(f">> hub X25519 pub: {crypto.x25519_pubkey_bytes(priv).hex()}", flush=True)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", args.port))
    # serve_one() runs forever per device, so the accept loop MUST NOT run it
    # inline: with listen(1) and an inline call, the first camera to connect
    # owned the server and every other camera sat unaccepted in the backlog
    # with its handshake bytes unread (observed: cam0 stuck at Recv-Q 70 while
    # cam1 was served).  A full accept queue is also indistinguishable from a
    # hung process, which is how this hid for 14 days.  One thread per device,
    # and a backlog with room for the whole fleet to reconnect at once.
    srv.listen(16)
    print(f">> control server (encrypted) on 0.0.0.0:{args.port}", flush=True)

    def session(conn, addr):
        # A camera that vanishes (power cut, Wi-Fi drop) must not pin its
        # thread forever: without a timeout the recv blocks indefinitely and
        # the device's reconnect is answered by a second, parallel session.
        conn.settimeout(max(30.0, args.poll * 4))
        try:
            serve_one(conn, priv, args.poll, args.rounds, args.time_sync_hours)
        except (ConnectionError, ValueError, OSError) as e:
            print(f">> control session ended [{addr[0]}]: {e}", flush=True)
        except Exception as e:                    # never kill the accept loop
            print(f">> control session error [{addr[0]}]: {e!r}", flush=True)
        finally:
            try:
                conn.close()
            except OSError:
                pass

    while True:
        conn, addr = srv.accept()
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        print(f">> C6 control connection from {addr}", flush=True)
        if args.rounds:                           # one-shot CLI/test mode
            session(conn, addr)
            return
        threading.Thread(target=session, args=(conn, addr),
                         name=f"ctrl-{addr[0]}", daemon=True).start()


if __name__ == "__main__":
    main()
