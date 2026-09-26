#!/usr/bin/env python3
"""
Media OTA driver (hub side) — pushes a paired P4+C6 firmware bundle to the camera
over the encrypted nn_ctrl control channel, mirroring the sensor split
download/swap flow.

The C6 dials this server (the provisioned hub control endpoint).  Over one
encrypted session we:

  OFFER(p4_ver, c6_ver)
  stage P4 : BEGIN -> DATA* -> VERIFY(sha)      (C6 relays chunks to the P4)
  stage C6 : BEGIN -> DATA* -> VERIFY(sha)      (C6 stages its own slot)
  ARM                                           (both staged+verified, persisted)
  [--apply] APPLY                               (C6 swaps P4 first, then itself)

With --apply the C6 reboots after APPLY; we wait for it to reconnect and read
STATUS to confirm both chips reached their target versions.

  media_ota_push.py --p4-bin <p4.bin> --p4-ver 0.1.1 \
                    --c6-bin <c6.bin> --c6-ver 0.1.1 \
                    [--port 8770] [--keydir /tmp/ble_hub_data] [--apply]
"""
import argparse
import hashlib
import json
import socket
import struct
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, "/home/chalos/ext/mx500/nn_project_nowest/hub")

from nn_sectun import SecureSession
from hub import crypto

OP_OFFER, OP_BEGIN, OP_DATA, OP_VERIFY, OP_ARM, OP_APPLY, OP_STATUS = (
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16)
TARGET_P4, TARGET_C6 = 0, 1
BLK = 1008


def req(sess, op, body=b"", timeout=None):
    if timeout is not None:
        sess.sock.settimeout(timeout)
    sess.send(bytes([op]) + body)
    rep = sess.recv()
    if timeout is not None:
        sess.sock.settimeout(None)
    if not rep or rep[0] != op:
        raise ValueError(f"bad reply op=0x{rep[0]:02x} (wanted 0x{op:02x})")
    return rep[1:]


def stage(sess, target, data, sha):
    name = "P4" if target == TARGET_P4 else "C6"
    size = len(data)
    st = req(sess, OP_BEGIN, bytes([target]) + struct.pack("<I", size), timeout=15)[0]
    if st != 0:
        raise RuntimeError(f"{name} BEGIN status={st}")
    print(f">> {name}: staging {size} B ({(size + BLK - 1)//BLK} blocks)")
    off = 0
    t0 = time.time()
    while off < size:
        chunk = data[off:off + BLK]
        st = req(sess, OP_DATA, bytes([target]) + struct.pack("<I", off) + chunk, timeout=10)[0]
        if st != 0:
            raise RuntimeError(f"{name} DATA @{off} status={st}")
        off += len(chunk)
        if (off // BLK) % 200 == 0:
            print(f"   {name} {off}/{size} ({off*100//size}%)")
    dt = time.time() - t0
    st = req(sess, OP_VERIFY, bytes([target]) + struct.pack("<I", size) + sha, timeout=20)[0]
    print(f">> {name}: pushed in {dt:.1f}s, VERIFY -> {'MATCH' if st == 0 else f'FAIL({st})'}")
    if st != 0:
        raise RuntimeError(f"{name} verify failed status={st}")


def drive_stage(sess, p4, c6, p4_ver, c6_ver, do_apply):
    offer = (p4_ver + "\n" + c6_ver).encode()
    st = req(sess, OP_OFFER, offer, timeout=15)[0]
    if st != 0:
        raise RuntimeError(f"OFFER rejected status={st}")
    print(f">> OFFER accepted (p4={p4_ver} c6={c6_ver})")
    stage(sess, TARGET_P4, p4, hashlib.sha256(p4).digest())
    stage(sess, TARGET_C6, c6, hashlib.sha256(c6).digest())
    st = req(sess, OP_ARM, timeout=15)[0]
    if st != 0:
        raise RuntimeError(f"ARM status={st}")
    print(">> ARMED — both slots staged + verified, awaiting apply")
    if do_apply:
        print(">> APPLY (P4 first, then C6); C6 will reboot...")
        st = req(sess, OP_APPLY, timeout=90)[0]
        print(f">> APPLY ack status={st}")
        return st == 0
    return True


def run_ota(p4: bytes, c6: bytes, p4_ver: str, c6_ver: str, priv,
            port: int = 8770, apply: bool = True, apply_only: bool = False,
            log=print) -> dict:
    """Drive one media OTA over the control channel the C6 dials into.  Blocks
    until done (~3 min for a full stage+apply).  Returns a result dict:
        {"result": "ok"|"mismatch"|"armed"|"apply_failed..."|"no_device",
         "c6": "<running ver>", "p4": "<running ver>"}
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port)); srv.listen(1); srv.settimeout(120)
    log(f">> media OTA server on :{port} (apply={apply})")
    out = {"result": "no_device", "c6": "", "p4": ""}
    expect_verify = False
    try:
        while True:
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                log(">> timed out waiting for camera to dial in")
                return out
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            log(f">> C6 connected from {addr}")
            try:
                conn.settimeout(20)              # don't wedge on a stale/half-open dial
                sess = SecureSession.accept(conn, priv)
                conn.settimeout(None)
                if not expect_verify:
                    if apply_only:
                        st = req(sess, OP_APPLY, timeout=90)[0]
                        if st != 0:
                            out["result"] = f"apply_failed({st})"; return out
                    else:
                        applied = drive_stage(sess, p4, c6, p4_ver, c6_ver, apply)
                        if not apply:
                            out["result"] = "armed"; return out
                        if not applied:
                            out["result"] = "apply_failed"; return out
                    expect_verify = True   # C6 rebooting; verify on reconnect
                else:
                    st = req(sess, OP_STATUS, timeout=15).decode(errors="replace")
                    log(f">> post-apply STATUS: {st}")
                    try:
                        d = json.loads(st)
                    except Exception:
                        d = {}
                    ok = d.get("c6") == c6_ver and d.get("p4") == p4_ver
                    out = {"result": "ok" if ok else "mismatch",
                           "c6": d.get("c6", ""), "p4": d.get("p4", "")}
                    return out
            except (ConnectionError, ValueError, OSError, RuntimeError) as e:
                log(f">> session ended: {e}")
            finally:
                conn.close()
    finally:
        srv.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--p4-bin", required=True)
    ap.add_argument("--p4-ver", required=True)
    ap.add_argument("--c6-bin", required=True)
    ap.add_argument("--c6-ver", required=True)
    ap.add_argument("--port", type=int, default=8770)
    ap.add_argument("--keydir", default="/tmp/ble_hub_data")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--apply-only", action="store_true",
                    help="skip staging; the node is already ARMED — just APPLY + verify")
    args = ap.parse_args()

    p4 = Path(args.p4_bin).read_bytes()
    c6 = Path(args.c6_bin).read_bytes()
    priv = crypto.load_or_generate_enc_key(Path(args.keydir))
    print(f">> hub X25519 pub: {crypto.x25519_pubkey_bytes(priv).hex()}")

    res = run_ota(p4, c6, args.p4_ver, args.c6_ver, priv,
                  port=args.port, apply=args.apply, apply_only=args.apply_only)
    ok = res["result"] == "ok"
    print(">> RESULT:", "BOTH ON TARGET ✓" if ok else f"{res['result']} ✗", res)
    sys.exit(0 if ok or res["result"] == "armed" else 1)


if __name__ == "__main__":
    main()
