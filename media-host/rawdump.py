#!/usr/bin/env python3
"""Minimal :8888 sectun reader — dumps C6 uplink records, NO GStreamer.
Isolates whether the C6 is sending typed records at all."""
import socket, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, "/home/chalos/ext/mx500/nn_project_nowest/hub")
from nn_sectun import SecureSession
from hub import crypto
from video_service import RecordParser, NN_REC_VIDEO, NN_REC_AUDIO

priv = crypto.load_or_generate_enc_key(Path("/tmp/ble_hub_data"))
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("0.0.0.0", 8888)); srv.listen(5)
print(">> rawdump listening :8888", flush=True)
while True:
    conn, addr = srv.accept()
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    conn.settimeout(20)
    print(f">> C6 {addr}", flush=True)
    try:
        sess = SecureSession.accept(conn, priv)
        print(">> handshake OK", flush=True)
        p = RecordParser(); rb = v = a = 0; t0 = time.time(); last = t0; tsmin = tsmax = None
        while True:
            d = sess.recv(); rb += len(d)
            for typ, flags, seq, ts, pl in p.feed(d):
                if typ == NN_REC_VIDEO: v += 1
                elif typ == NN_REC_AUDIO: a += 1
                tsmin = ts if tsmin is None else min(tsmin, ts)
                tsmax = ts if tsmax is None else max(tsmax, ts)
            if time.time() - last > 2:
                print(f">> {rb} B  video={v} audio={a}  ts[{tsmin}..{tsmax}]  buf={len(p.buf)}", flush=True)
                last = time.time()
    except Exception as e:
        print(f">> ended: {e}", flush=True)
    finally:
        conn.close()
