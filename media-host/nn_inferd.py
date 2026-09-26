#!/usr/bin/env python3
"""nn-inferd — shared inference service.

Camera pipelines stay separate processes; this owns the accelerators.  One
model instance per device instead of one per camera, fair scheduling between
cameras, and a wedged device can be drained without taking a camera down.

Transport: protobuf over AF_UNIX SOCK_SEQPACKET, frames via a shared memory
pool whose fd arrives once with SCM_RIGHTS — pixels never cross the socket.
See nn-media-stream/docs/ACCEL_SERVICES_DESIGN.md.
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import socket
import struct
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.environ.get("NN_ACCEL_PY", "/opt/nn-accel/py"))
sys.path.insert(0, os.environ.get("NN_ACCEL_GEN", "/opt/nn-accel/gen"))

from shmpool import Pool                      # noqa: E402
import nn_accel_pb2 as pb                     # noqa: E402

import numpy as np                            # noqa: E402


# ── devices ────────────────────────────────────────────────────────────────
class Device:
    """One accelerator with its own model instance.

    parallelism is how many jobs it sustains IN FLIGHT; the scheduler runs
    that many workers for it, so a fast device is never blocked behind a
    slow one.
    """

    def __init__(self, dev_id: str, kind: int, model_path: str,
                 parallelism: int = 1):
        self.id, self.kind, self.model_path = dev_id, kind, model_path
        self.parallelism = parallelism
        self.healthy = True
        self.fails = 0
        self.det = None

    def load(self):
        if self.kind == pb.DEVICE_TIDL:
            from tidl_detector import TidlYoloxDetector
            self.det = TidlYoloxDetector(self.model_path)
            return self
        from yolox_detector import YoloxDetector, YoloxNpuDetector
        if self.kind == pb.DEVICE_AIPU:
            self.det = YoloxNpuDetector(self.model_path)
        else:
            self.det = YoloxDetector(self.model_path)
        return self

    def run(self, rgb: np.ndarray, min_score: float):
        return self.det(rgb, min_score)


# ── job queue with per-camera fairness ─────────────────────────────────────
class FairQueue:
    """Round-robin across cameras, drop-oldest within a camera.

    A single busy camera must not starve the others, and a camera that
    outruns the accelerators must lose ITS OWN oldest frame — never block
    the service or grow without bound.
    """

    def __init__(self, per_camera_max: int = 4):
        self.per_max = per_camera_max
        self._q: dict[str, list] = {}
        self._order: list[str] = []
        self._cv = threading.Condition()
        self.dropped = 0

    def put(self, owner: str, job) -> bool:
        with self._cv:
            q = self._q.setdefault(owner, [])
            if owner not in self._order:
                self._order.append(owner)
            dropped = False
            while len(q) >= self.per_max:
                old = q.pop(0)
                old.drop("queue full")
                self.dropped += 1
                dropped = True
            q.append(job)
            self._cv.notify()
            return not dropped

    def get(self, timeout: float = 0.5):
        with self._cv:
            deadline = time.time() + timeout
            while True:
                for _ in range(len(self._order)):
                    owner = self._order.pop(0)
                    self._order.append(owner)          # rotate: fairness
                    q = self._q.get(owner)
                    if q:
                        return q.pop(0)
                left = deadline - time.time()
                if left <= 0:
                    return None
                self._cv.wait(left)


class Job:
    __slots__ = ("owner", "pool", "slot", "seq", "ts_ms", "deadline_ms",
                 "msg_id", "conn", "queued_at", "min_score")

    def __init__(self, owner, pool, slot, req, msg_id, conn):
        self.owner, self.pool, self.slot = owner, pool, slot
        self.seq, self.ts_ms = req.seq, req.ts_ms
        self.deadline_ms = req.deadline_ms or 0
        self.msg_id, self.conn = msg_id, conn
        self.queued_at = time.time()
        self.min_score = 0.05          # policy filtering happens upstream

    def expired(self) -> bool:
        """Late by EITHER measure: queued too long, or the frame itself is
        already stale.

        Queue time alone is not enough — a request can sit in the client's
        socket or arrive after a reconnect, and a detection that lands after
        its frame is gone paints a box on stale video.  The capture stamp is
        only trusted when it is plausible (same host, sane clock); a skewed
        or unset ts_ms falls back to queue time rather than dropping
        everything."""
        if self.deadline_ms <= 0:
            return False
        if (time.time() - self.queued_at) * 1000 > self.deadline_ms:
            return True
        if self.ts_ms:
            age_ms = time.time() * 1000 - self.ts_ms
            if 0 <= age_ms < 60_000:            # plausible => trust it
                return age_ms > self.deadline_ms
        return False

    def drop(self, why: str):
        self.pool.release(self.slot)
        _reply(self.conn, self.msg_id,
               pb.InferResp(seq=self.seq, ts_ms=self.ts_ms, dropped=True,
                            slot_released=True))


def _reply(conn, msg_id: int, resp: pb.InferResp):
    m = pb.Msg(id=msg_id); m.infer_resp.CopyFrom(resp)
    try:
        conn.send(m.SerializeToString())
    except OSError:
        pass


# ── service ────────────────────────────────────────────────────────────────
class InferService:
    def __init__(self, devices: list[Device], sock_path: str):
        self.devices = devices
        self.sock_path = sock_path
        self.q = FairQueue()
        self.stats = {"runs": 0, "dropped": 0, "queue_us": 0}
        self.cam_stats = {}            # owner -> {runs, dropped, queue_us}
        self._stop = threading.Event()

    def _cam(self, owner):
        st = self.cam_stats.get(owner)
        if st is None:
            st = self.cam_stats[owner] = {"runs": 0, "dropped": 0,
                                          "queue_us": 0}
        return st

    def start_workers(self):
        n = 0
        for dev in self.devices:
            for i in range(max(1, dev.parallelism)):
                threading.Thread(target=self._worker, args=(dev, i),
                                 name=f"{dev.id}#{i}", daemon=True).start()
                n += 1
        print(f">> {n} worker(s) across {len(self.devices)} device(s): " +
              ", ".join(f"{d.id}x{d.parallelism}" for d in self.devices),
              flush=True)

    def _worker(self, dev: Device, idx: int):
        while not self._stop.is_set():
            job = self.q.get()
            if job is None:
                continue
            if job.expired():
                job.drop("deadline")
                self.stats["dropped"] += 1
                self._cam(job.owner)["dropped"] += 1
                continue
            queue_us = int((time.time() - job.queued_at) * 1e6)
            t0 = time.time()
            try:
                frame = job.pool.frame(job.slot)          # zero copy
                rgb = np.frombuffer(frame, np.uint8, count=job.pool.height *
                                    job.pool.stride).reshape(
                                        job.pool.height, job.pool.stride // 3, 3)
                dets = dev.run(rgb, job.min_score)
                dev.fails = 0
            except Exception as e:
                dev.fails += 1
                if dev.fails >= 3 and dev.healthy:
                    dev.healthy = False        # stop feeding a wedged device
                    print(f"!! device {dev.id} unhealthy: {e}",
                          file=sys.stderr, flush=True)
                job.drop(f"device error: {e}")
                self.stats["dropped"] += 1
                self._cam(job.owner)["dropped"] += 1
                continue
            finally:
                del frame
                job.pool.release(job.slot)     # producer may refill now

            resp = pb.InferResp(seq=job.seq, ts_ms=job.ts_ms,
                                device_id=dev.id, slot_released=True,
                                latency_us=int((time.time() - t0) * 1e6),
                                queue_us=queue_us)
            for d in dets:
                box = d.get("box") or [0, 0, 0, 0]
                resp.dets.add(x=max(0, int(box[0])), y=max(0, int(box[1])),
                              w=max(0, int(box[2])), h=max(0, int(box[3])),
                              class_id=int(d.get("class_id", 0)),
                              conf_x1000=int(round(float(d.get("score", 0)) * 1000)))
            _reply(job.conn, job.msg_id, resp)
            self.stats["runs"] += 1
            self.stats["queue_us"] = queue_us
            st = self._cam(job.owner)
            st["runs"] += 1
            st["queue_us"] = queue_us

    def serve(self):
        if os.path.exists(self.sock_path):
            os.unlink(self.sock_path)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        srv.bind(self.sock_path)
        os.chmod(self.sock_path, 0o660)
        srv.listen(16)
        print(f">> nn-inferd listening on {self.sock_path}", flush=True)
        while not self._stop.is_set():
            try:
                conn, _ = srv.accept()
            except OSError:
                break
            threading.Thread(target=self._client, args=(conn,),
                             daemon=True).start()

    def _client(self, conn):
        pools: dict[int, Pool] = {}
        owner = "?"
        next_pool = 1
        try:
            while True:
                data, fds, _, _ = socket.recv_fds(conn, 65536, 1)
                if not data:
                    break
                msg = pb.Msg()
                msg.ParseFromString(data)
                which = msg.WhichOneof("body")

                if which == "hello":
                    owner = msg.hello.client or owner
                    caps = pb.Capabilities(service_version="0.1")
                    for d in self.devices:
                        caps.devices.add(id=d.id, kind=d.kind,
                                         parallelism=d.parallelism,
                                         healthy=d.healthy,
                                         models=[os.path.basename(d.model_path)])
                    caps.formats.extend([pb.PIX_RGB888, pb.PIX_BGR888])
                    out = pb.Msg(id=msg.id); out.caps.CopyFrom(caps)
                    conn.send(out.SerializeToString())

                elif which == "open_pool":
                    if not fds:
                        continue
                    p = Pool(fds[0])
                    pid = next_pool; next_pool += 1
                    pools[pid] = p
                    owner = msg.open_pool.owner or owner
                    print(f">> pool {pid} from {owner}: {p.width}x{p.height} "
                          f"x{p.slots} slots ({p.slot_bytes} B)", flush=True)
                    out = pb.Msg(id=msg.id); out.pool_id.id = pid
                    conn.send(out.SerializeToString())

                elif which == "infer":
                    p = pools.get(msg.infer.pool)
                    if p is None:
                        continue
                    self.q.put(owner, Job(owner, p, msg.infer.slot,
                                          msg.infer, msg.id, conn))
        except (OSError, EOFError):
            pass
        finally:
            for p in pools.values():
                p.close()
            conn.close()
            # Forget the client's counters when it goes away.  Otherwise a
            # one-off test client, or a camera that gets renamed, sits in the
            # stats forever and pads totals that people read as "live".
            self.cam_stats.pop(owner, None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--socket", default="/run/nn-inferd.sock")
    ap.add_argument("--model", default="",
                    help=".cix (AIPU) or .onnx (CPU) model")
    ap.add_argument("--aipu-parallel", type=int, default=1,
                    help="jobs the NPU sustains in flight")
    ap.add_argument("--tidl-model", default="",
                    help="TI model-zoo dir to serve on the C7x (BeagleY-AI)")
    ap.add_argument("--tidl-parallel", type=int, default=1)
    ap.add_argument("--cpu-fallback", default="",
                    help="onnx model to also serve on the CPU")
    args = ap.parse_args()

    devices = []
    if args.model:
        kind = pb.DEVICE_AIPU if args.model.endswith(".cix") else pb.DEVICE_CPU
        try:
            devices.append(Device("aipu0" if kind == pb.DEVICE_AIPU else "cpu0",
                                  kind, args.model, args.aipu_parallel).load())
            print(f">> loaded {args.model}", flush=True)
        except Exception as e:
            print(f"!! {args.model}: {e}", file=sys.stderr, flush=True)
    if args.tidl_model:
        try:
            devices.append(Device("c7x0", pb.DEVICE_TIDL, args.tidl_model,
                                  args.tidl_parallel).load())
            print(f">> loaded TIDL {args.tidl_model}", flush=True)
        except Exception as e:
            print(f"!! tidl {args.tidl_model}: {e}", file=sys.stderr, flush=True)
    if args.cpu_fallback:
        try:
            devices.append(Device("cpu0", pb.DEVICE_CPU, args.cpu_fallback, 1).load())
        except Exception as e:
            print(f"!! cpu fallback: {e}", file=sys.stderr, flush=True)
    if not devices:
        print("!! no devices — nothing to serve", file=sys.stderr, flush=True)
        return 1

    svc = InferService(devices, args.socket)
    svc.start_workers()
    threading.Thread(target=_stats_loop, args=(svc,), daemon=True).start()
    svc.serve()
    return 0


def _stats_loop(svc):
    stats_path = os.path.join(os.path.dirname(svc.sock_path), "stats.json")
    while True:
        time.sleep(10)
        print(f">> runs={svc.stats['runs']} dropped={svc.stats['dropped']} "
              f"(queue {svc.q.dropped}) last_queue_us={svc.stats['queue_us']}",
              flush=True)
        doc = {"ts": int(time.time()),
               "total": dict(svc.stats), "queue_dropped": svc.q.dropped,
               "devices": [{"id": d.id, "kind": d.kind,
                            "parallelism": d.parallelism,
                            "healthy": d.healthy} for d in svc.devices],
               "cameras": {k: dict(v) for k, v in svc.cam_stats.items()}}
        try:
            tmp = stats_path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(doc, f)
            os.replace(tmp, stats_path)
            os.chmod(stats_path, 0o644)
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(main())
