"""
YOLOX object detector behind a minimal interface.

    det = YoloxDetector("/home/chalos/models/yolox_s.onnx")
    dets = det(frame_rgb_uint8, min_score)   # -> [{"cls","score","box":[x,y,w,h]}]

Backend today: onnxruntime CPU (invocations are motion-gated, so latency in
the ~1-2 s range on the A53s is acceptable).  The interface is deliberately
tiny so a TIDL/NPU implementation can replace it without touching callers.
"""

from __future__ import annotations

import logging
import threading

import numpy as np

log = logging.getLogger("yolox")

COCO = (
    "person bicycle car motorcycle airplane bus train truck boat traffic_light "
    "fire_hydrant stop_sign parking_meter bench bird cat dog horse sheep cow "
    "elephant bear zebra giraffe backpack umbrella handbag tie suitcase frisbee "
    "skis snowboard sports_ball kite baseball_bat baseball_glove skateboard "
    "surfboard tennis_racket bottle wine_glass cup fork knife spoon bowl banana "
    "apple sandwich orange broccoli carrot hot_dog pizza donut cake chair couch "
    "potted_plant bed dining_table toilet tv laptop mouse remote keyboard "
    "cell_phone microwave oven toaster sink refrigerator book clock vase "
    "scissors teddy_bear hair_drier toothbrush").split()


class YoloxDetector:
    def __init__(self, model_path: str, input_size: int = 640, threads: int = 3):
        import onnxruntime as ort
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = threads
        opts.inter_op_num_threads = 1
        self.sess = ort.InferenceSession(model_path, sess_options=opts,
                                         providers=["CPUExecutionProvider"])
        self.input_name = self.sess.get_inputs()[0].name
        self.size = input_size
        self._grids, self._strides = self._make_grids(input_size)
        self._lock = threading.Lock()          # one inference at a time
        log.info("yolox ready: %s (%d threads)", model_path, threads)

    @staticmethod
    def _make_grids(size):
        grids, strides = [], []
        for s in (8, 16, 32):
            n = size // s
            xv, yv = np.meshgrid(np.arange(n), np.arange(n))
            grids.append(np.stack((xv, yv), 2).reshape(-1, 2))
            strides.append(np.full((n * n, 1), s))
        return (np.concatenate(grids).astype(np.float32),
                np.concatenate(strides).astype(np.float32))

    def _letterbox(self, rgb: np.ndarray):
        from PIL import Image
        h, w = rgb.shape[:2]
        r = min(self.size / w, self.size / h)
        nw, nh = int(w * r), int(h * r)
        img = Image.fromarray(rgb).resize((nw, nh), Image.BILINEAR)
        canvas = np.full((self.size, self.size, 3), 114, np.uint8)
        canvas[:nh, :nw] = np.asarray(img)
        return canvas, r

    def _infer(self, blob: np.ndarray) -> np.ndarray:
        """Run one forward pass; returns the (8400, 85) prediction plane.
        Overridden by YoloxNpuDetector to use the CIX NPU instead."""
        return self.sess.run(None, {self.input_name: blob})[0][0]

    def __call__(self, rgb: np.ndarray, min_score: float = 0.45,
                 nms_iou: float = 0.45, max_det: int = 20) -> list[dict]:
        with self._lock:
            canvas, ratio = self._letterbox(rgb)
            # YOLOX expects float32 BGR, no normalisation
            blob = canvas[:, :, ::-1].transpose(2, 0, 1)[None].astype(np.float32)
            out = self._infer(blob)

        # decode: xy = (pred + grid) * stride, wh = exp(pred) * stride
        xy = (out[:, :2] + self._grids) * self._strides
        wh = np.exp(out[:, 2:4]) * self._strides
        scores = out[:, 4:5] * out[:, 5:]
        cls = scores.argmax(1)
        conf = scores[np.arange(len(cls)), cls]
        keep = conf >= min_score
        if not keep.any():
            return []
        xy, wh, cls, conf = xy[keep], wh[keep], cls[keep], conf[keep]
        x1y1 = (xy - wh / 2) / ratio
        boxes = np.concatenate((x1y1, wh / ratio), 1)      # x,y,w,h image space

        # simple class-agnostic NMS
        order = conf.argsort()[::-1][:100]
        picked = []
        for i in order:
            bi = boxes[i]
            ok = True
            for j in picked:
                bj = boxes[j]
                ix = max(0, min(bi[0] + bi[2], bj[0] + bj[2]) - max(bi[0], bj[0]))
                iy = max(0, min(bi[1] + bi[3], bj[1] + bj[3]) - max(bi[1], bj[1]))
                inter = ix * iy
                union = bi[2] * bi[3] + bj[2] * bj[3] - inter
                if union > 0 and inter / union > nms_iou:
                    ok = False
                    break
            if ok:
                picked.append(i)
            if len(picked) >= max_det:
                break
        return [{"cls": COCO[int(cls[i])] if int(cls[i]) < len(COCO) else str(int(cls[i])),
                 "score": round(float(conf[i]), 3),
                 "box": [int(v) for v in boxes[i]]} for i in picked]


class YoloxNpuDetector(YoloxDetector):
    """YOLOX on the CIX P1 (Orange Pi 6 Plus) NPU — Arm China Zhouyi AIPU.

    Same pre/post-processing as the CPU path (letterbox → BGR CHW float32 →
    decode grids → NMS); only the forward pass differs, so we inherit
    everything and override __init__/_infer.

    Needs a model compiled for the NPU (.cix), NOT the .onnx: get
    `yolox_s.cix` from the CIX AI Model Hub on ModelScope
    (cix/ai_model_hub_25_Q3, models/ComputeVision/Object_Detection/onnx_yolox_s).
    Its input (1x3x640x640) and output (1x8400x85) match yolox_s.onnx exactly,
    which is why the shared decode works unchanged.

    The NPU returns a FLAT buffer per output tensor — the engine does not carry
    shape metadata — hence the explicit reshape.
    """

    OUT_SHAPE = (1, 8400, 85)

    def __init__(self, model_path: str, input_size: int = 640):
        # dist-packages holds NOE_Engine/libnoe from the CIX BSP; a venv created
        # without --system-site-packages won't see it.
        import sys as _sys
        for p in ("/usr/local/lib/python3.11/dist-packages",
                  "/usr/lib/python3/dist-packages"):
            if p not in _sys.path:
                _sys.path.append(p)
        from NOE_Engine import EngineInfer
        self._engine = EngineInfer(model_path)
        self.size = input_size
        self._grids, self._strides = self._make_grids(input_size)
        self._lock = threading.Lock()          # AIPU job queue is not reentrant
        log.info("yolox ready on NPU: %s", model_path)

    def _infer(self, blob: np.ndarray) -> np.ndarray:
        out = self._engine.forward(blob)[0]
        return np.reshape(out, self.OUT_SHAPE)[0]

    def close(self):
        try:
            self._engine.clean()
        except Exception:
            pass


def yuv420_to_rgb(yuv: np.ndarray, w: int, h: int) -> np.ndarray:
    """I420 ndarray (h*3/2, w) -> RGB uint8 (h, w, 3), BT.601. numpy-only.
    Also accepts a luma-only (h, w) frame (GRAY8 detection branch) and returns
    replicated-grey RGB — YOLOX degrades gracefully on greyscale input."""
    if yuv.shape[0] < h * 3 // 2:                 # luma-only
        yl = np.ascontiguousarray(yuv[:h, :w]).astype(np.uint8)
        return np.stack((yl, yl, yl), -1)
    y = yuv[:h].astype(np.float32)
    u = yuv[h:h + h // 4].reshape(h // 2, w // 2).astype(np.float32) - 128.0
    v = yuv[h + h // 4:h + h // 2].reshape(h // 2, w // 2).astype(np.float32) - 128.0
    u = u.repeat(2, 0).repeat(2, 1)[:h, :w]
    v = v.repeat(2, 0).repeat(2, 1)[:h, :w]
    r = y + 1.402 * v
    g = y - 0.344136 * u - 0.714136 * v
    b = y + 1.772 * u
    return np.clip(np.stack((r, g, b), -1), 0, 255).astype(np.uint8)


class NpuDetector:
    """HTTP client for the in-container C7x/TIDL inference server (npu_server.py).
    Same call signature as YoloxDetector; raises on server failure so callers
    can fall back."""

    def __init__(self, url: str = "http://127.0.0.1:8901", timeout: float = 10.0):
        import urllib.request
        self.url = url.rstrip("/")
        self.timeout = timeout
        # fail fast at construction if the server isn't up
        with urllib.request.urlopen(f"{self.url}/healthz", timeout=5) as r:
            info = __import__("json").load(r)
        if not info.get("ok"):
            raise RuntimeError(f"npu server not ready: {info}")
        self.ep = info.get("ep", "?")
        log.info("npu detector ready: %s (ep=%s)", self.url, self.ep)

    def __call__(self, rgb: np.ndarray, min_score: float = 0.45,
                 nms_iou: float = 0.45, max_det: int = 20) -> list[dict]:
        import json as _json
        import struct as _struct
        import urllib.request
        h, w = rgb.shape[:2]
        body = _struct.pack(">HH", w, h) + np.ascontiguousarray(rgb).tobytes()
        rq = urllib.request.Request(f"{self.url}/detect", data=body,
                                    headers={"Content-Type": "application/octet-stream"})
        with urllib.request.urlopen(rq, timeout=self.timeout) as r:
            out = _json.load(r)
        if "error" in out:
            raise RuntimeError(out["error"])
        dets = [d for d in out.get("detections", []) if d["score"] >= min_score]
        dets.sort(key=lambda d: -d["score"])
        return dets[:max_det]
