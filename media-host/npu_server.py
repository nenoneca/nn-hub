#!/usr/bin/env python3
"""
nn NPU inference server — runs INSIDE the edgeai LXC container as PID 1.

Serves YOLOX-S-lite object detection on the J722S C7x NPU (onnxruntime-TIDL)
over plain HTTP on 127.0.0.1:8901 (the container shares the host network
namespace, so the media server reaches it as localhost).

Deliberately minimal init: this process IS the container — no systemd, no
udev, no logind, no avahi.  (The TI rootfs's full systemd, sharing the host
/dev and netns, once impersonated host services on the shared D-Bus and took
the host's SSH session setup down for hours.  Never boot it again.)

Protocol:
  GET  /healthz           -> {"ok":true,"model":...,"ep":...}
  POST /detect            body: [u16 w][u16 h][rgb888 bytes]
                          -> [{"cls","score","box":[x,y,w,h]}]  (frame coords)
"""

import json
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import numpy as np

MODEL_DIR = "/opt/model_zoo/ONR-OD-8220-yolox-s-lite-mmdet-coco-640x640"
PORT = 8901
SIZE = 640
MIN_KEEP = 0.05          # server-side floor; client applies its own threshold

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

_lock = threading.Lock()
_sess = None
_input_name = None
_ep_used = "?"
_model_file = "?"


def _find_model():
    import os
    mdl_dir = os.path.join(MODEL_DIR, "model")
    onnx = [f for f in os.listdir(mdl_dir) if f.endswith(".onnx")]
    return os.path.join(mdl_dir, onnx[0]), os.path.join(MODEL_DIR, "artifacts")


def _load():
    global _sess, _input_name, _ep_used, _model_file
    import onnxruntime as ort
    model, artifacts = _find_model()
    _model_file = model
    so = ort.SessionOptions()
    so.log_severity_level = 3
    # TI dl-inferer recipe (edgeai_dl_inferer.py): on-target RUN mode needs
    # tidl_tools_path="null" (else the EP tries to compile → abort) + core_number.
    runtime_options = {"tidl_tools_path": "null",
                       "artifacts_folder": artifacts,
                       "core_number": 1}
    try:
        _sess = ort.InferenceSession(
            model, sess_options=so,
            providers=["TIDLExecutionProvider", "CPUExecutionProvider"],
            provider_options=[runtime_options, {}])
        _ep_used = "TIDL"
    except Exception as e:
        print(f"!! TIDL EP failed ({e}); CPU fallback", flush=True)
        _sess = ort.InferenceSession(
            model, providers=["CPUExecutionProvider"])
        _ep_used = "CPU"
    _input_name = _sess.get_inputs()[0].name
    inp = _sess.get_inputs()[0]
    print(f">> model {model} ep={_ep_used} input={inp.shape} {inp.type}",
          flush=True)


def _preprocess(rgb: np.ndarray):
    """Match TI param.yaml: resize_with_pad (corner, pad=114), reverse_channels
    (BGR), uint8, NCHW.  Returns (blob, ratio) where ratio maps model→frame."""
    from PIL import Image
    h, w = rgb.shape[:2]
    r = min(SIZE / w, SIZE / h)
    nw, nh = int(round(w * r)), int(round(h * r))
    img = Image.fromarray(rgb).resize((nw, nh), Image.BILINEAR)
    canvas = np.full((SIZE, SIZE, 3), 114, np.uint8)
    canvas[:nh, :nw] = np.asarray(img)               # corner (top-left) pad
    bgr = canvas[:, :, ::-1]                          # reverse_channels
    dtype = np.uint8 if "uint8" in _sess.get_inputs()[0].type else np.float32
    blob = np.ascontiguousarray(bgr.astype(dtype).transpose(2, 0, 1)[None])
    return blob, r


def _postprocess(outs, w, h, ratio):
    """TI od-8220 outputs: dets[N,5]=(x1,y1,x2,y2,score) + labels[N], coords in
    the 640 letterboxed space (corner pad → divide by ratio to reach frame)."""
    dets = []
    boxes = outs[0].reshape(-1, outs[0].shape[-1])
    labels = outs[1].reshape(-1) if len(outs) > 1 else np.zeros(len(boxes))
    for b, l in zip(boxes, labels):
        score = float(b[4])
        if score < MIN_KEEP:
            continue
        x1, y1, x2, y2 = (float(b[0]) / ratio, float(b[1]) / ratio,
                          float(b[2]) / ratio, float(b[3]) / ratio)
        ci = int(l)
        name = COCO[ci] if 0 <= ci < len(COCO) else str(ci)
        dets.append({"cls": name, "score": round(score, 3),
                     "box": [int(x1), int(y1), int(x2 - x1), int(y2 - y1)]})
    return dets


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path == "/healthz":
            self._json(200, {"ok": _sess is not None, "ep": _ep_used,
                             "model": _model_file})
        else:
            self._json(404, {"error": "unknown path"})

    def do_POST(self):
        if self.path != "/detect":
            return self._json(404, {"error": "unknown path"})
        try:
            n = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(n)
            w, h = struct.unpack(">HH", body[:4])
            rgb = np.frombuffer(body[4:4 + w * h * 3], np.uint8).reshape(h, w, 3)
            t0 = time.time()
            with _lock:
                blob, ratio = _preprocess(rgb)
                outs = _sess.run(None, {_input_name: blob})
            dets = _postprocess(outs, w, h, ratio)
            self._json(200, {"ms": int((time.time() - t0) * 1000),
                             "detections": dets})
        except Exception as e:
            self._json(500, {"error": str(e)})


if __name__ == "__main__":
    print(">> nn NPU server starting", flush=True)
    _load()
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
