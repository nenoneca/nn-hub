#!/usr/bin/env python3
"""
nn media — video streaming service.

This is the endpoint the C6 streams its (encrypted) H.264 to.  It:
  1. terminates the nn_sectun secure session and decrypts the H.264 byte-stream,
  2. feeds it into a live GStreamer pipeline (appsrc ! tee),
  3. exposes a small HTTP control API so a consumer (the hub) can ask it to
     "append a GST element at the end of the pipeline" — currently an RTP/UDP
     branch (tee. ! rtph264pay ! udpsink host=<hub> port=<n>) — to hand the
     video off to that consumer in real time.

     C6 ─enc H.264─▶ [appsrc ! tee] ─┬─(add branch)─▶ rtph264pay ! udpsink ─▶ hub
                                      └──────────────▶ appsink ─▶ PyAV decode (YUV)
                                                                  ─▶ motion detection

A second, always-on split off the SAME tee decodes the H.264 to YUV frames and,
every t ms (200 default), computes the motion difference, finds contour blobs and
their bounding boxes, and NMS-merges them — exposed as a box list at GET /motion.

Test mode (--test-file foo.h264) skips the C6 and loops a raw Annex-B file into
the pipeline instead, so the RTP + detection paths can be exercised without hardware.

Control API (aiohttp):
  GET  /status                       → pipeline + branch list + detection on?
  POST /branch  {host, port, pt?}    → add an RTP/UDP branch, returns {id, pt, ssrc?}
  DELETE /branch/{id}                → remove a branch
  GET  /motion                       → latest {ts, count, boxes:[[x,y,w,h]...], frame}

Needs: python3-gi + GStreamer (tee, appsink), numpy + scipy, PyAV (H.264 decode).
"""
import argparse
import asyncio
import json
import os
import collections
import faulthandler
import heapq
import socket
import struct
import sys
import threading
import traceback
import urllib.request as _urlreq
import time
from pathlib import Path

# ── fail loudly, never limp ─────────────────────────────────────────────────
# Under the nn-video supervisor this process is ONE slot; the supervisor
# restarts it in seconds.  So: a native crash prints the Python stack
# (faulthandler) instead of a bare "Segmentation fault", and an uncaught
# exception in ANY thread ends the process (exit 43) instead of leaving a
# dead daemon thread behind a control API that still answers — the "zombie
# slot" that looked alive for hours while no video moved (2026-09).
faulthandler.enable()
EXIT_THREAD_EXCEPTION = 43
EXIT_GST_ERROR = 44
EXIT_STATE_TIMEOUT = 45


SINGLE_PROCESS = False      # set by nnvideo: one process hosts every camera, never exit for one


def _camera_tag() -> str:
    """Which camera the calling code works for: the pipeline run's context
    variable, else the helper thread's name ("hls-ts:cam3")."""
    try:
        from nnvideo.pipeline import current_camera
        c = current_camera.get("")
        if c:
            return c
    except Exception:
        pass
    n = threading.current_thread().name
    return n.rsplit(":", 1)[1] if ":" in n else ""


_builtin_print = print


def print(*args, **kwargs):                                     # noqa: A001
    """In the one-process service every line names its camera so the
    hub's log page can filter by camera: '[cam3/svc] >> HLS: …'."""
    if SINGLE_PROCESS:
        c = _camera_tag()
        if c:
            args = (f"[{c}/svc]",) + args
    _builtin_print(*args, **kwargs)


def _thread_excepthook(args):
    if args.exc_type is SystemExit:
        return
    name = getattr(args.thread, "name", "?")
    print(f">> {'' if SINGLE_PROCESS else 'FATAL: '}uncaught {args.exc_type.__name__} in thread "
          f"{name}: {args.exc_value}", file=sys.stderr, flush=True)
    traceback.print_exception(args.exc_type, args.exc_value, args.exc_traceback)
    sys.stderr.flush()
    if not SINGLE_PROCESS:
        os._exit(EXIT_THREAD_EXCEPTION)


threading.excepthook = _thread_excepthook

import numpy as np
from scipy import ndimage

import gi
gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib  # noqa: E402

from aiohttp import web  # noqa: E402

# Live-view page: raw P4 H.264 over WebSocket -> jMuxer/MSE -> browser HW decoder.
# Passthrough, no server transcode; the browser's decoder handles the P4 stream.
LIVE_HTML = """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>nn camera live</title>
<style>html,body{margin:0;height:100%;background:#000}
#v{width:100vw;height:100vh;object-fit:contain;background:#000}
#ov{position:fixed;inset:0;width:100vw;height:100vh;pointer-events:none;z-index:5}
#det{position:fixed;bottom:6px;left:8px;color:#0ff;font:12px monospace;opacity:.85;z-index:6}
#s{position:fixed;top:6px;left:8px;color:#0f0;font:12px monospace;opacity:.7}
#gear{position:fixed;top:6px;right:10px;font:20px sans-serif;color:#ccc;cursor:pointer;
  opacity:.7;user-select:none;z-index:11}
#gear:hover{opacity:1}
#cfg{position:fixed;top:40px;right:10px;width:270px;background:rgba(18,18,22,.94);
  color:#ddd;font:13px monospace;border:1px solid #444;border-radius:8px;
  padding:10px 12px;display:none;z-index:10}
#cfg h3{margin:0 0 8px;font:bold 13px monospace;color:#8dc}
#cfg .row{display:flex;justify-content:space-between;align-items:center;margin:4px 0}
#cfg label{flex:1}
#cfg input{width:84px;background:#111;color:#0f0;border:1px solid #555;border-radius:4px;
  font:13px monospace;padding:2px 4px;text-align:right}
#cfg .ro{color:#888}
#cfg .hint{color:#777;font-size:11px;margin-top:6px}
#cfg .ok{color:#5f5}</style>
</head><body>
<video id=v autoplay muted playsinline></video>
<canvas id=ov></canvas>
<div id=s>connecting...</div>
<div id=det></div>
<div id=gear title="camera settings">&#9881;</div>
<div id=cfg>
 <h3>camera settings <span id=age class=ro></span></h3>
 <div id=rows></div>
 <div class=hint>wb_* apply live &amp; persist across reboots; greyed values are owned by the camera's auto loops</div>
 <div id=msg class=ok></div>
</div>
<script>
// jmuxer needs Media Source Extensions (MediaSource).  iPhone Safari does NOT
// implement MSE (Apple restricts it to macOS/iPadOS; iPhone is HLS/WebRTC only),
// so the jmuxer <video> never plays there.  Fall back to the WebRTC viewer,
// which iOS Safari fully supports (H.264 over WebRTC).
if (typeof window.MediaSource === 'undefined' &&
    typeof window.ManagedMediaSource === 'undefined') {
    location.replace('/webrtc');
}
</script>
<div id=nnb style="position:fixed;bottom:10px;right:12px;z-index:9;color:#ffd479;font:12px/1 ui-monospace,monospace;background:rgba(0,0,0,.45);padding:4px 7px;border-radius:6px"></div>
<script>
// yolo timing + capture timestamp badge (shared across /live, /hls, /webrtc).
// yolo_ms is the event engine's LAST inference — it stays 0 until motion has
// triggered YOLO at least once (a black static scene never does).
window.__nnts = 0;                                  // ws text frames override (per-frame accurate)
function nnFmtTs(ms){
  if(!ms) return "";
  if(ms < 1e12){ var t=Math.floor(ms/1000);        // unsynced device -> uptime
    return "up "+Math.floor(t/3600)+":"+String(Math.floor(t/60)%60).padStart(2,"0")+":"+String(t%60).padStart(2,"0"); }
  var d=new Date(ms);
  return d.toLocaleTimeString(undefined,{hour12:false})+"."+String(d.getMilliseconds()).padStart(3,"0");
}
setInterval(async function(){
  var y=0, ts=window.__nnts;
  try{ var j=await (await fetch("/api/event/live")).json(); y=j.yolo_ms||0; }catch(e){}
  if(!ts){ try{ var m=await (await fetch("/motion")).json(); ts=m.ts||0; }catch(e){} }
  document.getElementById("nnb").textContent="yolo="+y+"ms"+(ts?"  "+nnFmtTs(ts):"");
}, 1000);
</script>
<script src="/jmuxer.min.js"></script>
<script>
var v=document.getElementById('v'), s=document.getElementById('s'), n=0, t0=Date.now();
var jm=new JMuxer({node:v, mode:'video', flushingTime:0, fps:30, clearBuffer:true, debug:false});
function conn(){
  var ws=new WebSocket((location.protocol=='https:'?'wss://':'ws://')+location.host+'/ws');
  ws.binaryType='arraybuffer';
  ws.onopen=function(){ s.textContent='live'; };
  ws.onmessage=function(e){
    if(typeof e.data==='string'){                       // {"ts":...} capture time
      try{ window.__nnts = (JSON.parse(e.data).ts)||0; }catch(x){}
      return;                                           // never feed text to jmuxer
    }
    jm.feed({video:new Uint8Array(e.data)}); n++;
    if(Date.now()-t0>1000){ s.textContent='live ~'+n+' fps'; n=0; t0=Date.now(); } };
  ws.onclose=function(){ s.textContent='reconnecting...'; setTimeout(conn,1000); };
  ws.onerror=function(){ ws.close(); };
}
conn();

// -- detection overlay (NPU boxes over the live video) --
// The video is object-fit:contain, so the actual picture is letterboxed inside
// the element box; compute that inner rect and map frame coords into it.
var ov=document.getElementById("ov"), octx=ov.getContext("2d"), dinfo=document.getElementById("det");
async function drawDets(){
  try{
    var r=await fetch("/api/event/live"); var j=await r.json();
    var vw=j.w||1280, vh=j.h||720;
    var cw=window.innerWidth, ch=window.innerHeight;
    ov.width=cw; ov.height=ch;
    octx.clearRect(0,0,cw,ch);
    // object-fit:contain -> uniform scale, centred with letterbox bars
    var scale=Math.min(cw/vw, ch/vh);
    var dw=vw*scale, dh=vh*scale, ox=(cw-dw)/2, oy=(ch-dh)/2;
    var ds=j.detections||[];
    octx.lineWidth=2; octx.font="14px monospace"; octx.textBaseline="bottom";
    var nb=0;
    ds.forEach(function(d){
      var b=d.box; if(!b||b.length<4) return; nb++;
      var x=ox+b[0]*scale, y=oy+b[1]*scale, w=b[2]*scale, h=b[3]*scale;
      var col=d.cls==="person"?"#0f0":"#ff0";
      octx.strokeStyle=col; octx.strokeRect(x,y,w,h);
      octx.fillStyle=col; octx.fillText(d.cls+" "+d.score.toFixed(2), x+2, y-2);
    });
    var age=j.ts?((Date.now()-j.ts)/1000).toFixed(1)+"s":"-";
    dinfo.textContent=(j.active?"● REC  ":"")+nb+" obj  yolo="+(j.yolo_ms||0)+"ms  age="+age;
  }catch(e){}
}
setInterval(drawDets, 350);

// ── settings overlay ────────────────────────────────────────────────────
// Only controls the camera actually honors are editable.  The rest are
// owned by on-camera auto loops (AE rewrites exposure/gain every few frames;
// the IPA enhancement loop owns contrast; A/B-tested: manual sets are no-ops).
var EDIT=[['wb_r','WB red (milli)'],['wb_b','WB blue (milli)'],
          ['focus','focus 0-1023 (AF may re-adjust)']];
var RO=[['exposure','exposure — auto (AE)'],['gain','gain — auto (AE)'],
        ['contrast','contrast — auto (enhance)'],['brightness','brightness — auto'],
        ['saturation','saturation — auto'],['bitrate','bitrate — adaptive'],
        ['gop','gop — adaptive'],['fps_div','fps divisor'],['res','resolution'],
        ['fps','base fps']];
var cfg=document.getElementById('cfg'), rows=document.getElementById('rows'),
    msg=document.getElementById('msg'), age=document.getElementById('age'),
    open_=false, timer=null;
document.getElementById('gear').onclick=function(){
  open_=!open_; cfg.style.display=open_?'block':'none';
  if(open_){ refresh(); timer=setInterval(refresh,3000); }
  else clearInterval(timer);
};
function mkrow(k,label,ro){
  var d=document.createElement('div'); d.className='row';
  d.innerHTML='<label'+(ro?' class=ro':'')+'>'+label+'</label>';
  if(ro){ var sp=document.createElement('span'); sp.className='ro'; sp.id='f_'+k; d.appendChild(sp); }
  else{ var i=document.createElement('input'); i.id='f_'+k; i.type='text';
    i.onchange=function(){ apply(k, i.value); };
    d.appendChild(i); }
  rows.appendChild(d); return d;
}
EDIT.forEach(function(e){mkrow(e[0],e[1],false)});
RO.forEach(function(e){mkrow(e[0],e[1],true)});
function refresh(){
  fetch('/api/camera/settings').then(function(r){return r.json()}).then(function(j){
    if(!j.settings){ age.textContent='(no report yet)'; return; }
    age.textContent='('+j.age_s+'s ago)';
    Object.keys(j.settings).forEach(function(k){
      var el=document.getElementById('f_'+k); if(!el) return;
      if(el.tagName==='INPUT'){ if(document.activeElement!==el) el.value=j.settings[k]; }
      else el.textContent=j.settings[k];
    });
  }).catch(function(){ age.textContent='(fetch failed)'; });
}
function apply(k,val){
  var body={}; body[k]=parseInt(val,10);
  if(isNaN(body[k])){ msg.textContent='invalid value'; return; }
  fetch('/api/camera/set',{method:'POST',headers:{'content-type':'application/json'},
    body:JSON.stringify(body)}).then(function(r){return r.json()}).then(function(j){
      msg.textContent=j.applied?('applied '+JSON.stringify(j.applied)):('error: '+JSON.stringify(j));
      setTimeout(function(){msg.textContent=''},2500);
    }).catch(function(e){ msg.textContent='send failed'; });
}
</script></body></html>"""

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, "/home/chalos/ext/mx500/nn_project_nowest/hub")

# NB: do NOT claim alignment=au here.  The ingest pushes the H.264 in arbitrary
# ~4 KB chunks (see svc.feed), so a buffer is NOT a complete access unit.  If we
# assert alignment=au, h264parse trusts it and stops re-parsing, and hardware
# decoders are then fed malformed AUs: on the CIX P1 (Linlon/MVX) every v4l2
# decode branch silently stalls (device opens, no bus error, zero output — HLS
# produced no segments and the detection branch never delivered a frame).
# Leaving alignment unset lets h264parse assemble real AUs.
APPSRC_CAPS = "video/x-h264,stream-format=byte-stream"

# ── C6 uplink record demux (typed A/V records, emitted in timestamp order) ───
NN_REC_VIDEO = 0x56   # 'V' — H.264 (Annex-B) fragment payload
NN_REC_AUDIO = 0x41   # 'A' — one AAC (ADTS) frame
NN_REC_STATUS = 0x53  # 'S' — camera settings JSON (device -> UI)
NN_REC_DETECT = 0x44  # 'D' — edge inference detections (see nn_infer DESIGN.md)
NN_REC_CFGACK = 0x4B  # 'K' — device ack of a pushed config {v, applied}
NN_REC_HEART  = 0x48  # 'H' — device heartbeat {up_s, fps, drops, v}
NN_REC_HDR   = 16     # [u8 type][u8 flags][u16 seq][u64 ts_ms abs-epoch][u32 len]


def parse_detect_record(payload: bytes):
    """'D' 0x44 payload: [u8 ver][u8 count][u16 rsvd] + count x
    {u16 x,y,w,h (stream px), u16 class_id, u16 conf_x1000} — see
    nn_infer/DESIGN.md.  Returns event-engine det dicts, or None."""
    try:
        if len(payload) < 4 or payload[0] != 0:
            return None
        n = payload[1]
        out = []
        for i in range(n):
            o = 4 + i * 12
            x, y, w, h, cls, conf = struct.unpack_from("<6H", payload, o)
            out.append({"cls": COCO_CLASSES[cls] if cls < len(COCO_CLASSES) else str(cls),
                        "score": conf / 1000.0, "box": [x, y, w, h]})
        return out
    except Exception:
        return None


COCO_CLASSES = ["person","bicycle","car","motorcycle","airplane","bus","train",
    "truck","boat","traffic light","fire hydrant","stop sign","parking meter",
    "bench","bird","cat","dog","horse","sheep","cow","elephant","bear","zebra",
    "giraffe","backpack","umbrella","handbag","tie","suitcase","frisbee","skis",
    "snowboard","sports ball","kite","baseball bat","baseball glove","skateboard",
    "surfboard","tennis racket","bottle","wine glass","cup","fork","knife",
    "spoon","bowl","banana","apple","sandwich","orange","broccoli","carrot",
    "hot dog","pizza","donut","cake","chair","couch","potted plant","bed",
    "dining table","toilet","tv","laptop","mouse","remote","keyboard",
    "cell phone","microwave","oven","toaster","sink","refrigerator","book",
    "clock","vase","scissors","teddy bear","hair drier","toothbrush"]


class RecordParser:
    def _parse_direct(self, b: bytes):
        out = []
        n = len(b); o = 0
        while n - o >= NN_REC_HDR:
            ln = b[o+12] | (b[o+13] << 8) | (b[o+14] << 16) | (b[o+15] << 24)
            if b[o] not in (NN_REC_VIDEO, NN_REC_AUDIO, NN_REC_STATUS, NN_REC_DETECT,
                            NN_REC_CFGACK, NN_REC_HEART) or ln > 2 * 1024 * 1024:
                o += 1
                continue
            if n - o < NN_REC_HDR + ln:
                break
            out.append((b[o], b[o+1], b[o+2] | (b[o+3] << 8), int.from_bytes(b[o+4:o+12], "little"),
                        b[o+NN_REC_HDR:o+NN_REC_HDR+ln]))
            o += NN_REC_HDR + ln
        return out, b[o:]

    """Reassemble [type][flags][seq][ts_ms][len][payload] records from the C6
    byte stream (records may span / share nn_sectun recv() boundaries).

    Uses a read OFFSET instead of front-deleting each record (which is O(n) per
    record → O(n²) per burst and starves the socket read under the C6's bursty
    128 KB SBUF flushes); compacts only when the consumed prefix grows large."""
    def __init__(self):
        self.buf = bytearray()
        self.off = 0

    def feed(self, data: bytes):
        if not self.buf:
            # fast path: nothing pending, so parse straight out of the
            # plaintext record; only a record that spans into the next
            # sectun record goes through the reassembly buffer
            out, tail = self._parse_direct(data)
            if tail:
                self.buf += tail
            return out
        self.buf += data
        out = []
        b = self.buf
        n = len(b)
        o = self.off
        while n - o >= NN_REC_HDR:
            ln = b[o+12] | (b[o+13] << 8) | (b[o+14] << 16) | (b[o+15] << 24)
            # Desync (bogus type or length): clearing the buffer can't realign a
            # mid-record TCP stream — byte-scan forward to the next plausible
            # header instead (false matches self-correct on the next record).
            if b[o] not in (NN_REC_VIDEO, NN_REC_AUDIO, NN_REC_STATUS, NN_REC_DETECT,
                            NN_REC_CFGACK, NN_REC_HEART) or ln > 2 * 1024 * 1024:
                o += 1
                continue
            if n - o < NN_REC_HDR + ln:
                break
            typ = b[o]; flags = b[o+1]
            seq = b[o+2] | (b[o+3] << 8)
            ts = int.from_bytes(b[o+4:o+12], "little")    # u64 absolute epoch ms
            payload = bytes(b[o+NN_REC_HDR:o+NN_REC_HDR+ln])
            o += NN_REC_HDR + ln
            out.append((typ, flags, seq, ts, payload))
        # compact consumed prefix occasionally (amortized O(1) per record)
        if o:
            del b[:o]
            o = 0
        self.off = o
        return out


class AudioSink:
    """PyAV-decode the AAC (ADTS) records to confirm audio is live: tracks frame
    count, byte count, last device ts, and RMS level.  Optionally tees the raw
    ADTS to a .aac file."""
    def __init__(self, path: str = None):
        import av
        self.codec = av.CodecContext.create("aac", "r")
        self.frames = 0
        self.bytes = 0
        self.last_ts = 0
        self.level = 0.0
        self.f = open(path, "wb") if path else None

    def feed(self, adts: bytes, ts: int):
        self.bytes += len(adts)
        self.last_ts = ts
        if self.f:
            self.f.write(adts)
            self.f.flush()
        try:
            for pkt in self.codec.parse(adts):
                for fr in self.codec.decode(pkt):
                    a = fr.to_ndarray().astype(np.float32)
                    self.level = float(np.sqrt(np.mean(a * a)))
                    self.frames += 1
        except Exception:
            pass

    def status(self):
        return {"enabled": True, "frames": self.frames, "bytes": self.bytes,
                "last_ts": self.last_ts, "level": round(self.level, 1)}


class AVSync:
    """A/V-sync element: the C6 now relays records in arrival order (no on-chip
    reorder), so this orders the demuxed records by their ABSOLUTE device
    timestamp before they go downstream (video→H.264, audio→AAC) and on to RTP.
    A min-heap keyed by (ts, arrival) releases records once they fall a bounded
    window behind the newest timestamp — so late-but-earlier audio slots ahead
    of video, while a frame's same-ts fragments stay contiguous + in order."""
    def __init__(self, on_video, on_audio, window_ms=200, max_buf=512):
        self.on_video = on_video
        self.on_audio = on_audio
        self.window = window_ms
        self.max_buf = max_buf
        self.heap = []
        self._n = 0
        self.max_ts = 0
        self.released = 0
        self.reordered = 0
        self._last_emit_ts = 0

    def feed(self, typ, ts, payload):
        if ts > self.max_ts:
            self.max_ts = ts
        heapq.heappush(self.heap, (ts, self._n, typ, payload))
        self._n += 1
        cutoff = self.max_ts - self.window
        while self.heap and (self.heap[0][0] <= cutoff or len(self.heap) > self.max_buf):
            self._emit(heapq.heappop(self.heap))

    def flush(self):
        while self.heap:
            self._emit(heapq.heappop(self.heap))

    def _emit(self, item):
        ts, _, typ, payload = item
        if ts < self._last_emit_ts:
            self.reordered += 1     # released after a later-ts record already went
        self._last_emit_ts = ts
        self.released += 1
        if typ == NN_REC_VIDEO:
            self.on_video(payload, ts)
        elif typ == NN_REC_AUDIO:
            self.on_audio(payload, ts)

    def status(self):
        return {"window_ms": self.window, "buffered": len(self.heap),
                "released": self.released, "reordered": self.reordered}


# ── motion detection (decode → YUV → diff → contours → NMS → boxes) ──────────
def nms(boxes, iou_thresh=0.3):
    """Greedy non-max suppression on [x,y,w,h] boxes, larger area wins."""
    if not boxes:
        return []
    b = np.asarray(boxes, dtype=float)
    x1, y1 = b[:, 0], b[:, 1]
    x2, y2 = b[:, 0] + b[:, 2], b[:, 1] + b[:, 3]
    area = b[:, 2] * b[:, 3]
    order = area.argsort()[::-1]
    keep = []
    while order.size:
        i = order[0]
        keep.append(int(i))
        xx1 = np.maximum(x1[i], x1[order[1:]])
        yy1 = np.maximum(y1[i], y1[order[1:]])
        xx2 = np.minimum(x2[i], x2[order[1:]])
        yy2 = np.minimum(y2[i], y2[order[1:]])
        iw = np.maximum(0.0, xx2 - xx1)
        ih = np.maximum(0.0, yy2 - yy1)
        inter = iw * ih
        iou = inter / (area[i] + area[order[1:]] - inter + 1e-6)
        order = order[1:][iou <= iou_thresh]
    return [boxes[i] for i in keep]


class MotionDetector:
    """The 'detection element': input is decoded YUV (I420) luma frames.  Every
    `interval_ms` it diffs the current luma against the luma at the previous tick
    (so it captures motion over the whole window), thresholds + dilates into a
    motion mask, labels connected components (contour blobs), takes their
    bounding boxes, drops tiny ones, and NMS-merges overlaps -> many boxes."""

    def __init__(self, interval_ms=200, diff_thresh=18, min_area_frac=0.0008,
                 proc_width=480, iou_thresh=0.3):
        self.interval = interval_ms / 1000.0
        self.diff_thresh = diff_thresh
        self.min_area_frac = min_area_frac
        self.proc_width = proc_width
        self.iou_thresh = iou_thresh
        self._prev = None
        self._prev_small = None          # downscaled int16 luma of the previous tick
        self._last_tick = 0.0
        self._lock = threading.Lock()
        self._latest = {"ts": 0, "count": 0, "boxes": [], "frame": None}
        self._last_color = None          # (I420 ndarray copy, w, h) for /snapshot.jpg
        self.frames = 0
        self.ticks = 0

    def feed_yuv(self, yuv, w, h, now=None):
        """Legacy entry (an I420 ndarray): wraps it and defers to feed_frame."""
        from nnvideo.frame import Frame
        return self.feed_frame(Frame.from_i420_array(yuv, w, h), now)

    def feed_frame(self, frame, now=None):
        """One decoded frame, read as views — nothing is copied at full
        resolution.  The comparison runs on a downscaled int16 luma (the
        same downscale _detect always did), and THAT is what is kept from
        one tick to the next, so the frame's buffer goes back to the decoder
        as soon as the tick ends.  The on_tick callback receives the Frame;
        whoever takes it for inference owns its release."""
        self.frames += 1
        now = time.monotonic() if now is None else now
        if now - self._last_tick < self.interval:
            frame.release()
            return
        self._last_tick = now
        w, h = frame.w, frame.h
        f = max(1, w // self.proc_width)
        a = frame.y[::f, ::f].astype(np.int16)                 # the only full-frame read
        thumb = a.astype(np.uint8)
        # debug routes (/snapshot raw, /mjpeg) and the snapshot fallback read
        # these; a downscaled luma is what they get now instead of a 3 MB copy
        self._prev = thumb
        self._last_color = (thumb, thumb.shape[1], thumb.shape[0])
        prev = self._prev_small
        self._prev_small = a
        if prev is None or prev.shape != a.shape:
            frame.release()
            return
        boxes, ratio = self._detect_small(prev, a, w, h, f)
        self.ticks += 1
        with self._lock:
            self._latest = {"ts": int(now * 1000), "count": len(boxes),
                            "boxes": boxes, "frame": {"w": w, "h": h}}
        cb = getattr(self, "on_tick", None)
        if cb is not None:
            try:
                cb(int(time.time() * 1000), ratio, boxes, frame)
                return                                            # the callback owns the frame now
            except Exception as e:
                print(f">> event tick error: {e}", file=sys.stderr, flush=True)
        frame.release()

    def _detect(self, prev, cur, w, h):
        f = max(1, w // self.proc_width)                 # downscale for speed
        return self._detect_small(prev[::f, ::f].astype(np.int16), cur[::f, ::f].astype(np.int16), w, h, f)

    def _detect_small(self, b, a, w, h, f):
        mask = np.abs(a - b) > self.diff_thresh           # motion difference
        ratio = float(mask.mean())
        if not mask.any():
            return [], 0.0
        mask = ndimage.binary_dilation(mask, iterations=2)   # merge nearby pixels
        lbl, n = ndimage.label(mask)                      # contours = components
        if n == 0:
            return [], ratio
        min_area = self.min_area_frac * w * h
        boxes = []
        for sy, sx in ndimage.find_objects(lbl):
            x, yy = int(sx.start * f), int(sy.start * f)
            bw, bh = int((sx.stop - sx.start) * f), int((sy.stop - sy.start) * f)
            if bw * bh >= min_area:
                boxes.append([x, yy, bw, bh])
        return nms(boxes, self.iou_thresh), ratio

    def latest(self):
        with self._lock:
            d = dict(self._latest)
        d.update(enabled=True, frames=self.frames, ticks=self.ticks,
                 interval_ms=int(self.interval * 1000))
        return d


class VideoService:
    # ── single-process (nnvideo) support ─────────────────────────────────
    # One process now hosts every camera, so nothing per-camera may come
    # from the process environment.  These attributes carry what the old
    # per-slot units passed as Environment=; the env stays as the fallback
    # for the legacy single-slot mode.
    cam_id = ""                 # NN_CAM_ID
    hls_opts: dict = {}         # NN_HLS_* / NN_TRANSCODED overrides per camera
    on_wedged = None            # nnvideo: called instead of os._exit(42)
    on_fatal = None             # nnvideo: called (reason) instead of os._exit on a graph error
    on_frame = None             # nnvideo: decode branch → fn(yuv, w, h) instead of the in-graph detector
    closed = False              # nnvideo: set by close(); every helper loop of this graph exits on it
    _hls_procs = None           # child processes (ffmpeg, transcoder) this graph started

    def _track(self, proc):
        """Remember a child so close() can end it with the graph."""
        if self._hls_procs is None:
            self._hls_procs = set()
        self._hls_procs.add(proc)
        return proc

    def close(self):
        """Stop this graph's helper loops (HLS muxer respawn, watchdog,
        snapshot saver) and their children.  Without this a rebuilt camera
        left the old muxer thread respawning ffmpeg every 2 s against a port
        that no longer exists (seen on the OPi after the first operator
        reset, 2026-09-21)."""
        self.closed = True
        dm = getattr(self, "_dma_maps", None)
        if dm is not None:
            dm.close()
        for proc in list(self._hls_procs or ()):
            try:
                if proc.poll() is None:
                    proc.terminate()
            except Exception:
                pass

    def _tagged(self, fn):
        """Wrap a GLib callback so the camera-tag context is set while it
        runs (the GLib main loop is shared by every camera's graph)."""
        def run(*a, **k):
            try:
                from nnvideo.pipeline import current_camera
                tok = current_camera.set(self.cam_id)
            except Exception:
                tok = None
            try:
                return fn(*a, **k)
            finally:
                if tok is not None:
                    current_camera.reset(tok)
        return run

    def _feed_frame(self, frame) -> None:
        hook = self.on_frame
        if hook is not None:
            hook(frame)
        elif self._det is not None:
            self._det.feed_frame(frame)

    def _fatal(self, reason: str, code: int) -> None:
        print(f">> FATAL: {reason}", file=sys.stderr, flush=True)
        cb = self.on_fatal
        if cb is not None:
            try:
                cb(reason)
            except Exception as e:                       # noqa: BLE001
                print(f">> on_fatal failed: {e}", file=sys.stderr, flush=True)
            return
        os._exit(code)

    def opt(self, name: str, default=None):
        """Per-camera option: hls_opts first, then the environment."""
        v = self.hls_opts.get(name)
        return v if v is not None else os.environ.get(name, default)

    # Fixed-QP rate control for the HW re-encode branches, 0 = off (use the
    # encoder's own bitrate control).  Needed on the CIX P1 (Orange Pi 6 Plus):
    # its v4l2h264enc IGNORES video_bitrate/video_bitrate_mode entirely (proven:
    # identical output at 2 vs 20 Mbps) but DOES honour h264_*_frame_qp_value.
    # The beagle's WAVE5 honours video_bitrate, so leave this 0 there.
    enc_qp = 0

    # HLS segment dir, set from --hls-dir in main().  grab_snapshot_jpeg() reads
    # the newest complete segment from here, so it must track the real dir when a
    # second instance runs beside this one.
    hls_dir = None

    # Loopback TCP port feeding the HLS ffmpeg; per-instance (see --hls-port).
    hls_port = 5566

    # ── edge inference (nn_infer) ────────────────────────────────────────────
    # edge_caps: what the device advertised in its status record ("infer" key);
    # infer_mode: hub-adjustable 'auto'|'on'|'off' for THIS service's own
    # inference (persisted in the keydir).  Effective local inference =
    # on, or auto with no edge capability advertised.
    edge_caps = None
    infer_mode = "auto"
    infer_mode_path = None

    def load_infer_mode(self, keydir):
        import os as _os
        self.infer_mode_path = _os.path.join(keydir, "infer_mode")
        try:
            m = open(self.infer_mode_path).read().strip()
            if m in ("auto", "on", "off"):
                self.infer_mode = m
        except Exception:
            pass

    def set_infer_mode(self, mode):
        if mode not in ("auto", "on", "off"):
            return False
        self.infer_mode = mode
        try:
            if self.infer_mode_path:
                open(self.infer_mode_path, "w").write(mode)
        except Exception:
            pass
        self.apply_infer_mode()
        return True

    def local_infer_active(self):
        return self.infer_mode == "on" or (self.infer_mode == "auto"
                                           and not self.edge_caps)

    policy_doc = None
    policy_version = 0

    sess = None                    # live SecureSession (set by the ingest loop)
    device_policy_version = 0      # version the DEVICE reports as applied
    device_hb = None
    device_hb_ts = 0

    # service -> device control magics on the sectun channel:
    #   0xC7 binary bitrate/GOP/IDR (hot path, unchanged)
    #   0xC3 JSON config (this)
    CFG_MAGIC = 0xC3

    def push_device_config(self):
        """Send the edge policy + media-sink settings to the device.

        Versioned: the device stores the version and ignores a push it has
        already applied, so this is safe to call on every connect and every
        policy change."""
        sess = self.sess
        if sess is None or not self.policy_doc:
            return
        edge = (self.policy_doc.get("engines") or {}).get("edge")
        if edge is None:
            return
        # The wire (and the device's evaluator) is keyed by CLASS ID — the
        # device has no label table, the hub resolves names.  Translate here
        # so a device applying the config can't silently end up with an
        # empty policy it happily acks.
        by_id = {}
        for name, c in (edge.get("classes") or {}).items():
            try:
                cid = COCO_CLASSES.index(name)
            except ValueError:
                print(f">> policy: class {name!r} has no id in this label set"
                      " — not pushed", file=sys.stderr, flush=True)
                continue
            by_id[str(cid)] = c
        cfg = {"v": self.policy_version, "engine": "edge",
               "classes": by_id,
               "fps": int(edge.get("fps", 5) or 5),      # device-side rate cap
               "enabled": bool(edge.get("enabled", True))}
        try:
            blob = json.dumps(cfg, separators=(",", ":")).encode()
            sess.send(bytes([self.CFG_MAGIC]) + blob)
            print(f">> pushed edge config v{self.policy_version} "
                  f"({len(blob)} B)", flush=True)
        except Exception as e:
            print(f">> config push failed: {e}", file=sys.stderr, flush=True)

    def apply_policy(self):
        """Give the event engine the policy for whoever owns detection.

        Edge and service never evaluate at once (infer_mode decides), so the
        engine always gets exactly one engine's classes — whichever is
        actually producing the detections it sees."""
        eng = getattr(self, "event_engine", None)
        if eng is None or not self.policy_doc:
            return
        which = "service" if self.local_infer_active() else "edge"
        engines = (self.policy_doc.get("engines") or {})
        eng.set_policy(engines.get(which) or {}, self.policy_version)
        self.push_device_config()      # device owns its own copy of "edge"

    def apply_infer_mode(self):
        eng = getattr(self, "event_engine", None)
        if eng is not None:
            eng.local_infer = self.local_infer_active()
            self.apply_policy()          # ownership changed -> other engine's rules
            # edge boxes arrive in STREAM pixel space — teach the engine the
            # canvas so UI overlays scale correctly (camera adverts sw/sh)
            if self.edge_caps and self.edge_caps.get("sw"):
                eng.frame_wh = (self.edge_caps["sw"], self.edge_caps["sh"])

    def __init__(self):
        Gst.init(None)
        self.pipeline = Gst.Pipeline.new("nn-video")
        self.appsrc = Gst.ElementFactory.make("appsrc", "src")
        self.appsrc.set_property("is-live", True)
        self.appsrc.set_property("do-timestamp", True)
        self.appsrc.set_property("format", Gst.Format.TIME)
        self.appsrc.set_property("caps", Gst.Caps.from_string(APPSRC_CAPS))
        # Never let push-buffer block the socket-read loop: cap the queue and
        # drop oldest on overflow (bursty record delivery from the C6 reorder).
        self.appsrc.set_property("block", False)
        self.appsrc.set_property("max-bytes", 8 * 1024 * 1024)
        try:
            self.appsrc.set_property("leaky-type", 2)   # GST_APP_LEAKY_TYPE_DOWNSTREAM
        except Exception:
            pass
        self.tee = Gst.ElementFactory.make("tee", "t")
        self.tee.set_property("allow-not-linked", True)  # keep flowing w/ 0 branches
        for e in (self.appsrc, self.tee):
            self.pipeline.add(e)
        self.appsrc.link(self.tee)
        self.audio = None        # AudioSink, set in main() when audio is enabled
        self.avsync = None       # AVSync element, set per-ingest-session
        self.avsync_window_ms = 200

        self._branches = {}     # id -> dict(elements, tee_pad)
        self._next_id = 1
        self._lock = threading.Lock()
        self._det = None        # MotionDetector, set by add_detection_branch()
        self.loop = GLib.MainLoop()
        self.h264_subs = set()  # asyncio.Queue per /ws live-view client
        self.aio_loop = None    # aiohttp asyncio loop (set on first /ws connect)
        self._fed = 0           # H.264 AUs pushed to appsrc (source-of-truth count)
        self._qprobe = {}       # queue-name -> {in,out} buffer counters (/pipeline/stats)
        self._hls_q = None      # queue.Queue feeding the zero-copy HLS transcoder (opt-in)
        self._tc_writer = None  # nn-transcoded shm ring (opt-in, see main())
        self._tc_warned = False
        self._tc_port = None    # port the HLS ffmpeg reads; must stay stable
        # ── clean-snapshot state ──────────────────────────────────────────
        # The detection branch drops frames (leaky), so its latest frame can be
        # a corrupt P-frame.  Instead we keep a rolling buffer of the recent raw
        # H.264 and, on demand, extract the newest COMPLETE IDR access unit
        # (SPS+PPS+IDR) — reassembled across ingest records, since a big IDR is
        # fragmented into several — then HW-decode it (WAVE5, lenient) for a
        # clean, self-contained keyframe.  See grab_snapshot_jpeg().
        self._recent = collections.deque()   # recent H.264 payloads (rolling)
        self._recent_bytes = 0
        self._sps = None
        self._pps = None
        self.snapshot_interval = 30      # seconds between periodic saves (user setting)
        self.snapshot_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "snapshots")
        self.snapshot_last_ts = 0.0

    def _on_bus_message(self, bus, msg):
        """A pipeline ERROR is fatal for this slot: exit so the supervisor
        restarts a clean process (a wedged pipeline used to stay up, silent).
        Warnings are logged; EOS on a live appsrc pipeline is an error too."""
        t = msg.type
        if t == Gst.MessageType.ERROR:
            err, dbg = msg.parse_error()
            self._fatal(f"GStreamer error from {msg.src.get_name() if msg.src else '?'}: "
                        f"{err.message} ({dbg})", EXIT_GST_ERROR)
            return True
        if t == Gst.MessageType.EOS:
            self._fatal("pipeline EOS on a live source", EXIT_GST_ERROR)
            return True
        if t == Gst.MessageType.WARNING:
            w, dbg = msg.parse_warning()
            print(f">> gst warning from {msg.src.get_name() if msg.src else '?'}: {w.message}",
                  flush=True)
        return True

    def start(self):
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._tagged(self._on_bus_message))
        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            self._fatal("pipeline refused to go PLAYING", EXIT_STATE_TIMEOUT)
            return
        if getattr(self, "run_glib_loop", True):
            threading.Thread(target=self.loop.run, daemon=True, name="glib-main").start()
        # ASYNC is normal for a live appsrc pipeline (it preroll-waits for data);
        # bound the wait so a hardware element that never comes up is a
        # restart, not a silent hang.
        st, cur, _pend = self.pipeline.get_state(2 * Gst.SECOND)
        if st == Gst.StateChangeReturn.FAILURE:
            self._fatal("pipeline failed to reach PLAYING", EXIT_STATE_TIMEOUT)
            return
        self.started_at = time.time()
        print(f">> GStreamer pipeline {cur.value_nick.upper()} (appsrc ! tee)")

    def feed(self, data: bytes):
        self._fed += 1
        self._cache_keyframe(data)
        w = self._tc_writer                  # shared transcode service (shm)
        if w is not None:
            try:
                # Never blocks: a full ring means the transcoder is behind,
                # and HLS losing a frame must not stall the live uplink.
                # fragments in, whole access units out (see push_fragment)
                w.push_fragment(data, int(time.time() * 1000))
                # A silently-dropping ring looks like a slow transcoder, so
                # say it out loud the first time and then occasionally.
                d = w.dropped
                if d and (d == 1 or d % 500 == 0):
                    print(f">> nn-transcoded: ring full, {d} AUs dropped "
                          f"(transcoder behind or ring too small)",
                          file=sys.stderr, flush=True)
            except OSError:
                # The service restarted (its unit has Restart=on-failure).
                # Without this the camera keeps a dead socket forever and HLS
                # never returns — the same failure the inference client had.
                # Reconnect off-thread so ingest never blocks.
                if not self._tc_warned:
                    print(">> nn-transcoded connection lost — reconnecting",
                          file=sys.stderr, flush=True)
                    self._tc_warned = True
                self._tc_writer = None
                threading.Thread(target=self._tc_reconnect, name=f"tc-reconnect:{self.cam_id}",
                                 daemon=True).start()
        q = self._hls_q                      # feed the zero-copy HLS transcoder
        if q is not None:
            try:
                q.put_nowait(data)
            except Exception:                # full → drop oldest to stay live
                try: q.get_nowait(); q.put_nowait(data)
                except Exception: pass
        buf = Gst.Buffer.new_wrapped(data)
        self.appsrc.emit("push-buffer", buf)

    def pipeline_stats(self):
        """Buffer accounting for every queue in the live pipeline.  Pad probes
        are installed on first call (counts start then); a leaky queue drops
        silently, so dropped = in − out − currently-queued.  Query once to arm,
        wait a few seconds, query again to read accumulated drops."""
        rows = []
        try:
            it = self.pipeline.iterate_recurse()
        except Exception as e:
            return {"error": str(e)}
        while True:
            res = it.next()
            ok = res[0] if isinstance(res, tuple) else res
            el = res[1] if isinstance(res, tuple) else None
            if ok == Gst.IteratorResult.RESYNC:
                it.resync(); rows = []; continue
            if ok != Gst.IteratorResult.OK:
                break
            fac = el.get_factory()
            if not fac or fac.get_name() not in ("queue", "queue2"):
                continue
            name = el.get_name()
            c = self._qprobe.get(name)
            if c is None:
                c = {"in": 0, "out": 0}
                def _in(pad, info, c=c): c["in"] += 1; return Gst.PadProbeReturn.OK
                def _out(pad, info, c=c): c["out"] += 1; return Gst.PadProbeReturn.OK
                sp = el.get_static_pad("sink"); rp = el.get_static_pad("src")
                if sp: sp.add_probe(Gst.PadProbeType.BUFFER, _in)
                if rp: rp.add_probe(Gst.PadProbeType.BUFFER, _out)
                self._qprobe[name] = c
            def gp(p, d=0):
                try: return el.get_property(p)
                except Exception: return d
            lvl = gp("current-level-buffers")
            rows.append({
                "queue":     name,
                "in":        c["in"],
                "out":       c["out"],
                "queued":    lvl,
                "dropped":   max(0, c["in"] - c["out"] - lvl),
                "level_ms":  (gp("current-level-time") or 0) // 1_000_000,
                "leaky":     gp("leaky"),
            })
        out = {"fed": self._fed, "queues": rows}
        try:
            out["appsrc"] = {
                "level_bytes": self.appsrc.get_property("current-level-bytes"),
                "max_bytes":   self.appsrc.get_property("max-bytes"),
                "leaky_type":  self.appsrc.get_property("leaky-type"),
            }
        except Exception:
            pass
        return out

    def _cache_keyframe(self, data: bytes):
        """Append the payload to a rolling ~8 MB buffer of recent raw H.264 (a
        big IDR spans several ingest records, so we can't cache it per-record —
        we reassemble it from this buffer at grab time)."""
        try:
            self._recent.append(data)
            self._recent_bytes += len(data)
            while self._recent_bytes > 8_000_000 and len(self._recent) > 1:
                self._recent_bytes -= len(self._recent.popleft())
        except Exception:
            pass

    def _tc_reconnect(self):
        """Re-establish the transcode session after the service restarts.

        The port must come back the same, because the HLS ffmpeg is already
        reading it; if it does not, say so loudly rather than silently
        feeding a port nobody consumes."""
        import transcoded_client as _tc
        while self._tc_writer is None:
            time.sleep(10)
            try:
                w, port = _tc.try_open(getattr(self, "cam_id", "") or os.environ.get("NN_CAM_ID", "camera"),
                                       8_000_000, 15, self.enc_qp or 0, True)
            except Exception:                  # noqa: BLE001 - optional dep
                continue
            if not w:
                continue
            if self._tc_port and port != self._tc_port:
                print(f"!! nn-transcoded came back on port {port} but HLS "
                      f"reads {self._tc_port} — restart this camera",
                      file=sys.stderr, flush=True)
            self._tc_writer = w
            self._tc_warned = False
            print(">> nn-transcoded reconnected", file=sys.stderr, flush=True)
            return

    def _is_keyframe(self, data: bytes) -> bool:
        """IDR (NAL 5) or a parameter set present in this record.

        Only a hint for the transcoder's buffer flags — a wrong answer costs
        nothing because h264parse re-derives frame types downstream."""
        try:
            for _off, nt in self._scan_nals(data):
                if nt in (5, 7, 8):
                    return True
        except Exception:
            pass
        return False

    @staticmethod
    def _scan_nals(buf: bytes):
        """Yield (offset, nal_type) for each Annex-B start code in buf."""
        i = buf.find(b"\x00\x00\x01")
        while i != -1:
            if i + 3 < len(buf):
                yield (i - 1 if i > 0 and buf[i - 1] == 0 else i), (buf[i + 3] & 0x1f)
            i = buf.find(b"\x00\x00\x01", i + 3)

    def _latest_idr_au(self):
        """Reassemble and return the newest COMPLETE IDR access unit from the
        rolling buffer (SPS+PPS+IDR), or None.  'Complete' = bounded by the next
        frame's slice NAL, so all fragments of the IDR are included."""
        buf = b"".join(self._recent)
        if not buf:
            return None
        nals = list(self._scan_nals(buf))
        if not nals:
            return None
        # newest SPS/PPS in the buffer (for prepending if the IDR AU lacks them)
        for off, typ in nals:
            end = next((o for o, _ in nals if o > off), len(buf))
            if typ == 7:
                self._sps = buf[off:end]
            elif typ == 8:
                self._pps = buf[off:end]
        # find the last IDR (5) that is followed by another slice (1 or 5) so we
        # know its access unit is fully present
        idr_i = nxt = None
        for k in range(len(nals) - 1, -1, -1):
            if nals[k][1] == 5:
                for j in range(k + 1, len(nals)):
                    if nals[j][1] in (1, 5):
                        idr_i, nxt = k, nals[j][0]
                        break
                if idr_i is not None:
                    break
        if idr_i is None:
            return None
        start = nals[idr_i][0]
        j = idr_i - 1                       # back up over immediately-preceding SPS/PPS
        while j >= 0 and nals[j][1] in (7, 8):
            start = nals[j][0]
            j -= 1
        au = buf[start:nxt]
        if not any(t == 7 for _, t in self._scan_nals(au)):   # ensure SPS/PPS present
            au = (self._sps or b"") + (self._pps or b"") + au
        return au

    def grab_snapshot_jpeg(self, q: int = 90):
        """Decode the latest cached IDR through a throwaway WAVE5 (v4l2h264dec)
        pipeline → RGB → JPEG.  WHY WAVE5 and not software: the ESP32-P4 emits a
        non-conformant bitstream that software decoders (PyAV/ffmpeg) green out
        mid-frame, but the WAVE5 HW decoder is lenient and decodes it cleanly
        (it's the same decoder the re-encode path relies on).  videoconvert →
        RGB applies the stream's BT.709 colorimetry, and an IDR is
        self-contained so the result carries none of the detection branch's
        dropped-reference corruption.  Returns None if no keyframe is cached yet
        or the decode failed (caller keeps the previous saved snapshot)."""
        # Primary: grab a frame from the newest COMPLETE HLS segment via ffmpeg.
        # The .ts segments are self-contained GOPs (they start with an IDR), which
        # ffmpeg decodes cleanly at 1080p — unlike the continuous lossy record
        # stream (greens out) or the WAVE5 grab below (cannot get the busy HW
        # decoder, which is what froze the webapp thumbnail on the beagle).
        try:
            import glob as _glob, subprocess as _sp, tempfile as _tf
            _hd = self.hls_dir or os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "hls_live")
            def _mt(p):
                try:
                    return os.path.getmtime(p)
                except OSError:
                    return 0                  # rotated away between glob and stat (hlssink2 is quick)
            _segs = [p for p in sorted(_glob.glob(os.path.join(_hd, "seg*.ts")), key=_mt) if _mt(p)]
            if len(_segs) >= 2:
                _tmp = _tf.NamedTemporaryFile(suffix=".jpg", delete=False); _tmp.close()
                _r = _sp.run(["ffmpeg", "-v", "error", "-y", "-i", _segs[-2],
                              "-frames:v", "1", "-q:v", "3", _tmp.name],
                             capture_output=True, timeout=8)
                _data = None
                if _r.returncode == 0 and os.path.getsize(_tmp.name) > 0:
                    with open(_tmp.name, "rb") as _fh:
                        _data = _fh.read()
                try:
                    os.unlink(_tmp.name)
                except Exception:
                    pass
                if _data:
                    return _data
        except Exception as _e:
            print(">> hls snapshot fallback: %r" % _e, file=sys.stderr, flush=True)
        au = self._latest_idr_au()
        if not au:
            return None
        pipe = None
        try:
            import io as _io
            from PIL import Image
            pipe = Gst.parse_launch(
                "appsrc name=s is-live=false format=time ! "
                "h264parse ! v4l2h264dec ! videoconvert ! video/x-raw,format=RGB ! "
                "appsink name=out emit-signals=false max-buffers=2 drop=false sync=false")
            src = pipe.get_by_name("s")
            src.set_property("caps", Gst.Caps.from_string(APPSRC_CAPS))
            out = pipe.get_by_name("out")
            pipe.set_state(Gst.State.PLAYING)
            src.emit("push-buffer", Gst.Buffer.new_wrapped(au))
            src.emit("end-of-stream")
            rgb = None
            smp = out.emit("try-pull-sample", 4 * Gst.SECOND)
            if smp:
                buf = smp.get_buffer()
                st = smp.get_caps().get_structure(0)
                w, h = st.get_value("width"), st.get_value("height")
                ok, mi = buf.map(Gst.MapFlags.READ)
                try:
                    if ok and mi.size >= w * h * 3:
                        rgb = np.frombuffer(mi.data, np.uint8,
                                            count=w * h * 3).reshape(h, w, 3).copy()
                finally:
                    buf.unmap(mi)
            if rgb is None:
                return None
            b = _io.BytesIO()
            Image.fromarray(rgb).save(b, format="JPEG", quality=q)
            return b.getvalue()
        except Exception as e:
            print(f">> snapshot decode failed: {e}", file=sys.stderr, flush=True)
            return None
        finally:
            if pipe is not None:
                pipe.set_state(Gst.State.NULL)

    def _snapshot_saver(self):
        """Every snapshot_interval seconds, save the latest clean snapshot to
        <snapshot_dir>/latest.jpg (atomic overwrite).  The interval is read each
        loop so it can be changed at runtime via POST /api/snapshot/config."""
        import time as _t
        try:
            os.makedirs(self.snapshot_dir, exist_ok=True)
        except Exception as e:
            print(f">> snapshot dir error: {e}", file=sys.stderr, flush=True)
        latest = os.path.join(self.snapshot_dir, "latest.jpg")
        _t.sleep(5)                       # let the first IDR arrive
        while not self.closed:
            jpg = self.grab_snapshot_jpeg()
            if jpg:
                try:
                    tmp = latest + ".tmp"
                    with open(tmp, "wb") as f:
                        f.write(jpg)
                    os.replace(tmp, latest)        # atomic
                    self.snapshot_last_ts = _t.time()
                except Exception as e:
                    print(f">> snapshot save failed: {e}", file=sys.stderr, flush=True)
            _t.sleep(max(2, int(self.snapshot_interval or 30)))

    def ws_forward(self, payload: bytes, ts: int = 0):
        # Passthrough: hand each raw H.264 record to every live-view WebSocket
        # client (browser decodes it — no server transcode/decode). Called from
        # the ingest thread, so hop onto the asyncio loop thread-safely.
        # NN_DUMP_H264=<path>: append every raw AU to a file — the sanctioned
        # way to capture the exact camera bitstream for offline codec analysis.
        _dump = os.environ.get("NN_DUMP_H264")
        if _dump:
            try:
                with open(_dump, "ab") as _f:
                    _f.write(payload)
            except Exception:
                pass
        loop = self.aio_loop
        if loop is None or not self.h264_subs:
            return
        # Device capture timestamp (absolute epoch ms, from the record header)
        # as a throttled TEXT frame so players can show real capture time.
        if ts:
            now = time.time()
            if now - getattr(self, "_ts_sent", 0.0) > 0.25:
                self._ts_sent = now
                tmsg = '{"ts":%d}' % ts
                for q in list(self.h264_subs):
                    try:
                        loop.call_soon_threadsafe(q.put_nowait, tmsg)
                    except Exception:
                        pass
        for q in list(self.h264_subs):
            try:
                loop.call_soon_threadsafe(q.put_nowait, payload)
            except Exception:
                pass   # queue full = slow client; it recovers at the next keyframe

    # ── dynamic RTP/UDP branch (the appended GST element) ────────────────
    def add_rtp_branch(self, host: str, port: int, pt: int = 96,
                       reencode: bool = False, bitrate: int = 2500000,
                       gop: int = 30) -> int:
        """Append an RTP/UDP branch off the tee.

        reencode=True inserts a WAVE5 HARDWARE transcode
        (v4l2h264dec ! v4l2h264enc) before payloading.  This is REQUIRED for
        any consumer using ffmpeg/libav (aiortc, browsers): the ESP32-P4's
        hardware H.264 encoder emits a non-conformant bitstream that ffmpeg's
        software decoder fails on mid-slice (top rows decode, rest go green),
        while lenient hardware decoders (WAVE5, gst v4l2h264dec) handle it.
        Re-encoding through the WAVE5 encoder normalises it to standard H.264.
        Both decode and encode run in J722S silicon (~zero CPU).  gop = keyframe
        interval (frames); bitrate in bps.
        """
        result = {}
        done = threading.Event()

        def _add():
            q = Gst.ElementFactory.make("queue", None)
            q.set_property("leaky", 2)          # drop old on overflow (live)
            if reencode:
                # Deep TIME-bounded buffer.  A re-encode branch decodes then
                # re-encodes, so dropping an H.264 AU here loses a REFERENCE
                # frame → the WAVE5 re-decode smears/blocks and that corruption
                # is baked into the output (the HLS "bottom corrupted" bug).
                # Buffer transient enc/sink stalls (≤3 s) instead of dropping;
                # leaky stays as a last-resort backstop so a sustained stall can
                # never block the shared tee (and thus /live).
                q.set_property("max-size-buffers", 0)
                q.set_property("max-size-bytes", 0)
                q.set_property("max-size-time", 3 * Gst.SECOND)
            elements = [q]

            if reencode:
                # h264parse only in the reencode path (feeds the HW decoder).
                parse = Gst.ElementFactory.make("h264parse", None)
                parse.set_property("config-interval", -1)
                elements += [parse]
                dec = Gst.ElementFactory.make("v4l2h264dec", None)
                # videoconvert bridges the two WAVE5 m2m devices: it COPIES the
                # decoder's output into a fresh, correctly-strided NV12 buffer.
                # It's REQUIRED for correctness — a direct system-memory I420
                # dec!enc link is ~10x faster (61 vs 6 fps) but produces full-frame
                # GARBAGE in the live pipeline (buffer-lifecycle/stride race vs the
                # concurrent detection decoder), and a DMABuf dec!enc link fails
                # negotiation ("got 1 dmabuf but needed 3" — dec exports 1 plane
                # FD, enc import wants 3).  The videoconvert DMABuf read is the CPU
                # bottleneck; reduce it by downscaling/lowering fps, not removing.
                vconv = Gst.ElementFactory.make("videoconvert", None)
                rawcaps = Gst.ElementFactory.make("capsfilter", None)
                rawcaps.set_property("caps", Gst.Caps.from_string("video/x-raw,format=NV12"))
                enc = Gst.ElementFactory.make("v4l2h264enc", None)
                # bitrate + periodic keyframes so a mid-join viewer recovers fast
                ec = Gst.Structure.new_empty("controls")
                ec.set_value("video_bitrate", int(bitrate))
                ec.set_value("h264_i_frame_period", int(gop))
                ec.set_value("video_gop_size", int(gop))
                # CIX P1 ignores video_bitrate; fixed QP is the only knob that
                # works there (see VideoService.enc_qp).
                if self.enc_qp:
                    for _k in ("h264_i_frame_qp_value", "h264_p_frame_qp_value",
                               "h264_b_frame_qp_value"):
                        ec.set_value(_k, int(self.enc_qp))
                enc.set_property("extra-controls", ec)
                # constrained-baseline: universally decodable, no B-frames
                enccaps = Gst.ElementFactory.make("capsfilter", None)
                enccaps.set_property("caps", Gst.Caps.from_string(
                    "video/x-h264,profile=constrained-baseline"))
                parse2 = Gst.ElementFactory.make("h264parse", None)
                parse2.set_property("config-interval", -1)
                elements += [dec, vconv, rawcaps, enc, enccaps, parse2]

            pay = Gst.ElementFactory.make("rtph264pay", None)
            pay.set_property("config-interval", -1)  # SPS/PPS with every IDR
            pay.set_property("pt", pt)
            sink = Gst.ElementFactory.make("udpsink", None)
            sink.set_property("host", host)
            sink.set_property("port", port)
            sink.set_property("sync", False)
            sink.set_property("async", False)
            elements += [pay, sink]

            for e in elements:
                self.pipeline.add(e)
                e.sync_state_with_parent()
            for a, b in zip(elements, elements[1:]):
                a.link(b)
            tee_pad = self.tee.get_request_pad("src_%u")
            tee_pad.link(q.get_static_pad("sink"))
            with self._lock:
                bid = self._next_id
                self._next_id += 1
                self._branches[bid] = {"elements": tuple(elements), "tee_pad": tee_pad,
                                       "host": host, "port": port, "pt": pt}
            result["id"] = bid
            print(f">> branch {bid}: RTP/H264{' [WAVE5 re-encode]' if reencode else ''}"
                  f" → udp {host}:{port} pt={pt}")
            done.set()
            return False

        GLib.idle_add(self._tagged(_add))
        done.wait(timeout=5)
        return result.get("id", -1)

    # ── dynamic TCP branch: parsed H.264 byte-stream over loopback TCP ───
    def add_tcp_branch(self, port: int, reencode: bool = False, mux_ts: bool = False,
                       bitrate: int = 6_000_000, gop: int = 30) -> int:
        """tee ! queue(leaky) ! h264parse ! tcpserversink(127.0.0.1:port).

        Lossless local hand-off for consumers that can't drain RTP/UDP
        bursts in time (the hub's aiortc/ffmpeg ingest dropped IDR tails
        under event-loop jitter).  TCP + byte-stream = no jitter buffer,
        no packet loss, by construction.  sync-method=next-keyframe makes
        a late-joining consumer start on a clean IDR."""
        result = {}
        done = threading.Event()

        def _add():
            # No h264parse: the appsrc already carries byte-stream/AU H.264
            # (APPSRC_CAPS) and h264parse stalls trying to re-align the C6's
            # chunked buffers.  The consumer (hub ffmpeg tcp://,format=h264)
            # parses the byte-stream itself.
            q = Gst.ElementFactory.make("queue", None)
            q.set_property("leaky", 2)
            if reencode:
                # Deep TIME-bounded buffer so transient enc/sink stalls buffer
                # instead of DROPPING an H.264 AU — a dropped reference frame on
                # a re-encode branch makes the WAVE5 re-decode smear/block, and
                # that is baked into the output (HLS "bottom corrupted").  Leaky
                # stays as a backstop so it can't stall the shared tee → /live.
                q.set_property("max-size-buffers", 0)
                q.set_property("max-size-bytes", 0)
                q.set_property("max-size-time", 3 * Gst.SECOND)
            elements = [q]
            if reencode:
                # WAVE5 HW transcode → clean constrained-baseline H.264 (the raw
                # P4 stream is non-conformant → ffmpeg's copy/parse can't segment
                # it; the HW decoder can).  Same chain as add_rtp_branch.
                parse = Gst.ElementFactory.make("h264parse", None)
                parse.set_property("config-interval", -1)
                dec = Gst.ElementFactory.make("v4l2h264dec", None)
                # NN_REENC_VCONV=1 restores the videoconvert hop.  Default is
                # DIRECT dec -> enc linking: on the CIX P1 both ends are V4L2
                # and can negotiate dmabuf, while videoconvert CPU-maps every
                # frame from an UNCACHED DMABuf — measured ~1/10 realtime at
                # 1080p, i.e. one 1 s HLS segment every ~10 s: the actual cause
                # of "HLS buffers forever" on both cameras.
                vconv = (Gst.ElementFactory.make("videoconvert", None)
                         if os.environ.get("NN_REENC_VCONV", "0") == "1" else None)
                rawcaps = Gst.ElementFactory.make("capsfilter", None)
                rawcaps.set_property("caps", Gst.Caps.from_string("video/x-raw,format=NV12"))
                enc = Gst.ElementFactory.make("v4l2h264enc", None)
                ec = Gst.Structure.new_empty("controls")
                ec.set_value("video_bitrate", int(bitrate))
                ec.set_value("h264_i_frame_period", int(gop))
                ec.set_value("video_gop_size", int(gop))
                # CIX P1 ignores video_bitrate; fixed QP is the only knob that
                # works there (see VideoService.enc_qp).
                if self.enc_qp:
                    for _k in ("h264_i_frame_qp_value", "h264_p_frame_qp_value",
                               "h264_b_frame_qp_value"):
                        ec.set_value(_k, int(self.enc_qp))
                enc.set_property("extra-controls", ec)
                enccaps = Gst.ElementFactory.make("capsfilter", None)
                enccaps.set_property("caps", Gst.Caps.from_string(
                    "video/x-h264,profile=constrained-baseline"))
                parse2 = Gst.ElementFactory.make("h264parse", None)
                parse2.set_property("config-interval", -1)
                elements += ([parse, dec, vconv, rawcaps, enc, enccaps, parse2]
                             if vconv else
                             [parse, dec, rawcaps, enc, enccaps, parse2])
            if mux_ts and not reencode:
                # mpegtsmux needs PARSED, AU-aligned H.264 (the re-encode path
                # gets that from its own trailing h264parse).  Without this the
                # branch links but never pushes a byte — HLS stays empty.
                # Safe here because a non-re-encoded source is a conformant
                # encoder (BeagleY WAVE5), not the C6's chunked buffers.
                tsparse = Gst.ElementFactory.make("h264parse", None)
                tsparse.set_property("config-interval", -1)
                elements += [tsparse]
            mux = None
            if mux_ts:
                # Wrap in MPEG-TS *inside* the pipeline: TS carries the real
                # PTS across the TCP boundary, so a downstream segmenter paces
                # correctly at ANY source fps (raw h264 loses all timing).
                mux = Gst.ElementFactory.make("mpegtsmux", None)
                elements += [mux]
            sink = Gst.ElementFactory.make("tcpserversink", None)
            sink.set_property("host", "127.0.0.1")
            sink.set_property("port", port)
            sink.set_property("sync", False)
            elements += [sink]
            for e in elements:
                self.pipeline.add(e)
                e.sync_state_with_parent()
            if mux is not None:
                # h264parse -> mpegtsmux request pad, mux -> sink
                for a, b in zip(elements[:-2], elements[1:-2]):
                    a.link(b)
                elements[-3].get_static_pad("src").link(mux.get_request_pad("sink_%d"))
                mux.link(sink)
            else:
                for a, b in zip(elements, elements[1:]):
                    a.link(b)
            tee_pad = self.tee.get_request_pad("src_%u")
            tee_pad.link(q.get_static_pad("sink"))
            with self._lock:
                bid = self._next_id
                self._next_id += 1
                self._branches[bid] = {"elements": tuple(elements), "tee_pad": tee_pad,
                                       "host": "127.0.0.1", "port": port, "pt": 0}
            result["id"] = bid
            print(f">> branch {bid}: H264{' [WAVE5 re-encode]' if reencode else ''}"
                  f" byte-stream → tcp 127.0.0.1:{port}")
            done.set()
            return False

        GLib.idle_add(self._tagged(_add))
        done.wait(timeout=5)
        return result.get("id", -1)

    # ── dynamic HLS branch: tee ! queue ! [WAVE5?] ! h264parse ! hlssink2 ────
    def add_hls_branch(self, out_dir, reencode=False, bitrate=6_000_000, gop=30):
        """Write MPEG-TS HLS segments + live.m3u8 to out_dir off the tee.

        HLS is delivered over HTTP/TCP → lossless, so it has NONE of the
        blocky/mosaic artifacts WebRTC (RTP/UDP) shows on loss, and it plays
        natively on iPhone Safari.  reencode=False keeps the RAW P4 H.264 (best
        quality — browsers/HW decoders are lenient) but relies on h264parse
        coping with the non-conformant AUs; reencode=True normalises it through
        the WAVE5 HW transcode (universally decodable, slight quality cost)."""
        result = {}
        done = threading.Event()

        def _add():
            q = Gst.ElementFactory.make("queue", None)
            q.set_property("leaky", 2)
            if reencode:
                # Deep TIME-bounded buffer so transient enc/sink stalls buffer
                # instead of DROPPING an H.264 AU — a dropped reference frame on
                # a re-encode branch makes the WAVE5 re-decode smear/block, and
                # that is baked into the output (HLS "bottom corrupted").  Leaky
                # stays as a backstop so it can't stall the shared tee → /live.
                q.set_property("max-size-buffers", 0)
                q.set_property("max-size-bytes", 0)
                q.set_property("max-size-time", 3 * Gst.SECOND)
            elements = [q]
            if reencode:
                parse = Gst.ElementFactory.make("h264parse", None)
                parse.set_property("config-interval", -1)
                dec = Gst.ElementFactory.make("v4l2h264dec", None)
                # both codec ends are V4L2 on the CIX — direct link by default
                # (NN_REENC_VCONV=1 restores the videoconvert hop)
                vconv = (Gst.ElementFactory.make("videoconvert", None)
                         if os.environ.get("NN_REENC_VCONV", "0") == "1" else None)
                rawcaps = Gst.ElementFactory.make("capsfilter", None)
                rawcaps.set_property("caps", Gst.Caps.from_string("video/x-raw,format=NV12"))
                enc = Gst.ElementFactory.make("v4l2h264enc", None)
                ec = Gst.Structure.new_empty("controls")
                ec.set_value("video_bitrate", int(bitrate))
                ec.set_value("h264_i_frame_period", int(gop))
                ec.set_value("video_gop_size", int(gop))
                # CIX P1 ignores video_bitrate; fixed QP is the only knob that
                # works there (see VideoService.enc_qp).
                if self.enc_qp:
                    for _k in ("h264_i_frame_qp_value", "h264_p_frame_qp_value",
                               "h264_b_frame_qp_value"):
                        ec.set_value(_k, int(self.enc_qp))
                enc.set_property("extra-controls", ec)
                enccaps = Gst.ElementFactory.make("capsfilter", None)
                enccaps.set_property("caps", Gst.Caps.from_string(
                    "video/x-h264,profile=constrained-baseline"))
                elements += ([parse, dec, vconv, rawcaps, enc, enccaps] if vconv
                             else [parse, dec, rawcaps, enc, enccaps])
            parse2 = Gst.ElementFactory.make("h264parse", None)
            parse2.set_property("config-interval", -1)
            hls = Gst.ElementFactory.make("hlssink2", None)
            if hls is None:
                print(">> HLS: hlssink2 element missing (gst-plugins-bad)"); done.set(); return False
            hls.set_property("location", os.path.join(out_dir, "seg%05d.ts"))
            hls.set_property("playlist-location", os.path.join(out_dir, "live.m3u8"))
            hls.set_property("target-duration", 1)
            hls.set_property("playlist-length", 6)
            hls.set_property("max-files", 12)
            elements += [parse2, hls]

            chain = elements[:-1]          # q .. parse2 (hlssink2 linked via request pad)
            for e in elements:
                self.pipeline.add(e)
                e.sync_state_with_parent()
            for a, b in zip(chain, chain[1:]):
                a.link(b)
            vpad = hls.get_request_pad("video")
            parse2.get_static_pad("src").link(vpad)
            tee_pad = self.tee.get_request_pad("src_%u")
            tee_pad.link(q.get_static_pad("sink"))
            with self._lock:
                bid = self._next_id
                self._next_id += 1
                self._branches[bid] = {"elements": tuple(elements), "tee_pad": tee_pad,
                                       "host": out_dir, "port": 0, "pt": 0}
            result["id"] = bid
            print(f">> branch {bid}: HLS{' [WAVE5 re-encode]' if reencode else ' [raw]'}"
                  f" → {out_dir}/live.m3u8")
            done.set()
            return False

        GLib.idle_add(self._tagged(_add))
        done.wait(timeout=5)
        return result.get("id", -1)

    def remove_branch(self, bid: int) -> bool:
        with self._lock:
            b = self._branches.pop(bid, None)
        if not b:
            return False
        done = threading.Event()

        def _remove():
            tee_pad = b["tee_pad"]
            q = b["elements"][0]
            tee_pad.unlink(q.get_static_pad("sink"))
            self.tee.release_request_pad(tee_pad)
            for e in b["elements"]:
                e.set_state(Gst.State.NULL)
                self.pipeline.remove(e)
            print(f">> branch {bid} removed")
            done.set()
            return False

        GLib.idle_add(_remove)
        done.wait(timeout=5)
        return True

    # ── split branch: decode H.264 → YUV → motion detection ──────────────
    def add_detection_branch(self, detector: "MotionDetector",
                             decoder: str = "auto", fmt: str = "I420") -> bool:
        """Split off the same tee as the RTP branch and run the detector.

        decoder:
          "v4l2" — in-pipeline HARDWARE decode (e.g. WAVE5 on AM67A/J722S):
                   tee ! queue ! h264parse ! v4l2h264dec ! videoconvert !
                   GRAY8 ! appsink → detector.  Near-zero CPU.
          "pyav" — legacy software path: tee ! queue ! appsink(H.264) →
                   PyAV decode worker → detector.
          "auto" — v4l2 when the v4l2h264dec element exists, else pyav.
        """
        if decoder in ("auto", "v4l2") and Gst.ElementFactory.find("v4l2h264dec"):
            return self._add_detection_branch_v4l2(detector, fmt)
        if decoder == "v4l2":
            print(">> detection branch disabled (v4l2h264dec not available)")
            return False
        try:
            import av  # noqa: F401
        except Exception as e:
            print(f">> detection branch disabled (no PyAV: {e})")
            return False
        self._det = detector
        self._det_q = collections.deque(maxlen=240)   # H.264 chunks; drop oldest
        self._det_cv = threading.Condition()

        def _add():
            q = Gst.ElementFactory.make("queue", None)
            q.set_property("leaky", 2)
            sink = Gst.ElementFactory.make("appsink", None)
            sink.set_property("emit-signals", True)
            sink.set_property("sync", False)
            sink.set_property("max-buffers", 8)
            sink.set_property("drop", True)
            sink.connect("new-sample", self._on_h264_sample)
            for e in (q, sink):
                self.pipeline.add(e)
                e.sync_state_with_parent()
            q.link(sink)
            tee_pad = self.tee.get_request_pad("src_%u")
            tee_pad.link(q.get_static_pad("sink"))
            print(">> detection branch: tee → appsink → PyAV decode (YUV) → motion detect")
            return False

        GLib.idle_add(self._tagged(_add))
        threading.Thread(target=self._det_worker, daemon=True, name=f"detector:{self.cam_id}").start()
        return True

    def _add_detection_branch_v4l2(self, detector: "MotionDetector", fmt: str = "I420") -> bool:
        """Hardware-decode detection branch.  The decoder outputs the format
        the detector reads (I420, or GRAY8 luma) NATIVELY — no videoconvert:
        that element rewrote every frame at 30 fps whether anyone wanted it
        (measured 2026-09-21: 540 MB/s of memory traffic for three cameras).
        GRAY8 is requested as DMABUF so Python reads the decoder's pages
        through an mmap; I420 comes through gst-python's map (one copy)."""
        self._det = detector
        self._det_fmt = fmt
        self._dma_maps = None

        def _add():
            q = Gst.ElementFactory.make("queue", None)
            q.set_property("leaky", 2)
            parse = Gst.ElementFactory.make("h264parse", None)
            dec = Gst.ElementFactory.make("v4l2h264dec", None)
            if fmt == "GRAY8":
                try:
                    dec.set_property("capture-io-mode", 4)        # dmabuf
                except Exception:
                    pass
            capsf = Gst.ElementFactory.make("capsfilter", None)
            capsf.set_property("caps", Gst.Caps.from_string(f"video/x-raw,format={fmt}"))
            sink = Gst.ElementFactory.make("appsink", None)
            sink.set_property("emit-signals", True)
            sink.set_property("sync", False)
            sink.set_property("max-buffers", 4)
            sink.set_property("drop", True)
            sink.connect("new-sample", self._on_raw_sample)
            for e in (q, parse, dec, capsf, sink):
                self.pipeline.add(e)
                e.sync_state_with_parent()
            q.link(parse); parse.link(dec); dec.link(capsf); capsf.link(sink)
            tee_pad = self.tee.get_request_pad("src_%u")
            tee_pad.link(q.get_static_pad("sink"))
            print(f">> detection branch: tee → h264parse → v4l2h264dec (HW, {fmt}"
                  f"{', dmabuf' if fmt == 'GRAY8' else ''}) → appsink → motion detect")
            return False

        GLib.idle_add(self._tagged(_add))
        return True

    def _on_raw_sample(self, sink):
        sample = sink.emit("pull-sample")
        if not sample:
            return Gst.FlowReturn.OK
        gate = self.frame_wanted
        if gate is not None and not gate():
            # nobody wants this frame (detection paces itself to one per
            # interval): drop it here, before anything is mapped or copied
            return Gst.FlowReturn.OK
        from nnvideo.frame import DmaMaps, Frame
        caps = sample.get_caps().get_structure(0)
        w, h = caps.get_value("width"), caps.get_value("height")
        fmt = caps.get_value("format") or getattr(self, "_det_fmt", "I420")
        buf = sample.get_buffer()
        ts_ms = int(time.time() * 1000)
        frame = None
        if fmt == "GRAY8":
            try:
                import gi
                gi.require_version("GstAllocators", "1.0")
                from gi.repository import GstAllocators
                mem = buf.peek_memory(0)
                if GstAllocators.is_dmabuf_memory(mem):
                    fd = GstAllocators.dmabuf_memory_get_fd(mem)
                    if self._dma_maps is None:
                        self._dma_maps = DmaMaps()
                    mm = self._dma_maps.get(fd, mem.size)
                    stride = mem.size // h if mem.size >= w * h else w
                    frame = Frame("GRAY8", w, h, mm, sample=sample, fd=fd, stride=stride, ts_ms=ts_ms)
            except Exception as e:                                   # noqa: BLE001
                print(f">> dmabuf frame failed ({e}); mapping instead", flush=True)
        if frame is None:
            ok, mi = buf.map(Gst.MapFlags.READ)
            if not ok:
                return Gst.FlowReturn.OK
            try:
                data = mi.data                    # gst-python: already a bytes copy
            finally:
                buf.unmap(mi)
            need = int(w * h * (1 if fmt == "GRAY8" else 1.5))
            if len(data) < need:
                return Gst.FlowReturn.OK
            stride = w if len(data) == need else (len(data) * 2 // (3 * h) if fmt != "GRAY8" else len(data) // h)
            frame = Frame("GRAY8" if fmt == "GRAY8" else "I420", w, h, data, stride=stride, ts_ms=ts_ms)
        self._feed_frame(frame)
        return Gst.FlowReturn.OK

    def _on_h264_sample(self, sink):
        sample = sink.emit("pull-sample")
        if sample:
            buf = sample.get_buffer()
            ok, mi = buf.map(Gst.MapFlags.READ)
            if ok:
                data = bytes(mi.data)
                buf.unmap(mi)
                with self._det_cv:
                    self._det_q.append(data)
                    self._det_cv.notify()
        return Gst.FlowReturn.OK

    def _det_worker(self):
        import av
        codec = av.CodecContext.create("h264", "r")   # the decoder element
        while True:
            with self._det_cv:
                while not self._det_q:
                    self._det_cv.wait()
                data = self._det_q.popleft()
            try:
                for pkt in codec.parse(data):
                    for frame in codec.decode(pkt):
                        yuv = frame.to_ndarray()        # I420 (h*3/2, w)
                        from nnvideo.frame import Frame as _F
                        self._feed_frame(_F.from_i420_array(yuv, frame.width, frame.height))
            except Exception:
                pass    # transient (e.g. decoding before the first keyframe)

    def motion_status(self):
        return self._det.latest() if self._det else {"enabled": False}

    def audio_status(self):
        return self.audio.status() if self.audio else {"enabled": False}

    def status(self):
        with self._lock:
            return {
                "state": "playing",
                "detection": self._det is not None,
                "audio": self.audio is not None,
                "branches": [{"id": k, "host": v["host"], "port": v["port"], "pt": v["pt"]}
                             for k, v in self._branches.items()],
            }


# ── server-driven adaptive encoder control ──────────────────────────────────
class AdaptController:
    """Steers the device encoder over the sectun back-channel.  Detects device
    frame drops from gaps in the video-record seq field (a poisoned/dropped frame
    never arrives, so its seq is missing) and runs an AIMD loop every second:
      congested → bitrate x0.8 + GOP shorter (N--)
      clean     → bitrate +step + GOP longer  (N++)
    GOP = fps * 2^N, N in [0, N_MAX].  Control wire: [0xC7][cmd][u32-be value],
    cmd 1=bitrate(bps) 2=gop(frames) 3=fps_divisor. """
    MAGIC = 0xC7
    CMD_BITRATE, CMD_GOP, CMD_FPS_DIV, CMD_FORCE_IDR = 1, 2, 3, 4
    # 5..11 are camera tuning (WB/focus/exposure/...) on the ESP cameras —
    # REBOOT deliberately sits well clear of that range.
    CMD_REBOOT = 20
    CMD_CLEAR_USER_DATA = 21
    # The esp-hosted SDIO link measures ~35 Mbit/s end-to-end (blast test), so
    # 1.5 Mbit/s left the picture soft.  Cap at 6 Mbit/s for crisp 720p with a
    # faster ramp; the loop still backs off x0.8 on any drop, so it self-limits
    # to whatever the Wi-Fi air actually sustains.
    # N_MAX=0 pins the DEVICE GOP at fps (30 = 1 s IDRs) instead of letting it
    # grow to fps<<2 = 120 (4 s).  Why: the P4 encoder's non-conformance (a CAVLC
    # desync ~MB col74/row25 = bottom-right) happens in a P-frame and PROPAGATES
    # until the next IDR.  The browser (/live) conceals it, but the WAVE5 HLS/RTP
    # RE-ENCODE bakes whatever WAVE5 decoded into the output — so a long GOP turns
    # one bad P-frame into up to 4 s of baked-in bottom-right corruption.  Short
    # GOP clears it every 1 s (the unit test re-encoded a ~gop30 sample CLEAN, a
    # gop120 live stream CORRUPT).  Costs ~+0.3 Mbit/s of IDRs — trivial on the
    # ~35 Mbit/s link.
    BR_MIN, BR_MAX, BR_UP, N_MAX = 500_000, 6_000_000, 300_000, 0

    # Operator strategy (webapp: camera Settings).  Fetched from the hub
    # every POLICY_REFRESH_S so a change takes effect without a restart.
    #   quality : pin fps at the chosen level, let bitrate ride to whatever
    #             the link sustains — sharp picture, fewer frames.
    #   fps     : hold full fps and shed bitrate first; only once bitrate is
    #             at the chosen floor do we start dropping frames.
    POLICY_REFRESH_S = 10.0
    FPS_DIV_MAX = 6            # 30 fps / 6 = 5 fps, the slowest we will go
    # Queueing-delay thresholds.  HI = "there is a standing queue, back
    # off"; LO = "still draining, keep asking for less than the link
    # carries".  CAP_FRAC leaves headroom so the queue never re-forms.
    QUEUE_HI_MS, QUEUE_LO_MS = 1200.0, 400.0
    CAP_FRAC, DRAIN_FRAC, PROBE_FRAC = 0.90, 0.65, 2.0

    def __init__(self, sess, fps=30, start_bitrate=3_000_000):
        self.sess = sess; self.fps = fps
        self.br = start_bitrate; self.n = 0
        self.mode = "fps"          # default: today's behaviour
        self.level = 5
        self.br_floor = self.BR_MIN
        self.target_fps = fps
        self.fps_div = 1
        self._policy_at = 0.0
        self.net_kbps = 0
        self._last_seq = None
        self._gaps = self._frames = self._bytes = 0
        # Queueing-delay congestion signal.  Frame-loss (seq gaps) is the
        # WRONG signal on this uplink: the link is reliable, so an
        # over-committed encoder never loses a frame — it just builds a
        # queue on the CAMERA.  drops stayed 0 while HLS ran 35 s behind
        # live (measured cam1 2026-08-18).  One-way delay exposes exactly
        # that standing queue.  Device ts_ms is absolute epoch ms, so any
        # host/device clock offset is a constant and cancels in owd-base.
        self._owd_min = None            # min one-way delay this window
        self._owd_buckets = []          # [(bucket_start, min_owd)] ~5 min
        self._gp_kbps = 0.0             # goodput EWMA (what the link CARRIES)
        self._nk_buckets = []           # decaying high-water for net_kbps
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._stop = threading.Event()
        self._th = threading.Thread(target=self._loop, daemon=True)

    def start(self):
        self._send(self.CMD_GOP, self.fps << self.n)
        self._send(self.CMD_BITRATE, self.br)
        self._th.name = f"adapt:{getattr(self, 'cam_id', '') or ''}"     # print() tags lines by thread name
        self._th.start()

    def force_idr(self):
        """Ask the device for an immediate IDR (new viewer joined — gives the
        stream a decode point now instead of up to one GOP later)."""
        self._send(self.CMD_FORCE_IDR, 0)

    def stop(self):
        self._stop.set()

    def on_video(self, seq, flags, nbytes, dev_ts_ms=None):
        with self._lock:
            self._bytes += nbytes
            if dev_ts_ms:
                # Not a true latency (clocks differ) — only its VARIATION
                # above the running baseline is used, which is the queue.
                owd = time.time() * 1000.0 - float(dev_ts_ms)
                if self._owd_min is None or owd < self._owd_min:
                    self._owd_min = owd
            if flags & 0x02:                       # VID_START = a new frame
                if self._last_seq is not None:
                    gap = (seq - self._last_seq - 1) & 0xFFFF
                    if 0 < gap < 1000:             # ignore u16 wrap / reorder
                        self._gaps += gap
                self._last_seq = seq
                self._frames += 1

    def _send(self, cmd, value):
        try:
            # serialized: the adapt thread and viewer-join force_idr() both send
            # on the same sectun session; concurrent sends corrupt the cipher stream
            with self._send_lock:
                self.sess.send(bytes([self.MAGIC, cmd]) +
                               struct.pack(">I", int(value) & 0xFFFFFFFF))
        except Exception as e:
            print(f">> adapt: send failed: {e}", file=sys.stderr, flush=True)

    def _loop(self):
        while not self._stop.wait(1.0):
            with self._lock:
                gaps, frames = self._gaps, self._frames
                kbps = self._bytes * 8 / 1000.0
                owd = self._owd_min
                self._owd_min = None
                self._gaps = self._frames = self._bytes = 0
            if frames == 0:                        # stream idle this window
                continue

            # Goodput = what the link actually delivered.  Commanding more
            # than this cannot reach the viewer; it only deepens the
            # camera's transmit queue, which IS the latency.
            self._gp_kbps = (0.7 * self._gp_kbps + 0.3 * kbps
                             if self._gp_kbps else kbps)

            # Baseline one-way delay = min over the last ~5 min, in 30 s
            # buckets so a slow clock drift is tracked instead of pinning
            # the baseline at an unreachable old minimum.
            queue_ms = 0.0
            now = time.time()
            if owd is not None:
                b = int(now // 30)
                if self._owd_buckets and self._owd_buckets[-1][0] == b:
                    self._owd_buckets[-1] = (b, min(self._owd_buckets[-1][1], owd))
                else:
                    self._owd_buckets.append((b, owd))
                self._owd_buckets = [x for x in self._owd_buckets if x[0] > b - 10]
                queue_ms = owd - min(x[1] for x in self._owd_buckets)

            # net_kbps feeds the operator's bitrate slider.  A pure
            # high-water mark only ever grows, so one lucky burst pins the
            # slider at a bandwidth the link has not carried for hours.
            self._nk_buckets.append((now, kbps))
            self._nk_buckets = [x for x in self._nk_buckets if x[0] > now - 60]
            self.net_kbps = int(max(x[1] for x in self._nk_buckets))

            self._refresh_policy()

            # Cap the ask at the measured goodput — but ONLY while a queue
            # is actually standing.  Capping unconditionally is a trap: a
            # low bitrate yields low goodput yields a low cap, and the
            # stream is pinned at the floor forever with no way to probe
            # back up.  With no queue there is no evidence the link is
            # full, so AIMD is allowed to probe; goodput binds only once
            # delay says we have over-committed, and then it binds BELOW
            # goodput so the standing queue drains instead of holding.
            draining = queue_ms > self.QUEUE_LO_MS
            if draining:
                cap = max(self.BR_MIN,
                          min(self.BR_MAX,
                              int(self._gp_kbps * 1000 * self.DRAIN_FRAC)))
            else:
                # Probing is allowed, but not blindly to BR_MAX: with a
                # ~0.8 Mbps link and a 6 Mbps ceiling the loop overshoots
                # by 7x every cycle, and each overshoot is a fresh multi-
                # second queue the viewer sees as lag.  Two times measured
                # goodput is enough headroom to discover real capacity
                # while keeping the sawtooth small.
                # Headroom is measured from the 60 s PEAK, not the EWMA.
                # The EWMA tracks what the encoder happened to emit, and a
                # dark static scene emits almost nothing — pinning the cap
                # to that would starve the very burst (someone walks in)
                # the stream exists to carry.  The peak is the best recent
                # evidence of what the link can actually pass.
                cap = max(self.BR_MIN,
                          min(self.BR_MAX,
                              int(self.net_kbps * 1000 * self.PROBE_FRAC)))
            congested = gaps > 0 or queue_ms > self.QUEUE_HI_MS

            if self.mode == "quality":
                # Frames are the sacrifice: hold the chosen fps and let
                # bitrate probe up to whatever the link carries.
                want_div = max(1, min(self.FPS_DIV_MAX,
                                      round(self.fps / max(1, self.target_fps))))
                if want_div != self.fps_div:
                    self.fps_div = want_div
                    self._send(self.CMD_FPS_DIV, self.fps_div)
                if congested:
                    self.br = max(int(self.br * 0.8), self.BR_MIN); self.n = 0
                else:
                    self.br = min(self.br + self.BR_UP, self.BR_MAX)
                    self.n = min(self.n + 1, self.N_MAX)
                self.br = max(self.BR_MIN, min(self.br, cap))
            else:
                # Motion is the priority: shed bitrate down to the operator's
                # floor first, and only then start dropping frames.  Recover
                # in the reverse order — frames back before bitrate — so the
                # stream returns to full motion as soon as it can.
                if congested:
                    if self.br > self.br_floor:
                        self.br = max(int(self.br * 0.8), self.br_floor)
                    elif self.fps_div < self.FPS_DIV_MAX:
                        self.fps_div += 1
                        self._send(self.CMD_FPS_DIV, self.fps_div)
                    self.n = 0
                else:
                    if self.fps_div > 1:
                        self.fps_div -= 1
                        self._send(self.CMD_FPS_DIV, self.fps_div)
                    else:
                        self.br = min(self.br + self.BR_UP, self.BR_MAX)
                        self.n = min(self.n + 1, self.N_MAX)
                # Never ask for more than the link carries.  The operator's
                # floor normally wins (below it we shed FRAMES, not
                # quality) — but a STANDING QUEUE outranks it.  A floor set
                # above what the link can pass is unsatisfiable, and
                # honouring it there just buys unbounded latency: the
                # viewer gets a perfectly-encoded picture of 30 s ago.
                # While the queue drains, quality yields.
                floor = self.BR_MIN if draining else self.br_floor
                self.br = max(floor, min(self.br, max(cap, floor)))
            gop = self.fps << self.n
            self._send(self.CMD_BITRATE, self.br)
            self._send(self.CMD_GOP, gop)
            print(f">> adapt[{self.mode}]: drops={gaps} rx={kbps:.0f}kbps "
                  f"gp={self._gp_kbps:.0f}kbps queue={queue_ms:.0f}ms "
                  f"cap={cap} -> bitrate={self.br} gop={gop} "
                  f"fps={self.fps // self.fps_div} (N={self.n})",
                  file=sys.stderr, flush=True)

    def _refresh_policy(self):
        """Pull the operator's strategy from the hub.  Failures are silent and
        keep the last known policy — a hub hiccup must not change how a
        camera streams."""
        now = time.time()
        if now - self._policy_at < self.POLICY_REFRESH_S:
            return
        self._policy_at = now
        cam = getattr(self, "cam_id", "") or os.environ.get("NN_CAM_ID", "")
        if not cam:
            return
        try:
            import urllib.request as _u
            with _u.urlopen(
                    f"http://127.0.0.1:8769/api/v1/cameras/{cam}/stream-policy",
                    timeout=3) as r:
                d = json.loads(r.read().decode())
        except Exception:
            return
        self.mode = d.get("mode", self.mode)
        self.level = int(d.get("level", self.level))
        val = d.get("value")
        if d.get("unit") == "fps" and val:
            self.target_fps = max(1, int(val))
        elif d.get("unit") == "kbps" and val:
            self.br_floor = max(self.BR_MIN, int(val) * 1000)


# ── ingest: nn_sectun (from the C6) or a test file ──────────────────────────
def ingest_c6(svc: VideoService, port: int, keydir: str):
    from nn_sectun import SecureSession
    from hub import crypto
    priv = crypto.load_or_generate_enc_key(Path(keydir))
    print(f">> stream service X25519 pub: {crypto.x25519_pubkey_bytes(priv).hex()}")
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(5)
    print(f">> nn_sectun ingest listening on 0.0.0.0:{port}")
    while True:
        conn, addr = srv.accept()
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        conn.settimeout(25)   # detect a dead/half-open peer → re-accept (flaky Wi-Fi).
                              # Must exceed the C6 net_task SO_SNDTIMEO (20 s) so a
                              # transient coex stall doesn't make the host tear down
                              # the session before the C6 rides through it.
        print(f">> C6 connected from {addr}")
        ctrl = None
        try:
            sess = SecureSession.accept(conn, priv)
            print(f">> handshake OK; device {sess.device_pub.hex()[:16]}...")
            parser = RecordParser()
            # A/V-sync element: order records by absolute device ts before they
            # go to the H.264 / AAC sinks (and onward to RTP).
            avsync = AVSync(
                on_video=lambda pl, ts: (svc.feed(pl), svc.ws_forward(pl, ts)),
                on_audio=lambda pl, ts: (svc.audio.feed(pl, ts) if svc.audio else None),
                window_ms=svc.avsync_window_ms)
            svc.avsync = avsync
            if getattr(svc, "adapt_enabled", False):
                ctrl = AdaptController(sess, fps=getattr(svc, "adapt_fps", 30))
                svc._adapt_ctrl = ctrl
                svc.ctrl = ctrl
                ctrl.start()
                print(">> adaptive control ON (server-driven)")
            # config channel: this session is how the device is configured,
            # so (re)push on every connect — the device ignores a version it
            # already has, making reconnect storms free.
            svc.sess = sess
            svc.device_policy_version = 0
            svc.push_device_config()
            import time as _t, sys as _sys
            _last = _t.time(); _rb = 0; _rv = 0; _ra = 0
            while True:
                data = sess.recv()
                _rb += len(data)
                for typ, flags, seq, ts, payload in parser.feed(data):
                    eng = getattr(svc, "event_engine", None)
                    if eng is not None and typ in (NN_REC_VIDEO, NN_REC_AUDIO):
                        eng.append("V" if typ == NN_REC_VIDEO else "A",
                                   ts, flags, payload)
                    if typ == NN_REC_STATUS:
                        try:
                            svc.cam_settings = json.loads(payload.decode())
                            # The ESP status record deliberately stamps its
                            # ts with RAW esp_timer uptime (never timesynced,
                            # unlike video records) — so this field IS the
                            # device's uptime_ms.  Keep it for /uptime.
                            svc.cam_settings_dev_ts = ts
                            svc.cam_settings_ts = time.time()
                            # Firmware identity ride-along: report it to the
                            # hub the same way the BeagleY camera's OTA agent
                            # does, so every camera's running version is
                            # visible in one place (webapp camera Settings).
                            # the slider's upper bound is the best rate this
                            # link has actually carried, so publish it
                            _ad = getattr(svc, "_adapt_ctrl", None)
                            if _ad is not None and getattr(_ad, "net_kbps", 0):
                                svc.cam_settings["net_kbps"] = _ad.net_kbps
                            _fw = svc.cam_settings.get("fw")
                            _img = svc.cam_settings.get("img")
                            # Re-report on change AND periodically: reporting
                            # once per connection made the hub's copy age out
                            # (and never recover if the hub lost it), so a
                            # camera that had not reconnected in hours looked
                            # stale even though it was streaming fine.
                            _due = (time.time() - getattr(svc, "_fw_reported_at", 0)) > 900
                            # Watchdog telemetry rides along: a change in the
                            # reset count re-reports immediately, so a camera
                            # that just self-recovered is visible at once.
                            _rst = svc.cam_settings.get("rst")
                            _wrc = svc.cam_settings.get("wdt_rc")
                            _sig = (_fw, _wrc)
                            if _fw and _img and (_sig != getattr(svc, "_fw_reported", None) or _due):
                                svc._fw_reported = _sig
                                svc._fw_reported_at = time.time()
                                try:
                                    import urllib.request as _u
                                    _cam = getattr(svc, "cam_id", None) or os.environ.get("NN_CAM_ID", "")
                                    if _cam:
                                        _doc = {"version": _fw, "device_type": _img}
                                        if _rst is not None: _doc["rst"] = _rst
                                        if _wrc is not None: _doc["wdt_rc"] = _wrc
                                        _b = json.dumps(_doc).encode()
                                        _rq = _u.Request(
                                            f"http://127.0.0.1:8769/api/v1/cameras/{_cam}/bundle",
                                            data=_b, method="PUT",
                                            headers={"Content-Type": "application/json"})
                                        _u.urlopen(_rq, timeout=5).read()
                                        print(f">> reported firmware {_fw} ({_img}) to hub")
                                except Exception as _e:
                                    print(f">> firmware report failed: {_e}")
                            if "infer" in svc.cam_settings:
                                svc.edge_caps = svc.cam_settings["infer"]
                                svc.apply_infer_mode()
                                print(f">> edge inference advertised: {svc.edge_caps}")
                        except Exception:
                            pass
                        continue
                    if typ == NN_REC_CFGACK:
                        try:
                            a = json.loads(payload.decode())
                            svc.device_policy_version = int(a.get("v") or 0)
                            print(f">> device applied config v"
                                  f"{svc.device_policy_version}"
                                  f"{'' if a.get('applied') else ' (REJECTED: %s)' % a.get('err')}",
                                  flush=True)
                        except Exception:
                            pass
                        continue
                    if typ == NN_REC_HEART:
                        try:
                            svc.device_hb = json.loads(payload.decode())
                            svc.device_hb_ts = time.time()
                        except Exception:
                            pass
                        continue
                    if typ == NN_REC_DETECT:
                        dets = parse_detect_record(payload)
                        if dets is not None and eng is not None:
                            eng.edge_tick(ts, dets)
                        continue
                    if typ == NN_REC_VIDEO:
                        _rv += 1
                        if ctrl: ctrl.on_video(seq, flags, len(payload), ts)
                    elif typ == NN_REC_AUDIO:
                        _ra += 1
                    avsync.feed(typ, ts, payload)         # order by ts → downstream
                if _t.time() - _last > 2:
                    print(f">> ingest: {_rb} B, video_recs={_rv} audio_recs={_ra} "
                          f"avsync={avsync.status()} buf={len(parser.buf)}",
                          file=_sys.stderr, flush=True)
                    _last = _t.time()
        except Exception as e:
            import traceback as _tb
            print(f">> ingest session ended: {e!r}\n{_tb.format_exc()}")
        finally:
            if ctrl: ctrl.stop()
            conn.close()


def _split_access_units(data: bytes) -> list[bytes]:
    """Split an Annex-B stream into access units, parameter sets attached to the
    frame they precede.  A new AU begins at a VCL NAL (type 1/5) when the current
    one already holds one — correct for single-slice-per-frame streams, which is
    what both the P4 encoder and x264 produce."""
    nals: list[tuple[int, int, bool]] = []       # (offset incl. start code, type, new_pic)
    i = 0
    while True:
        j = data.find(b"\x00\x00\x01", i)
        if j < 0:
            break
        start = j - 1 if j > 0 and data[j - 1] == 0 else j        # 4-byte form if present
        typ = data[j + 3] & 0x1F if j + 3 < len(data) else -1
        # first_mb_in_slice is the leading ue(v) of the slice header, and ue(v)==0
        # is the single bit 1 — so a set MSB means this slice starts a new picture.
        # Without this a multi-slice frame (x264 sliced-threads) would be split into
        # one AU per slice, and every AU but the first would reference a missing PPS.
        new_pic = typ in (1, 5) and j + 4 < len(data) and bool(data[j + 4] & 0x80)
        nals.append((start, typ, new_pic))
        i = j + 3

    # AUD/SEI/SPS/PPS belong to the picture that FOLLOWS them, so they must open the
    # next AU, not trail the previous one.  Get this wrong and every IDR access unit
    # starts bare: the ring still opens a GOP chunk on it, but the mp4 mux then fails
    # with "non-existing PPS 0 referenced" and the event is lost.
    LEADING = (6, 7, 8, 9)
    aus: list[bytes] = []
    cur, have_vcl = None, False
    for off, typ, new_pic in nals:
        if cur is None:
            cur = off
        elif have_vcl and (new_pic or typ in LEADING):    # the next picture starts here
            aus.append(data[cur:off])
            cur, have_vcl = off, False
        have_vcl = have_vcl or typ in (1, 5)
    if cur is not None:
        aus.append(data[cur:])
    return [a for a in aus if a]


def ingest_file(svc: VideoService, path: str, fps: float = 30.0):
    """Loop a raw Annex-B file in place of the C6, emulating its record framing.

    The C6 sends every access unit as one or more <=4 KB *fragments*, flagged
    VID_START (0x02) on the first and VID_END (0x04) on the last, each with an
    absolute device timestamp.  The event ring reassembles AUs from those flags
    (event_engine.mux_mp4) and opens a GOP when a VID_START fragment begins with
    SPS/IDR, so feeding undifferentiated chunks — as this used to — left it
    unable to find one complete AU and events muxed to nothing.

    Fragmenting the AUs also keeps the GStreamer side honest: it still receives
    partial access units, which is exactly the input that must NOT be declared
    alignment=au in APPSRC_CAPS.
    """
    import time
    data = open(path, "rb").read()
    aus = _split_access_units(data)
    print(f">> test ingest: looping {len(data)} bytes / {len(aus)} AUs from {path} "
          f"@ {fps:g} fps")
    step = 1.0 / fps if fps > 0 else 0.0
    ts = int(time.time() * 1000)                     # absolute epoch ms, like the device
    due = time.monotonic()
    while True:
        for au in aus:
            eng = getattr(svc, "event_engine", None)
            for off in range(0, len(au), 4096):
                piece = au[off:off + 4096]
                flags = (0x02 if off == 0 else 0) | \
                        (0x04 if off + 4096 >= len(au) else 0)
                if eng is not None:
                    eng.append("V", ts, flags, piece)
                svc.feed(piece)
                svc.ws_forward(piece)
            ts += int(round(step * 1000)) or 1
            due += step
            slack = due - time.monotonic()
            if slack > 0:
                time.sleep(slack)
            else:
                due = time.monotonic()               # fell behind: don't bank the debt


# ── WebRTC relay sink (aiortc) ───────────────────────────────────────────────
# Relays the live H.264 to browsers over WebRTC, in-process.  A single shared
# WAVE5-re-encoded RTP branch (127.0.0.1) feeds an aiortc MediaPlayer; a
# MediaRelay fans it out so N viewers share one transcode.  The re-encode is
# REQUIRED — the ESP32-P4 bitstream is non-conformant for libav/browsers (top
# rows decode, rest go green); WAVE5 (v4l2h264dec!v4l2h264enc) normalises it.
_WEBRTC_SDP = ("v=0\r\no=- 0 0 IN IP4 {ip}\r\ns=nn-camera\r\nc=IN IP4 {ip}\r\n"
               "t=0 0\r\nm=video {port} RTP/AVP {pt}\r\n"
               "a=rtpmap:{pt} H264/90000\r\na=fmtp:{pt} packetization-mode=1\r\n")

_WEBRTC_PAGE = """<!doctype html><meta charset=utf-8><title>nn-camera WebRTC</title>
<style>body{margin:0;background:#111}video{width:100vw;height:100vh;object-fit:contain}</style>
<video id=v autoplay playsinline muted controls></video>
<div id=nnb style="position:fixed;bottom:10px;right:12px;z-index:9;color:#ffd479;font:12px/1 ui-monospace,monospace;background:rgba(0,0,0,.45);padding:4px 7px;border-radius:6px"></div>
<script>
// yolo timing + capture timestamp badge (shared across /live, /hls, /webrtc).
// yolo_ms is the event engine's LAST inference — it stays 0 until motion has
// triggered YOLO at least once (a black static scene never does).
window.__nnts = 0;                                  // ws text frames override (per-frame accurate)
function nnFmtTs(ms){
  if(!ms) return "";
  if(ms < 1e12){ var t=Math.floor(ms/1000);        // unsynced device -> uptime
    return "up "+Math.floor(t/3600)+":"+String(Math.floor(t/60)%60).padStart(2,"0")+":"+String(t%60).padStart(2,"0"); }
  var d=new Date(ms);
  return d.toLocaleTimeString(undefined,{hour12:false})+"."+String(d.getMilliseconds()).padStart(3,"0");
}
setInterval(async function(){
  var y=0, ts=window.__nnts;
  try{ var j=await (await fetch("/api/event/live")).json(); y=j.yolo_ms||0; }catch(e){}
  if(!ts){ try{ var m=await (await fetch("/motion")).json(); ts=m.ts||0; }catch(e){} }
  document.getElementById("nnb").textContent="yolo="+y+"ms"+(ts?"  "+nnFmtTs(ts):"");
}, 1000);
</script><script>
(async()=>{const pc=new RTCPeerConnection();
pc.addTransceiver('video',{direction:'recvonly'});
pc.ontrack=e=>{document.getElementById('v').srcObject=e.streams[0]};
const o=await pc.createOffer();await pc.setLocalDescription(o);
const r=await fetch('/webrtc/offer',{method:'POST',headers:{'content-type':'application/json'},
  body:JSON.stringify({sdp:pc.localDescription.sdp})});
await pc.setRemoteDescription(await r.json());})();
</script>"""


def _patch_aiortc_x264_preset():
    """aiortc encodes H.264 senders with libx264 at the default 'medium'
    preset — only a few fps at 720p on A53 cores, so browsers that prefer
    H.264 over VP8 see a stalled/black video.  Wrap the encoder factory to
    force preset=ultrafast (realtime; quality is fine for a live monitor)."""
    try:
        import fractions
        import av as _av
        import aiortc.codecs.h264 as _h264
        if getattr(_h264.create_encoder_context, "_nn_patched", False):
            return
        def _fast(codec_name, width, height, bitrate):
            # mirror of aiortc's factory, with preset set BEFORE open()
            codec = _av.CodecContext.create(codec_name, "w")
            codec.width = width
            codec.height = height
            codec.bit_rate = bitrate
            codec.pix_fmt = "yuv420p"
            codec.framerate = fractions.Fraction(_h264.MAX_FRAME_RATE, 1)
            codec.time_base = fractions.Fraction(1, _h264.MAX_FRAME_RATE)
            codec.options = {"level": "31", "tune": "zerolatency",
                             "preset": "ultrafast"}
            codec.profile = "Baseline"
            codec.open()
            return codec
        _fast._nn_patched = True
        _h264.create_encoder_context = _fast
    except Exception as e:
        print(f">> WARN: x264 preset patch failed: {e}", file=sys.stderr, flush=True)


class WebRTCSink:
    def __init__(self, svc: "VideoService"):
        _patch_aiortc_x264_preset()
        self.svc = svc
        self._player = None
        self._relay = None
        self._branch_id = -1
        self._sdp_path = None
        self._last_frame = 0.0   # wall time of the last frame the source decoded
        self._watch_task = None
        self.pcs = set()
        self._src_lock = None   # created lazily on the aiohttp loop

    async def _ensure_source(self):
        """Create — or transparently REBUILD — the shared WAVE5→RTP→MediaPlayer
        source.  Liveness-based: a heartbeat subscriber stamps _last_frame per
        decoded frame; a viewer joining against a stale source (device
        rebooted, branch died, decoder wedged) tears it down and rebuilds.
        This is robust where teardown-on-last-viewer was not: zombie pcs
        (browser tabs that vanish without close) can't pin a dead source."""
        import asyncio, time
        if self._src_lock is None:
            self._src_lock = asyncio.Lock()
        async with self._src_lock:
            if self._player is not None:
                if time.time() - self._last_frame < 4.0:
                    return                      # source demonstrably live
                print(">> WebRTC: source stale — rebuilding", file=sys.stderr, flush=True)
                await self._destroy_locked()
            import tempfile, socket
            from aiortc.contrib.media import MediaPlayer, MediaRelay
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
            # 6 Mbit/s re-encode (was 2.5M — that softness was the main WebRTC
            # quality hit, on top of UDP loss).  The link does ~35 Mbit/s and the
            # device source is capped at 6 Mbit/s, so this is ~transparent.
            bid = self.svc.add_rtp_branch("127.0.0.1", port, pt=96, reencode=True,
                                          bitrate=6_000_000, gop=30)
            if bid < 0:
                raise RuntimeError("failed to add local RTP branch for WebRTC")
            f = tempfile.NamedTemporaryFile("w", suffix=".sdp", delete=False)
            f.write(_WEBRTC_SDP.format(ip="127.0.0.1", port=port, pt=96)); f.close()
            self._sdp_path = f.name; self._branch_id = bid
            # av.open blocks until RTP arrives — run it off the event loop so a
            # slow/dead branch can't wedge every HTTP endpoint of the service.
            loop = asyncio.get_running_loop()
            def _open():
                # decode=False → H.264 PASSTHROUGH: aiortc forwards the already
                # conformant WAVE5-re-encoded RTP straight to the browser with NO
                # second decode+software-encode (that hidden transcode was the
                # main WebRTC quality/CPU hit).  Browser decodes the H.264 itself.
                return MediaPlayer(
                    f.name, format="sdp", decode=False,
                    options={"protocol_whitelist": "file,udp,rtp",
                             "fflags": "nobuffer", "max_delay": "500000"})
            self._player = await loop.run_in_executor(None, _open)
            self._relay = MediaRelay()
            self._last_frame = time.time()      # grace until first frame lands
            self._watch_task = asyncio.ensure_future(self._watch(self._player, self._relay))
            print(f">> WebRTC: source branch {bid} up (WAVE5 re-encode → udp 127.0.0.1:{port})",
                  file=sys.stderr, flush=True)

    async def _watch(self, player, relay):
        """Heartbeat subscriber: continuously consume the source so (a) the
        MediaPlayer never loses its last consumer (the stale-pipeline bug) and
        (b) _last_frame tracks real decode liveness for _ensure_source."""
        import time, contextlib
        track = relay.subscribe(player.video, buffered=False)
        try:
            while player is self._player:
                await track.recv()
                self._last_frame = time.time()
        except Exception:
            pass
        finally:
            with contextlib.suppress(Exception):
                track.stop()

    async def _destroy_locked(self):
        """Tear down the current source (caller holds _src_lock)."""
        import contextlib, os
        if self._watch_task:
            self._watch_task.cancel()
            self._watch_task = None
        if self._player is not None:
            with contextlib.suppress(Exception):
                self._player.video.stop()
        if self._branch_id >= 0:
            with contextlib.suppress(Exception):
                self.svc.remove_branch(self._branch_id)
        if self._sdp_path:
            with contextlib.suppress(Exception):
                os.unlink(self._sdp_path)
        self._player = None; self._relay = None
        self._branch_id = -1; self._sdp_path = None

    async def answer(self, offer_sdp: str) -> str:
        import contextlib
        from aiortc import RTCPeerConnection, RTCSessionDescription, RTCRtpSender
        # reap zombie pcs (tabs that vanished without close)
        for old in list(self.pcs):
            if old.connectionState in ("failed", "closed"):
                self.pcs.discard(old)
                with contextlib.suppress(Exception):
                    await old.close()
        await self._ensure_source()
        pc = RTCPeerConnection()
        self.pcs.add(pc)

        @pc.on("connectionstatechange")
        async def _on_state():
            print(f">> WebRTC pc: {pc.connectionState} (viewers={len(self.pcs)})",
                  file=sys.stderr, flush=True)
            if pc.connectionState in ("failed", "closed"):
                self.pcs.discard(pc)
                with contextlib.suppress(Exception):
                    await pc.close()

        pc.addTrack(self._relay.subscribe(self._player.video))
        # decode=False passthrough emits ONLY H.264 — pin the video transceiver to
        # H.264 BEFORE setRemoteDescription so aiortc can't answer VP8 (which would
        # leave the passthrough sender with a codec it can't produce → zero frames,
        # exactly what a VP8-first Chrome offer triggered).  Set here (not after
        # setRemoteDescription) because the post-negotiation reorder was ignored.
        h264 = [c for c in RTCRtpSender.getCapabilities("video").codecs
                if c.mimeType == "video/H264"]
        if h264:
            for t in pc.getTransceivers():
                if t.kind == "video":
                    with contextlib.suppress(Exception):
                        t.setCodecPreferences(h264)
        await pc.setRemoteDescription(RTCSessionDescription(sdp=offer_sdp, type="offer"))
        await pc.setLocalDescription(await pc.createAnswer())
        return pc.localDescription.sdp


# ── control API ─────────────────────────────────────────────────────────────
@web.middleware
async def _security_headers_mw(request, handler):
    """Silence Chrome DevTools audit warnings: nosniff + explicit no-store
    (everything this service serves is live/dynamic)."""
    resp = await handler(request)
    try:
        resp.headers.setdefault("X-Content-Type-Options", "nosniff")
        resp.headers.setdefault("Cache-Control", "no-store")
    except Exception:
        pass
    return resp


DIAG_HTML = """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1"><title>ISP Diagnostics</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;margin:0;background:#111;color:#eee}
header{padding:12px 16px;background:#1c1c22;font-size:18px;font-weight:600;position:sticky;top:0}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:14px;padding:16px}
.card{background:#1c1c22;border:1px solid #2a2a33;border-radius:8px;overflow:hidden}
.card img{width:100%;display:block;background:#000;cursor:zoom-in}
.meta{padding:8px 10px}
.nm{font-weight:600;font-size:13px;word-break:break-all}
.ts{color:#9aa;margin:2px 0 6px;font-size:12px}
table{width:100%;border-collapse:collapse;font-size:11px}
td{padding:1px 4px;border-bottom:1px solid #26262e}
td.k{color:#8ab;white-space:nowrap}td.v{color:#dde;text-align:right;font-variant-numeric:tabular-nums}
.empty{padding:48px;text-align:center;color:#889}
</style></head><body>
<header>ISP Diagnostics <span id=cnt style="color:#889;font-weight:400"></span></header>
<div id=g class=grid></div>
<script>
function fmtTs(t){if(!t)return '';try{return new Date(t).toLocaleString()}catch(e){return t}}
async function load(){
  let j; try{j=await (await fetch('/diag/list')).json()}catch(e){return}
  const g=document.getElementById('g');
  document.getElementById('cnt').textContent='('+j.items.length+')';
  if(!j.items.length){g.innerHTML='<div class=empty>No diagnostic captures yet. Flash the ISP-diag firmware and it will push raw frames here.</div>';return}
  g.innerHTML='';
  for(const it of j.items){
    const s=it.settings||{};let rows='<tr><td class=k>resolution</td><td class=v>'+it.w+'\\u00d7'+it.h+' '+(it.fmt||'')+'</td></tr>';
    for(const k of Object.keys(s))rows+='<tr><td class=k>'+k+'</td><td class=v>'+s[k]+'</td></tr>';
    const d=document.createElement('div');d.className='card';
    d.innerHTML='<img loading=lazy src="/diag/img/'+it.name+'.jpg" onclick="window.open(this.src)">'+
      '<div class=meta><div class=nm>'+(it.device||it.name)+'</div><div class=ts>'+fmtTs(it.ts)+'</div>'+
      '<table>'+rows+'</table></div>';
    g.appendChild(d);
  }
}
load();setInterval(load,5000);
</script></body></html>"""


def make_app(svc: VideoService) -> web.Application:
    # ── ISP diagnostics: firmware pushes raw ISP frames + settings; we JPEG them ──
    _DIAG_DIR = Path(getattr(svc, "diag_dir", None) or
                     (Path(os.path.dirname(os.path.abspath(__file__))) / "diag"))
    _DIAG_DIR.mkdir(parents=True, exist_ok=True)

    def _save_diag(meta, raw):
        from PIL import Image
        w = int(meta.get("w", 0)); h = int(meta.get("h", 0))
        fmt = str(meta.get("fmt", "RGB888")).upper()
        if fmt in ("RGB888", "RGB24", "RGB"):
            im = Image.frombytes("RGB", (w, h), bytes(raw[:w * h * 3]))
        elif fmt in ("RGB565", "RGBP"):
            a = np.frombuffer(bytes(raw[:w * h * 2]), dtype="<u2").reshape(h, w)
            r = ((a >> 11) & 0x1F) << 3; g = ((a >> 5) & 0x3F) << 2; b = (a & 0x1F) << 3
            im = Image.fromarray(np.dstack([r, g, b]).astype("uint8"), "RGB")
        elif fmt in ("GRAY", "GRAY8", "Y"):
            im = Image.frombytes("L", (w, h), bytes(raw[:w * h]))
        elif fmt in ("BGGR8", "SBGGR8", "BAYER8"):
            # simple 2x2-block demosaic (half res) — enough to see colour + geometry
            a = np.frombuffer(bytes(raw[:w * h]), dtype=np.uint8).reshape(h, w)
            B = a[0::2, 0::2].astype(np.uint16)
            G = (a[0::2, 1::2].astype(np.uint16) + a[1::2, 0::2].astype(np.uint16)) // 2
            R = a[1::2, 1::2].astype(np.uint16)
            hh = min(B.shape[0], G.shape[0], R.shape[0]); ww = min(B.shape[1], G.shape[1], R.shape[1])
            im = Image.fromarray(np.dstack([R[:hh, :ww], G[:hh, :ww], B[:hh, :ww]]).astype("uint8"), "RGB")
        else:
            raise ValueError("unsupported fmt " + fmt)
        dev = "".join(c for c in str(meta.get("device", "cam")) if c.isalnum() or c in "-_")[:40] or "cam"
        ts = int(meta.get("ts", int(time.time() * 1000)))
        base = "%s_%d" % (dev, ts)
        im.save(str(_DIAG_DIR / (base + ".jpg")), quality=92)
        with open(str(_DIAG_DIR / (base + ".json")), "w") as f:
            json.dump(meta, f, indent=1)
        # keep the raw bytes for Bayer captures so the phase can be tested host-side
        if fmt in ("BGGR8", "SBGGR8", "BAYER8"):
            with open(str(_DIAG_DIR / (base + ".raw")), "wb") as f:
                f.write(bytes(raw[:w * h]))
        return base

    async def diag_isp(req):
        """WS: firmware sends a TEXT meta frame then BINARY raw-pixel chunks."""
        import aiohttp
        ws = web.WebSocketResponse(max_msg_size=64 * 1024 * 1024, heartbeat=30)
        await ws.prepare(req)
        peer = req.remote
        print(">> /diag/isp connected from %s" % peer, file=sys.stderr, flush=True)
        meta = None; buf = bytearray()
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    meta = json.loads(msg.data); buf = bytearray()
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    if meta is None:
                        continue
                    buf += msg.data
                    if len(buf) >= int(meta.get("bytes", 1 << 62)):
                        try:
                            base = _save_diag(meta, buf)
                            await ws.send_str(json.dumps({"ok": True, "saved": base}))
                            print(">> /diag/isp saved %s (%d B)" % (base, len(buf)),
                                  file=sys.stderr, flush=True)
                        except Exception as e:
                            await ws.send_str(json.dumps({"ok": False, "err": str(e)}))
                            print(">> /diag/isp save error: %r" % e, file=sys.stderr, flush=True)
                        meta = None; buf = bytearray()
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        return ws

    async def diag_list(req):
        items = []
        for jf in sorted(_DIAG_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
            base = jf.stem
            if not (_DIAG_DIR / (base + ".jpg")).exists():
                continue
            try:
                meta = json.loads(jf.read_text())
            except Exception:
                meta = {}
            items.append({"name": base, "device": meta.get("device"), "ts": meta.get("ts"),
                          "w": meta.get("w"), "h": meta.get("h"), "fmt": meta.get("fmt"),
                          "settings": meta.get("settings", {})})
        return web.json_response({"items": items})

    async def diag_img(req):
        name = os.path.basename(req.match_info["name"])
        p = _DIAG_DIR / name
        if not p.exists():
            return web.Response(status=404, text="not found")
        return web.FileResponse(str(p))

    async def diag_page(req):
        return web.Response(text=DIAG_HTML, content_type="text/html")

    async def status(req):
        return web.json_response(svc.status())

    async def health(req):
        """For the nn-video supervisor: liveness of the pieces that can die
        quietly.  fed = H.264 access units pushed into the pipeline;
        threads = the worker threads currently alive by name."""
        hls_age = None
        try:
            hdir = svc.hls_dir or os.path.join(os.path.dirname(os.path.abspath(__file__)), "hls_live")
            segs = [os.path.getmtime(os.path.join(hdir, f)) for f in os.listdir(hdir) if f.endswith(".ts")]
            hls_age = round(time.time() - max(segs), 1) if segs else None
        except Exception:
            pass
        return web.json_response({
            "fed": svc._fed,
            "uptime_s": int(time.time() - getattr(svc, "started_at", time.time())),
            "hls_newest_age_s": hls_age,
            "device_hb_age_s": (round(time.time() - svc.device_hb_ts, 1)
                                if getattr(svc, "device_hb_ts", 0) else None),
            "threads": sorted(t.name for t in threading.enumerate()),
        })

    async def event_status(req):
        eng = getattr(svc, "event_engine", None)
        return web.json_response(eng.status() if eng else {"enabled": False})

    async def device_status(req):
        """What the DEVICE says about itself: its advertised capabilities, its
        detector self-test result and its heartbeat.

        The device has been sending this all along (NN_REC_STATUS ->
        svc.cam_settings) and nothing ever exposed it, so the one field whose
        whole purpose is to explain a dead edge engine ("infer_error") could
        never reach an operator or a test.  A camera with a broken detector
        emits the same 'D' records as a camera looking at a blank wall, so
        without this the two are indistinguishable from outside."""
        eng = getattr(svc, "event_engine", None)
        hb_ts = getattr(svc, "device_hb_ts", 0) or 0
        return web.json_response({
            "settings": getattr(svc, "cam_settings", None),
            "edge_caps": svc.edge_caps,
            "heartbeat": getattr(svc, "device_hb", None),
            "heartbeat_age_s": (round(time.time() - hb_ts, 1) if hb_ts else None),
            "policy_version": getattr(svc, "device_policy_version", None),
            "yolo_src": (eng.stats.get("yolo_src") if eng else None)})

    async def infer_mode_get(req):
        return web.json_response({
            "mode": svc.infer_mode,
            "edge_caps": svc.edge_caps,
            "local_infer_active": svc.local_infer_active()})

    async def infer_mode_set(req):
        try:
            body = await req.json()
        except Exception:
            return web.json_response({"error": "bad json"}, status=400)
        if not svc.set_infer_mode(body.get("mode", "")):
            return web.json_response({"error": "mode must be auto|on|off"},
                                     status=400)
        return web.json_response({"mode": svc.infer_mode,
                                  "local_infer_active": svc.local_infer_active()})

    async def event_live(req):
        """Latest detection boxes for the /live overlay (cheap, polled ~3 Hz)."""
        eng = getattr(svc, "event_engine", None)
        if eng is None:
            return web.json_response({"detections": [], "w": 1280, "h": 720})
        w, h = eng.frame_wh
        return web.json_response({
            "detections": eng.last_dets, "ts": eng.last_dets_ts,
            "w": w, "h": h, "active": bool(eng._active),
            "yolo_ms": eng.stats.get("yolo_ms", 0)})

    async def event_config(req):
        eng = getattr(svc, "event_engine", None)
        if eng is None:
            return web.json_response({"error": "event engine disabled"}, status=503)
        body = await req.json()
        for k in ("motion_thresh", "quiet_s", "min_score", "yolo_interval_s"):
            if k in body:
                setattr(eng, k, float(body[k]))
        return web.json_response(eng.status())

    async def event_ring_bin(req):
        """Debug: stream the A/V ring as [kind u8][ts u64][flags u8][len u32][payload]."""
        eng = getattr(svc, "event_engine", None)
        if eng is None:
            return web.json_response({"error": "no engine"}, status=503)
        recs = eng.ring.snapshot_from(0)
        import struct as _st
        resp = web.StreamResponse()
        resp.content_type = "application/octet-stream"
        await resp.prepare(req)
        for kind, ts, flags, payload in recs:
            await resp.write(_st.pack(">BQBI", ord(kind), ts, flags, len(payload)) + payload)
        await resp.write_eof()
        return resp

    async def event_test(req):
        eng = getattr(svc, "event_engine", None)
        if eng is None:
            return web.json_response({"error": "event engine disabled"}, status=503)
        body = {}
        try:
            body = await req.json()
        except Exception:
            pass
        eid = eng.force_event(float(body.get("duration_s", 6.0)))
        return web.json_response({"started": eid})

    async def cam_provinfo(req):
        """What a camera must be told to talk to THIS service.

        Provisioning needs the stream endpoint AND this instance's
        stream pubkey — each camera service owns its own keydir, so the
        hub key is the wrong answer (a camera given it completes ECDH
        and then fails every sealed record with InvalidTag).  Exposing
        it here means the add-device wizard never has to ask an operator
        for values the deployment already knows."""
        from pathlib import Path as _P
        try:
            from hub import crypto as _c
        except Exception:
            import crypto as _c                      # noqa: F401
        try:
            priv = _c.load_or_generate_enc_key(_P(svc.keydir))
            pub = priv.public_key().public_bytes_raw().hex()
        except Exception as e:                       # noqa: BLE001
            return web.json_response({"error": f"keydir unreadable: {e}"},
                                     status=500)
        return web.json_response({
            "cam_id": getattr(svc, "cam_id", "") or os.environ.get("NN_CAM_ID", ""),
            "stream_port": svc.stream_port,
            "stream_pub": pub,
            "keydir": svc.keydir,
        })

    async def cam_uptime(req):
        """Device uptime, from whichever channel this camera already has:
        the BeagleY app's heartbeat record carries up_s (5 s cadence), the
        ESP cameras' periodic status record carries "up" (fw 36162d9+ /
        next flash).  No new traffic — this only reads what arrived."""
        import time as _t
        hb = getattr(svc, "device_hb", None)
        if hb and "up_s" in hb:
            return web.json_response({
                "up_s": int(hb.get("up_s") or 0),
                "age_s": round(_t.time() - getattr(svc, "device_hb_ts", 0), 1),
                "source": "heartbeat"})
        cs = getattr(svc, "cam_settings", None) or {}
        if "up" in cs:
            return web.json_response({
                "up_s": int(cs.get("up") or 0),
                "age_s": round(_t.time() - getattr(svc, "cam_settings_ts", 0), 1),
                "source": "status"})
        # Older ESP firmware has no "up" field, but its status record's ts
        # is raw uptime_ms anyway (magnitude check guards against a future
        # firmware that stamps epoch there: 1e11 ms ~ 3 years of uptime).
        dev_ts = getattr(svc, "cam_settings_dev_ts", 0)
        if cs and 0 < dev_ts < 1e11:
            return web.json_response({
                "up_s": int(dev_ts / 1000 +
                            (_t.time() - getattr(svc, "cam_settings_ts", 0))),
                "age_s": round(_t.time() - getattr(svc, "cam_settings_ts", 0), 1),
                "source": "status-ts"})
        return web.json_response(
            {"error": "device does not report uptime "
                      "(firmware predates it)"}, status=404)

    async def cam_clear(req):
        """Unregister: one CTRL_CLEAR down the control channel.  The
        device arms a wipe flag and restarts itself; its KV store (which
        holds the identity keypair) is erased on the way up, so it comes
        back with a NEW pubkey — unauthorized until an operator adds it
        to the keydir again.  Fire-and-forget like reboot."""
        ctrl = getattr(svc, "_adapt_ctrl", None) or getattr(svc, "ctrl", None)
        if ctrl is None:
            return web.json_response(
                {"error": "no control session — camera not connected"},
                status=503)
        try:
            ctrl._send(ctrl.CMD_CLEAR_USER_DATA, 0)
        except Exception as e:                       # noqa: BLE001
            return web.json_response({"error": f"send failed: {e}"}, status=502)
        # The ctrl channel is fire-and-forget, and firmware that predates
        # cmd 21 silently drops it — which once let the hub archive a
        # camera "as cleared" while the device streamed on unchanged.
        # Detection: a device that honors CLEAR goes DOWN (its periodic
        # records stop while it restarts — cam3 takes ~30 s to return),
        # while an ignoring device keeps its 5 s status/heartbeat cadence
        # right through the window.  (First cut compared the ctrl object
        # identity, but a dead controller lingers past any short grace —
        # false "ignored" on cam3, measured 2026-08-20.)
        import time as _t
        def _last_seen():
            return max(getattr(svc, "device_hb_ts", 0) or 0,
                       getattr(svc, "cam_settings_ts", 0) or 0)
        # Silence only MEANS something if the device was chattering
        # BEFORE the command.  A camera that was already quiet (wedged
        # session, lost uplink) would otherwise read as "cleared" while
        # nothing happened — observed 2026-08-21 on cam1, whose sealed
        # ctrl channel could not even decrypt the command.
        if _t.time() - _last_seen() > 6.5:
            return web.json_response(
                {"ok": True, "sent": "clear_user_data", "applied": False,
                 "reason": "device was already silent before the command — "
                           "cannot confirm it was received"})
        t_sent = _t.time()
        applied = False
        for _ in range(10):                    # up to ~10 s
            await asyncio.sleep(1.0)
            if _t.time() - max(_last_seen(), t_sent) > 6.5:
                applied = True                 # silent past a full cadence
                break
        return web.json_response({"ok": True, "sent": "clear_user_data",
                                  "applied": applied})

    async def cam_reboot(req):
        """Operator reboot: one CTRL_REBOOT down the device control channel.
        Fire-and-forget by design — the device drops the session as it goes
        down, so the proof of work is the stream coming back, not an ack.
        Firmware that predates the command ignores it (both camera apps
        drop unknown ctrl cmds), which the caller sees as "no effect"."""
        ctrl = getattr(svc, "_adapt_ctrl", None) or getattr(svc, "ctrl", None)
        if ctrl is None:
            return web.json_response(
                {"error": "no control session — camera not connected"},
                status=503)
        try:
            ctrl._send(ctrl.CMD_REBOOT, 0)
        except Exception as e:                       # noqa: BLE001
            return web.json_response({"error": f"send failed: {e}"}, status=502)
        return web.json_response({"ok": True, "sent": "reboot",
                                  "note": "watch the stream drop and return"})

    async def cam_settings(req):
        cfg = getattr(svc, "cam_settings", None)
        ts = getattr(svc, "cam_settings_ts", 0)
        return web.json_response({"settings": cfg, "age_s": round(time.time() - ts, 1) if cfg else None})

    _CAM_CMDS = {"bitrate": 1, "gop": 2, "fps_div": 3, "wb_r": 5, "wb_b": 6,
                 "focus": 7, "exposure": 8, "brightness": 9, "contrast": 10,
                 "saturation": 11}

    async def cam_set(req):
        body = await req.json()
        ctrl = getattr(svc, "ctrl", None)
        if ctrl is None:
            return web.json_response({"error": "no device session"}, status=503)
        applied = {}
        loop = asyncio.get_running_loop()
        for name, val in body.items():
            cmd = _CAM_CMDS.get(name)
            if cmd is None:
                continue
            # ctrl._send is a blocking socket write — keep it off the loop
            await loop.run_in_executor(None, ctrl._send, cmd, int(val))
            applied[name] = int(val)
        return web.json_response({"applied": applied})

    async def add_branch(req):
        body = await req.json()
        transport = body.get("transport", "rtp")
        port = int(body["port"])
        if transport == "tcp":
            bid = svc.add_tcp_branch(port)
            if bid < 0:
                return web.json_response({"error": "failed to add branch"}, status=500)
            return web.json_response({"id": bid, "host": "127.0.0.1", "port": port,
                                      "transport": "tcp", "encoding": "H264"})
        host = body["host"]
        pt = int(body.get("pt", 96))
        reencode = bool(body.get("reencode", False))
        bitrate = int(body.get("bitrate", 2500000))
        gop = int(body.get("gop", 30))
        bid = svc.add_rtp_branch(host, port, pt, reencode=reencode,
                                 bitrate=bitrate, gop=gop)
        if bid < 0:
            return web.json_response({"error": "failed to add branch"}, status=500)
        return web.json_response({"id": bid, "host": host, "port": port, "pt": pt,
                                  "encoding": "H264/90000", "reencode": reencode})

    async def del_branch(req):
        bid = int(req.match_info["id"])
        ok = svc.remove_branch(bid)
        return web.json_response({"removed": ok}, status=200 if ok else 404)

    async def motion(req):
        return web.json_response(svc.motion_status())

    async def ws_h264(req):
        # Live view (passthrough): stream raw P4 H.264 to the browser; the
        # browser's hardware decoder plays it (via jMuxer/MSE).  No transcode.
        import asyncio
        ws = web.WebSocketResponse(max_msg_size=0)
        await ws.prepare(req)
        svc.aio_loop = asyncio.get_running_loop()
        q = asyncio.Queue(maxsize=300)
        svc.h264_subs.add(q)
        # fast join: ask the device for an immediate IDR so the browser can
        # start decoding now rather than up to one adaptive GOP (~10s) later
        # (join force-IDR removed: device encoder restarts leak memory; the
        # adaptive GOP is capped at 120 so a joiner decodes within ~5 s anyway)
        print(">> /ws client connected; subs=%d loop=set" % len(svc.h264_subs),
              file=sys.stderr, flush=True)
        try:
            while True:
                data = await q.get()
                if isinstance(data, str):
                    await ws.send_str(data)      # {"ts": <epoch-ms>} capture time
                else:
                    await ws.send_bytes(data)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            svc.h264_subs.discard(q)
        return ws

    async def live_page(req):
        return web.Response(text=LIVE_HTML, content_type="text/html")

    async def jmuxer_js(req):
        import os
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jmuxer.min.js")
        if not os.path.exists(p):
            return web.Response(status=404, text="jmuxer.min.js not found next to video_service.py")
        return web.FileResponse(p, headers={"Content-Type": "application/javascript"})

    async def snapshot(req):
        # Debug: return the detection branch's latest decoded luma frame (raw
        # GRAY8) — a reliable in-process view of the live camera, independent of
        # the RTP branch path.
        det = svc._det
        prev = getattr(det, "_prev", None) if det is not None else None
        if prev is None:
            return web.json_response({"error": "no frame yet"}, status=503)
        y = np.ascontiguousarray(prev).astype(np.uint8)
        h, w = y.shape
        return web.Response(body=y.tobytes(), content_type="application/octet-stream",
                            headers={"X-Width": str(w), "X-Height": str(h)})

    async def snapshot_jpg(req):
        # Colour snapshot.  Default: serve the latest PERIODICALLY-SAVED clean
        # snapshot (a freshly-decoded IDR → never a corrupt P-frame from the
        # lossy detection branch; correct BT.709 colour).  ?fresh=1 decodes a
        # new IDR now.  Falls back to the detection branch's last frame only if
        # no keyframe has been cached yet.
        import io
        latest = os.path.join(svc.snapshot_dir, "latest.jpg")
        if not req.query.get("fresh"):
            try:
                if os.path.exists(latest):
                    return web.FileResponse(latest, headers={"Cache-Control": "no-store"})
            except Exception:
                pass
        jpg = svc.grab_snapshot_jpeg()
        if jpg is not None:
            return web.Response(body=jpg, content_type="image/jpeg",
                                headers={"Cache-Control": "no-store"})
        from PIL import Image
        det = svc._det
        lc = getattr(det, "_last_color", None) if det is not None else None
        if lc is None:
            return web.json_response({"error": "no colour frame yet"}, status=503)
        yuv, w, h = lc
        from yolox_detector import yuv420_to_rgb
        try:    q = max(30, min(95, int(req.query.get("q", 90))))
        except Exception: q = 90
        buf = io.BytesIO()
        Image.fromarray(yuv420_to_rgb(yuv, w, h)).save(buf, format="JPEG", quality=q)
        return web.Response(body=buf.getvalue(), content_type="image/jpeg",
                            headers={"Cache-Control": "no-store"})

    async def pipeline_stats(req):
        # Per-queue buffer drop accounting across the live GStreamer pipeline.
        return web.json_response(svc.pipeline_stats())

    async def snapshot_config(req):
        # User setting: periodic-save interval (seconds).  GET reads, POST sets.
        import time as _t
        if req.method == "POST":
            try:
                body = await req.json()
            except Exception:
                body = {}
            if body.get("interval_s") is not None:
                try:
                    svc.snapshot_interval = max(0, int(body["interval_s"]))
                except Exception:
                    return web.json_response({"error": "interval_s must be an integer"},
                                             status=400)
        return web.json_response({
            "interval_s":        svc.snapshot_interval,
            "dir":               svc.snapshot_dir,
            "last_saved_age_s":  (round(_t.time() - svc.snapshot_last_ts, 1)
                                  if svc.snapshot_last_ts else None),
        })

    async def mjpeg(req):
        # Live view: stream the detection branch's already-decoded luma frames as
        # motion-JPEG (grayscale).  Reuses frames the pipeline already decodes, so
        # no WAVE5 contention and no transcode branch — viewable in any browser at
        # http://<host>:8899/mjpeg
        import asyncio, io
        from PIL import Image
        # Query knobs: ?fps=20&h=720&q=75  (downscale keeps the JPEG encode cheap
        # so higher fps doesn't re-saturate the CPU and bring the mosaic back).
        try:    fps = max(1, min(30, int(req.query.get("fps", 20))))
        except Exception: fps = 20
        try:    out_h = max(120, min(1080, int(req.query.get("h", 720))))
        except Exception: out_h = 720
        try:    q = max(30, min(95, int(req.query.get("q", 75))))
        except Exception: q = 75
        period = 1.0 / fps
        boundary = "nnframe"
        resp = web.StreamResponse(status=200, headers={
            "Content-Type": "multipart/x-mixed-replace; boundary=%s" % boundary,
            "Cache-Control": "no-cache, no-store", "Connection": "close"})
        await resp.prepare(req)
        try:
            while True:
                t0 = time.monotonic()
                det = svc._det
                prev = getattr(det, "_prev", None) if det is not None else None
                if prev is not None:
                    img = Image.fromarray(np.ascontiguousarray(prev).astype(np.uint8))
                    if img.height > out_h:            # downscale for a cheap encode
                        img = img.resize((int(img.width * out_h / img.height), out_h))
                    buf = io.BytesIO(); img.save(buf, format="JPEG", quality=q)
                    jpg = buf.getvalue()
                    await resp.write(b"--%s\r\nContent-Type: image/jpeg\r\n"
                                     b"Content-Length: %d\r\n\r\n" % (boundary.encode(), len(jpg))
                                     + jpg + b"\r\n")
                await asyncio.sleep(max(0.0, period - (time.monotonic() - t0)))
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        return resp

    async def audio(req):
        return web.json_response(svc.audio_status())

    async def avsync(req):
        return web.json_response(svc.avsync.status() if svc.avsync else {"enabled": False})

    webrtc_sink = WebRTCSink(svc)

    async def webrtc_page(req):
        return web.Response(text=_WEBRTC_PAGE, content_type="text/html")

    async def webrtc_offer(req):
        try:
            body = await req.json()
            ans = await webrtc_sink.answer(body["sdp"])
            return web.json_response({"type": "answer", "sdp": ans})
        except Exception as e:
            import traceback; traceback.print_exc()
            return web.json_response({"error": str(e)}, status=500)

    # ── HLS: lossless HTTP delivery (no RTP/UDP mosaic), iPhone-native ───────
    # NOT /dev/shm — the systemd sandbox (PrivateDevices) gives this service a
    # private, unwritable /dev/shm.  Use a dir next to the script (writable by
    # the chalos service user); segments are small + short-lived (delete_segments).
    # --hls-dir makes this per-instance: the dir is WIPED at startup, so two
    # services sharing one (a second camera on the same host) would delete each
    # other's segments and both live views would stall.
    HLS_DIR = svc.hls_dir or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "hls_live")
    svc.hls_dir = HLS_DIR              # resolved dir; grab_snapshot_jpeg reads it
    os.makedirs(HLS_DIR, exist_ok=True)
    for _f in os.listdir(HLS_DIR):
        try: os.remove(os.path.join(HLS_DIR, _f))
        except OSError: pass
    # HLS via ffmpeg reading a lossless loopback-TCP H.264 branch off the tee.
    # (GStreamer hlssink2 asserts on the raw appsrc segment format; ffmpeg -c
    # copy just repackages the elementary H.264 into MPEG-TS segments — no
    # decode, so it keeps the RAW stream the browser decodes leniently, i.e.
    # /live quality — and HTTP/TCP delivery means none of WebRTC's UDP mosaic.)
    import subprocess as _sp
    # Per-instance: this loopback port carries the re-encoded H.264 to the HLS
    # ffmpeg.  It is a FIXED bind, so a second camera service on the same host
    # cannot bind it and its HLS silently produces zero segments while every
    # other branch (snapshot, detection) keeps working — which is exactly how
    # it failed: frames flowed, /hls 404'd.
    _HLS_PORT = svc.hls_port
    # Re-encode is REQUIRED (NN_HLS_REENCODE=1, default): the raw P4 H.264 is a
    # non-conformant, chunked byte-stream with no clean AU/keyframe boundaries
    # (the TCP branch even omits h264parse because it stalls on it), so ffmpeg
    # -c copy reads it but produces ZERO segments.  The WAVE5 transcode
    # normalises + re-delimits it into segmentable conformant H.264.  Only a
    # device-side H.264 conformance fix would allow raw (NN_HLS_REENCODE=0) HLS.
    _hls_reencode = str(svc.opt("NN_HLS_REENCODE", "1")) == "1"
    _RAW_PORT = 5567
    _TC_BIN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wave5_transcode_stream")
    # NN_HLS_ZEROCOPY: re-encode via the standalone WAVE5 V4L2 m2m transcoder
    # (dec CAPTURE dmabuf -> enc OUTPUT DMABUF import, NO videoconvert) instead of
    # the GStreamer videoconvert branch.  The videoconvert CPU-maps every 720p
    # frame from an uncached DMABuf → ~6 fps → the re-encode queue drops ~65% of
    # frames; the zero-copy path runs ~46x faster (269 fps) so it never drops.
    _hls_zerocopy = str(svc.opt("NN_HLS_ZEROCOPY", "0")) == "1" and os.path.exists(_TC_BIN)

    def _ffmpeg_hls_cmd(input_args, wallclock=True):
        seg = os.path.join(HLS_DIR, "seg%05d.ts"); m3u8 = os.path.join(HLS_DIR, "live.m3u8")
        return (["ffmpeg", "-hide_banner", "-loglevel", "error", "-fflags", "+genpts",
                 # Bounded probe: at night the fixed-QP re-encode collapses to a
                 # few KB/s and ffmpeg's default probe (MBs) starves for minutes
                 # before the FIRST segment — the player just buffers.  64 KB is
                 # plenty to find SPS/PPS + an IDR on this stream.
                 "-probesize", "65536", "-analyzeduration", "1000000",
                 # Stamp packets with the REAL clock as they arrive.  ffmpeg
                 # anchors EXT-X-PROGRAM-DATE-TIME when it starts and then
                 # advances it by MEDIA time, so every stall (a camera
                 # reconnect, a codec hiccup) burns wall time that the date
                 # never regains — it had drifted 10 min behind on cam0 and
                 # 31 min on cam3, and the live view shows that date as the
                 # capture time.  Wall-clock stamps keep it honest no matter
                 # how long the process runs.
                 ]
                + (["-use_wallclock_as_timestamps", "1"] if wallclock else [])
                + input_args +
                # 1 s segments with a 6-slot window meant a segment lived ~7 s:
                # an iPhone even slightly behind requested one that was already
                # deleted, and iOS reports that fatal fetch as MEDIA_ERR_
                # SRC_NOT_SUPPORTED(4) — the intermittent "HLS error" users saw
                # on a healthy stream.  2 s segments + a 10-slot window + a
                # delete threshold keep ~30 s of history on disk (Apple's own
                # guidance is ~6 s targets; this stays low-latency while giving
                # a lagging client real slack).  Low-latency viewing is the
                # jMuxer/MSE path anyway — HLS is the compatibility path.
                ["-c:v", "copy", "-f", "hls", "-hls_time", _hls_segtime,
                 "-hls_list_size", "10",
                 # keep ~30 s on disk however short segments get: deleting
                 # them sooner is what made iOS report a fatal
                 # MEDIA_ERR_SRC_NOT_SUPPORTED on an otherwise healthy stream
                 "-hls_delete_threshold", "20",
                 "-hls_flags", "delete_segments+omit_endlist+independent_segments+program_date_time",
                 "-hls_segment_type", "mpegts", "-hls_segment_filename", seg, m3u8])

    # NN_HLS_MODE=gst (default): segment INSIDE the GStreamer pipeline with
    # hlssink2.  The tcp->ffmpeg path throws the buffers' timestamps away at
    # the raw-h264 boundary, so ffmpeg stamps 25 fps regardless of the actual
    # source rate — and the camera legitimately varies fps with light (OV5647
    # AE stretches exposure: measured 7 fps at night) and network conditions.
    # hlssink2 muxes the REAL PTS into the TS segments, so players pace
    # correctly at any source rate.  NN_HLS_MODE=ffmpeg restores the old path.
    # HLS latency is governed by the KEYFRAME interval, not by hls_time: a
    # segment can only start on a keyframe, so gop=15 at 5.8 fps produced
    # 2.6 s segments and a player (which buffers ~3) sat ~10 s behind.
    # Shorter GOP = lower latency at the cost of more keyframes (bitrate).
    _hls_gop = int(svc.opt("NN_HLS_GOP", "8"))
    _hls_segtime = str(svc.opt("NN_HLS_SEGTIME", "1"))
    _hls_mode = str(svc.opt("NN_HLS_MODE", "ts"))
    if _hls_mode == "ts":
        # In-pipeline mpegtsmux (real PTS survive the TCP hop) + ffmpeg doing
        # only time-based rotation of an already-timed TS stream.  hlssink2
        # ("gst" mode) died in splitmuxsink segment-format assertions right at
        # its max-files rotation on gst 1.22 — ffmpeg's rotation ran for days.
        # Prefer the shared transcode service when a re-encode is needed: it
        # owns the codec, so a wedged encoder is recovered by rebuilding one
        # session instead of restarting this camera.  The camera then feeds
        # it RAW H.264 and ffmpeg reads the service's port.  If the service
        # is not running we transcode in-process exactly as before, so HLS
        # never depends on it.
        # Prefer the shared transcode service when a re-encode is needed: it
        # owns the codec, so a wedged encoder is rebuilt as one session
        # instead of restarting this camera.  Access units reach it through
        # shared memory (see transcoded_client) — no second copy of the
        # stream, and no extra GStreamer branch on this tee at all.  If the
        # service is not running we transcode in-process exactly as before,
        # so HLS never depends on it.
        # Opt-in PER CAMERA (NN_TRANSCODED=1).  Without this gate any camera
        # that happens to restart adopts the shared service just because the
        # file on disk has the wiring — which is exactly how cam0 joined
        # unnoticed after a crash.  Moving a camera onto it must be a
        # decision, not an accident.
        _tc_port = None
        if _hls_reencode and str(svc.opt("NN_TRANSCODED", "")) == "1":
            try:
                from transcoded_client import try_open as _tc_try
                _w, _tc_port = _tc_try(getattr(svc, "cam_id", "") or os.environ.get("NN_CAM_ID", "camera"),
                                       8_000_000, _hls_gop, svc.enc_qp or 0, True)
                if _tc_port:
                    svc._tc_writer = _w        # feed() starts pushing now
                    svc._tc_port = _tc_port
            except Exception as _e:            # noqa: BLE001 - optional
                print(f">> nn-transcoded unavailable ({_e}) — transcoding "
                      f"in-process", flush=True)
        if _tc_port:
            _hls_src_port = _tc_port
        else:
            svc.add_tcp_branch(_HLS_PORT, reencode=_hls_reencode, mux_ts=True,
                               bitrate=8_000_000, gop=_hls_gop)
            _hls_src_port = _HLS_PORT
        def _pdt_skew():
            """Seconds the playlist's EXT-X-PROGRAM-DATE-TIME lags wall clock,
            or None if it can't be read."""
            import datetime as _dt, re as _re
            try:
                with open(os.path.join(HLS_DIR, "live.m3u8")) as _f:
                    m = None
                    for _ln in _f:
                        if _ln.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
                            m = _ln.split(":", 1)[1].strip()
                    if not m:
                        return None
                stamp = _dt.datetime.fromisoformat(m)
                now = _dt.datetime.now(stamp.tzinfo)
                return (now - stamp).total_seconds()
            except Exception:
                return None

        def _hls_ts():
            # ffmpeg anchors PROGRAM-DATE-TIME once and then advances it by
            # MEDIA time, so every stall (camera reconnect, codec hiccup,
            # service restart with no video) permanently pushes the playlist
            # date behind wall clock — measured 11m31s on cam3, and the live
            # view shows that date as the capture time.
            # -use_wallclock_as_timestamps fixes it, but it cannot be used on
            # the nn-transcoded path (its connect-time burst would stamp the
            # date into the FUTURE).  So: watch the skew and re-anchor by
            # restarting the ffmpeg leg when it drifts too far.  One ~2 s
            # segment gap, only when actually needed.
            PDT_MAX_SKEW = float(svc.opt("NN_HLS_PDT_MAX_SKEW", "45"))
            while not svc.closed:
                os.makedirs(HLS_DIR, exist_ok=True)
                proc = None
                try:
                    proc = svc._track(_sp.Popen(_ffmpeg_hls_cmd(
                        ["-f", "mpegts", "-i", f"tcp://127.0.0.1:{_hls_src_port}"],
                        # nn-transcoded flushes a buffered burst the moment
                        # ffmpeg connects; stamping those packets "now" puts
                        # the playlist date ~10 s in the FUTURE.  Its mpegtsmux
                        # timestamps are already sound in the small, but on
                        # that path media time runs ~1.3 s/min slower than wall
                        # clock, so the playlist date slides ~78 s/hour behind
                        # and the watchdog below can only bound it, not stop it
                        # (measured on cam1, 2026-08-17).  NN_HLS_WALLCLOCK=1
                        # forces wall-clock stamping there too: a bounded
                        # one-off offset beats unbounded accumulation.
                        wallclock=(_tc_port is None or
                                   str(svc.opt("NN_HLS_WALLCLOCK", "")) == "1"))))
                    while not svc.closed:
                        try:
                            _rc = proc.wait(timeout=30)
                            # WHY it exited matters: a silent respawn loop
                            # resets the playlist to seq 0 every couple of
                            # seconds, so the player never accumulates the
                            # ~3 segments it needs and the UI shows nothing.
                            print(f">> HLS: ffmpeg exited rc={_rc} — "
                                  f"restarting in 2s", flush=True)
                            break                      # ffmpeg exited on its own
                        except _sp.TimeoutExpired:
                            pass
                        skew = _pdt_skew()
                        if skew is not None and skew > PDT_MAX_SKEW:
                            print(f">> HLS: playlist date {skew:.0f}s behind wall "
                                  f"clock — re-anchoring", flush=True)
                            proc.terminate()
                            try: proc.wait(timeout=10)
                            except Exception: proc.kill()
                            break
                    if svc.closed and proc.poll() is None:
                        proc.terminate()
                except Exception as e:
                    print(">> HLS ts error:", e, flush=True)
                    if proc is not None:
                        try: proc.kill()
                        except Exception: pass
                time.sleep(2)
        threading.Thread(target=_hls_ts, daemon=True, name=f"hls-ts:{svc.cam_id}").start()
        print(">> HLS: in-pipeline mpegtsmux → ffmpeg rotation (real PTS, variable-fps safe)"
              " → %s/live.m3u8" % HLS_DIR, flush=True)
    elif _hls_mode == "gst":
        os.makedirs(HLS_DIR, exist_ok=True)
        svc.add_hls_branch(HLS_DIR, reencode=_hls_reencode, bitrate=8_000_000, gop=_hls_gop)
        print(">> HLS: in-pipeline hlssink2 (real PTS, variable-fps safe) → %s/live.m3u8"
              % HLS_DIR, flush=True)
    elif _hls_zerocopy:
        # Feed the RAW P4 H.264 straight to the transcoder's stdin (the same bytes
        # that drive /ws); it re-encodes zero-copy (dec dmabuf -> enc, no
        # videoconvert) and ffmpeg segments its conformant stdout.  A bounded
        # queue decouples the ingest thread from any ffmpeg/disk hiccup.
        import queue as _q
        svc._hls_q = _q.Queue(maxsize=800)
        def _hls_zc():
            while not svc.closed:
                tc = ff = None
                try:
                    os.makedirs(HLS_DIR, exist_ok=True)
                    tc = svc._track(_sp.Popen([_TC_BIN, "8000000", "15"], stdin=_sp.PIPE, stdout=_sp.PIPE))
                    ff = svc._track(_sp.Popen(_ffmpeg_hls_cmd(["-f", "h264", "-i", "pipe:0"]), stdin=tc.stdout))
                    tc.stdout.close()          # ffmpeg owns the read end
                    while ff.poll() is None:
                        try:
                            data = svc._hls_q.get(timeout=0.5)
                        except _q.Empty:
                            continue
                        try:
                            tc.stdin.write(data)
                        except Exception:
                            break              # transcoder died → respawn
                except Exception as e:
                    print(">> HLS zerocopy error:", e, flush=True)
                finally:
                    for pr in (ff, tc):
                        try:
                            if pr: pr.terminate()
                        except Exception: pass
                time.sleep(2)
        threading.Thread(target=_hls_zc, daemon=True, name=f"hls-zc:{svc.cam_id}").start()
        print(">> HLS: ZERO-COPY WAVE5 transcode (feed → %s → ffmpeg) → %s/live.m3u8"
              % (_TC_BIN, HLS_DIR), flush=True)
    else:
        # 8 Mbit/s GStreamer WAVE5 re-encode (videoconvert path); gop=15 so any
        # dropped-reference smear self-heals within 0.5 s (see notes above).
        svc.add_tcp_branch(_HLS_PORT, reencode=_hls_reencode, bitrate=8_000_000, gop=_hls_gop)
        def _hls_ffmpeg():
            while not svc.closed:
                os.makedirs(HLS_DIR, exist_ok=True)
                try:
                    svc._track(_sp.Popen(_ffmpeg_hls_cmd(["-f", "h264", "-i", f"tcp://127.0.0.1:{_HLS_PORT}"]))).wait()
                except Exception as e:
                    print(">> HLS ffmpeg error:", e, flush=True)
                time.sleep(2)
        threading.Thread(target=_hls_ffmpeg, daemon=True, name=f"hls-ffmpeg:{svc.cam_id}").start()
        print(">> HLS: ffmpeg -c copy tcp://127.0.0.1:%d → %s/live.m3u8" % (_HLS_PORT, HLS_DIR), flush=True)

    # ── HLS liveness watchdog ────────────────────────────────────────────────
    # The v4l2 branches on the CIX P1 can stall with NO bus error and no
    # actionable ffmpeg error (see the APPSRC_CAPS note near the top of this
    # file): the device stays open and simply produces nothing.  Ingest keeps
    # decrypting records the whole time, so every counter looks healthy while
    # the playlist is frozen.  Observed after a camera re-provision on
    # 2026-08-21: cam0 ingested for 12 minutes with zero new segments, ffmpeg
    # respawning every 32 s on "could not find codec parameters", and only a
    # service restart brought it back.
    #
    # So compare the two things that must move together: AUs pushed into the
    # pipeline (svc._fed) and segment files coming out.  Video in but no video
    # out for NN_HLS_WATCHDOG_S means the pipeline is wedged in a state we
    # cannot clear from inside GStreamer — exit non-zero and let systemd's
    # Restart=on-failure rebuild it, the same recovery cam3's control path uses.
    def _hls_watchdog():
        grace = float(svc.opt("NN_HLS_WATCHDOG_S", "120"))
        if grace <= 0:
            return
        last_fed = -1
        last_out = time.time()          # last time a segment appeared
        last_seg = None
        while not svc.closed:
            time.sleep(15)
            try:
                # newest by MODIFICATION TIME, not by name: a re-anchored or
                # respawned ffmpeg restarts its numbering at zero, so by name
                # the old high-numbered segments stayed "newest" for the 120 s
                # it takes them to be deleted and the watchdog reset a graph
                # that was writing segments the whole time (cam0, 2026-09-22 08:08)
                newest = None
                for f in os.listdir(HLS_DIR):
                    if f.endswith(".ts"):
                        try:
                            newest = max(newest or 0, os.path.getmtime(os.path.join(HLS_DIR, f)))
                        except OSError:
                            pass                        # rotated away mid-listing
            except Exception:
                newest = None
            fed = getattr(svc, "_fed", 0)
            if newest != last_seg:
                last_seg, last_out = newest, time.time()
            if fed == last_fed:
                # No video arriving either — the camera is simply offline.
                # That is not a pipeline fault; do not restart over it.
                last_out = time.time()
            last_fed = fed
            stalled = time.time() - last_out
            if stalled > grace:
                cb = getattr(svc, "on_wedged", None)
                if cb is not None:
                    # nnvideo: reset THIS camera's graph, keep the process
                    print(f">> HLS WATCHDOG: {stalled:.0f}s of video ingest with no new "
                          f"segment (fed={fed}) — pipeline wedged, resetting camera", flush=True)
                    try:
                        cb(stalled)
                    except Exception as _e:
                        print(f">> on_wedged failed: {_e}", flush=True)
                    last_out = time.time()
                    continue
                print(f">> HLS WATCHDOG: {stalled:.0f}s of video ingest with no new "
                      f"segment (fed={fed}) — pipeline wedged, exiting for restart",
                      flush=True)
                os._exit(42)            # systemd Restart=on-failure
    threading.Thread(target=_hls_watchdog, daemon=True, name=f"hls-watchdog:{svc.cam_id}").start()

    _HLS_PAGE ="""<!doctype html><meta charset=utf-8><title>nn-camera HLS</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<style>html,body{margin:0;height:100%;background:#000}video{width:100vw;height:100vh;object-fit:contain}</style>
<video id=v autoplay playsinline muted controls></video>
<script src="https://cdn.jsdelivr.net/npm/hls.js@1/dist/hls.min.js"></script>
<div id=nnb style="position:fixed;bottom:10px;right:12px;z-index:9;color:#ffd479;font:12px/1 ui-monospace,monospace;background:rgba(0,0,0,.45);padding:4px 7px;border-radius:6px"></div>
<script>
// yolo timing + capture timestamp badge (shared across /live, /hls, /webrtc).
// yolo_ms is the event engine's LAST inference — it stays 0 until motion has
// triggered YOLO at least once (a black static scene never does).
window.__nnts = 0;                                  // ws text frames override (per-frame accurate)
function nnFmtTs(ms){
  if(!ms) return "";
  if(ms < 1e12){ var t=Math.floor(ms/1000);        // unsynced device -> uptime
    return "up "+Math.floor(t/3600)+":"+String(Math.floor(t/60)%60).padStart(2,"0")+":"+String(t%60).padStart(2,"0"); }
  var d=new Date(ms);
  return d.toLocaleTimeString(undefined,{hour12:false})+"."+String(d.getMilliseconds()).padStart(3,"0");
}
setInterval(async function(){
  var y=0, ts=window.__nnts;
  try{ var j=await (await fetch("/api/event/live")).json(); y=j.yolo_ms||0; }catch(e){}
  if(!ts){ try{ var m=await (await fetch("/motion")).json(); ts=m.ts||0; }catch(e){} }
  document.getElementById("nnb").textContent="yolo="+y+"ms"+(ts?"  "+nnFmtTs(ts):"");
}, 1000);
</script>
<script>
var v=document.getElementById('v'), url='/hls/live.m3u8';
if (v.canPlayType('application/vnd.apple.mpegurl')) {       // Safari / iOS: native HLS
  v.src=url; v.play().catch(function(){});
} else if (window.Hls && Hls.isSupported()) {               // Chrome/Firefox: hls.js
  var h=new Hls({lowLatencyMode:true, liveSyncDurationCount:2,
                 liveMaxLatencyDurationCount:6, maxLiveSyncPlaybackRate:1.5,
                 backBufferLength:4});
  h.loadSource(url); h.attachMedia(v);
} else { document.body.innerHTML='<p style=color:#fff>HLS not supported in this browser</p>'; }
</script>"""

    async def hls_page(req):
        return web.Response(text=_HLS_PAGE, content_type="text/html")

    async def hls_file(req):
        name = req.match_info["name"]
        if "/" in name or ".." in name:
            return web.Response(status=400)
        path = os.path.join(HLS_DIR, name)
        if not os.path.isfile(path):
            return web.Response(status=404, text="not ready")
        ctype = ("application/vnd.apple.mpegurl" if name.endswith(".m3u8")
                 else "video/mp2t" if name.endswith(".ts")
                 else "application/octet-stream")
        return web.FileResponse(path, headers={"Content-Type": ctype})

    app = web.Application(middlewares=[_security_headers_mw])
    app.add_routes([
        web.get("/status", status),
        web.get("/health", health),
        web.get("/hls", hls_page),
        web.get("/hls/{name}", hls_file),
        web.post("/branch", add_branch),
        web.delete("/branch/{id}", del_branch),
        web.get("/webrtc", webrtc_page),
        web.post("/webrtc/offer", webrtc_offer),
        web.get("/motion", motion),
        web.get("/snapshot", snapshot),
        web.get("/snapshot.jpg", snapshot_jpg),
        web.get("/api/snapshot/config", snapshot_config),
        web.post("/api/snapshot/config", snapshot_config),
        web.get("/pipeline/stats", pipeline_stats),
        web.get("/mjpeg", mjpeg),
        web.get("/live", live_page),
        web.get("/api/camera/provinfo", cam_provinfo),
        web.get("/api/camera/uptime", cam_uptime),
        web.post("/api/camera/clear", cam_clear),
        web.post("/api/camera/reboot", cam_reboot),
        web.get("/api/camera/settings", cam_settings),
        web.get("/api/event/status", event_status),
        web.get("/api/event/live", event_live),
        web.get("/api/device/status", device_status),
        web.get("/api/infer/mode", infer_mode_get),
        web.post("/api/infer/mode", infer_mode_set),
        web.post("/api/event/config", event_config),
        web.post("/api/event/test", event_test),
        web.get("/api/event/ring.bin", event_ring_bin),
        web.post("/api/camera/set", cam_set),
        web.get("/ws", ws_h264),
        web.get("/jmuxer.min.js", jmuxer_js),
        web.get("/audio", audio),
        web.get("/avsync", avsync),
        # ── ISP diagnostics ──
        web.get("/diag/isp", diag_isp),      # firmware WS: raw frame + settings
        web.get("/diag", diag_page),         # webapp: list captures
        web.get("/diag/list", diag_list),    # JSON index
        web.get("/diag/img/{name}", diag_img),
    ])
    return app


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream-port", type=int, default=8888, help="nn_sectun ingest from the C6")
    ap.add_argument("--control-port", type=int, default=8899, help="HTTP control API")
    ap.add_argument("--keydir", default="/tmp/ble_hub_data")
    ap.add_argument("--test-file", default=None, help="loop a raw .h264 instead of the C6")
    ap.add_argument("--motion-interval-ms", type=int, default=200,
                    help="motion-detection cadence (0 = disable the detection branch)")
    ap.add_argument("--motion-thresh", type=int, default=18, help="luma diff threshold")
    ap.add_argument("--motion-decoder", choices=("auto", "v4l2", "pyav"), default="auto",
                    help="detection-branch decoder: v4l2 = in-pipeline HW decode "
                         "(v4l2h264dec, e.g. WAVE5), pyav = software; auto picks v4l2 if present")
    ap.add_argument("--audio", action="store_true", default=True,
                    help="demux + decode AAC audio records (GET /audio)")
    ap.add_argument("--no-audio", dest="audio", action="store_false")
    ap.add_argument("--audio-file", default=None, help="also tee raw ADTS to this .aac file")
    ap.add_argument("--adapt", action="store_true", default=True,
                    help="server-driven adaptive bitrate/GOP over the sectun back-channel")
    ap.add_argument("--no-adapt", dest="adapt", action="store_false")
    ap.add_argument("--adapt-fps", type=int, default=30, help="device capture fps (for GOP = fps*2^N)")
    ap.add_argument("--avsync-window-ms", type=int, default=200,
                    help="A/V-sync reorder window (host orders records by device ts)")
    ap.add_argument("--snapshot-interval", type=int, default=30,
                    help="seconds between periodic saved snapshots (0 = disable); "
                         "runtime-adjustable via POST /api/snapshot/config")
    ap.add_argument("--hls-port", type=int, default=5566,
                    help="loopback TCP port feeding the HLS ffmpeg; MUST be unique "
                         "per instance (a second camera on this host needs its own)")
    ap.add_argument("--hls-dir", default=None,
                    help="HLS segment dir (default: ./hls_live next to this script). "
                         "MUST be unique per instance — it is wiped at startup")
    ap.add_argument("--snapshot-dir", default=None,
                    help="directory for the periodic latest.jpg snapshot "
                         "(default: ./snapshots next to this script)")
    # ── motion/AI event recording ──
    ap.add_argument("--hub-url", default="http://127.0.0.1:8769",
                    help="hub base URL for event media upload links")
    ap.add_argument("--event-motion-thresh", type=float, default=0.02,
                    help="T: fraction of changed pixels that arms YOLO (runtime-adjustable)")
    ap.add_argument("--event-keep-s", type=int, default=60, help="L: A/V ring buffer seconds")
    ap.add_argument("--event-max-s", type=int, default=120, help="V: max event length seconds")
    ap.add_argument("--event-quiet-s", type=float, default=3.0,
                    help="end event after this long without detection/motion")
    ap.add_argument("--yolo-model", default="/home/chalos/models/yolox_s.onnx",
                    help="YOLOX onnx path ('' = motion-only events, no AI gate)")
    ap.add_argument("--enc-qp", type=int, default=0,
                    help="fixed QP (1-51) for the HW re-encode branches; 0=off. "
                         "Needed on CIX P1, whose encoder ignores video_bitrate")
    ap.add_argument("--yolo-url", default="http://127.0.0.1:8901",
                    help="NPU inference server URL (preferred; '' = CPU only)")
    ap.add_argument("--no-events", dest="events", action="store_false", default=True)
    args = ap.parse_args()

    svc = VideoService()
    svc.enc_qp = args.enc_qp
    svc.hls_dir = args.hls_dir         # None = default beside this script
    svc.hls_port = args.hls_port       # must differ per instance on one host
    svc.avsync_window_ms = args.avsync_window_ms
    svc.adapt_enabled = args.adapt
    svc.adapt_fps = args.adapt_fps
    svc.snapshot_interval = args.snapshot_interval
    if args.snapshot_dir:
        svc.snapshot_dir = args.snapshot_dir
    svc.start()

    if args.snapshot_interval > 0:
        threading.Thread(target=svc._snapshot_saver, daemon=True, name="snapshots").start()
        print(f">> snapshot saver: every {args.snapshot_interval}s (clean IDR) "
              f"→ {svc.snapshot_dir}/latest.jpg", file=sys.stderr, flush=True)

    if args.audio:
        try:
            svc.audio = AudioSink(args.audio_file)
            print(">> audio demux on (AAC records → /audio)")
        except Exception as e:
            print(f">> audio demux disabled ({e})")

    if args.motion_interval_ms > 0:
        det = MotionDetector(interval_ms=args.motion_interval_ms, diff_thresh=args.motion_thresh)
        svc.add_detection_branch(det, decoder=args.motion_decoder)
        # tee → decode (HW v4l2 / PyAV) → motion → boxes (GET /motion)

        if args.events:
            import event_engine as event_engine_mod
            detector = None
            # Prefer the shared daemon: one model instance for every camera
            # instead of one per process (and it schedules fairly across
            # them).  Falls back to a private detector if it is not running,
            # so a camera never loses detection because of a missing service.
            if detector is None and os.environ.get("NN_INFERD_SOCK",
                                                   "/run/nn-inferd.sock"):
                try:
                    from inferd_client import ReconnectingInferd
                    detector = ReconnectingInferd(owner=os.environ.get("NN_CAM_ID"))
                    print(f">> detector: nn-inferd {detector.devices}",
                          file=sys.stderr, flush=True)
                except Exception as e:
                    print(f">> nn-inferd unavailable ({e}) — local detector",
                          file=sys.stderr, flush=True)

            if detector is None and args.yolo_url:
                try:
                    from yolox_detector import NpuDetector
                    detector = NpuDetector(args.yolo_url)
                    print(f">> detector: NPU at {args.yolo_url}",
                          file=sys.stderr, flush=True)
                except Exception as e:
                    print(f">> NPU unavailable ({e}) — trying CPU",
                          file=sys.stderr, flush=True)
            if detector is None and args.yolo_model:
                try:
                    # A .cix model is NPU-compiled (CIX P1 / Zhouyi AIPU) and must
                    # go through NOE_Engine; .onnx runs on the CPU via onnxruntime.
                    if args.yolo_model.endswith(".cix"):
                        from yolox_detector import YoloxNpuDetector
                        detector = YoloxNpuDetector(args.yolo_model)
                        print(">> detector: NPU (CIX AIPU, local NOE_Engine)",
                              file=sys.stderr, flush=True)
                    else:
                        from yolox_detector import YoloxDetector
                        detector = YoloxDetector(args.yolo_model)
                        print(">> detector: CPU onnxruntime", file=sys.stderr, flush=True)
                except Exception as e:
                    print(f">> yolox unavailable ({e}) — motion-only events",
                          file=sys.stderr, flush=True)
            eng = event_engine_mod.EventEngine(
                hub_url=args.hub_url, detector=detector,
                keep_s=args.event_keep_s, max_s=args.event_max_s,
                motion_thresh=args.event_motion_thresh,
                quiet_s=args.event_quiet_s)
            svc.event_engine = eng
            # The SERVICE engine has capabilities too: every detector variant
            # we ship (onnx CPU, .cix NPU, remote NPU server) is COCO-trained,
            # so advertise the label set rather than 80 strings.  Without this
            # a camera with no EDGE inference (the ESP32 cameras) offered an
            # empty class list and its policy could not be configured.
            svc.service_caps = ({
                "model": os.path.basename(args.yolo_model or args.yolo_url or "yolox"),
                "labels": "coco80", "classes": 80, "max_agg": 30,
            } if detector is not None else None)
            svc.apply_infer_mode()

            def _tick(ts_ms, ratio, boxes, frame):
                # engine decides if/when it needs pixels; hand it the frame
                if (eng._active is None and ratio >= eng.motion_thresh) or (eng._active is not None):
                    eng.motion_tick(ts_ms, ratio, boxes, frame)
                else:
                    frame.release()
                    eng.motion_tick(ts_ms, ratio, boxes, None)
            det.on_tick = _tick
            print(f">> event engine on: T={args.event_motion_thresh} "
                  f"L={args.event_keep_s}s V={args.event_max_s}s "
                  f"yolo={'on' if detector else 'OFF'} hub={args.hub_url}",
                  file=sys.stderr, flush=True)

    svc.load_infer_mode(args.keydir)
    # handlers run in another scope; keep what they need
    svc.keydir = args.keydir
    svc.stream_port = args.stream_port
    svc.apply_infer_mode()

    if args.test_file:
        threading.Thread(target=ingest_file, args=(svc, args.test_file), daemon=True, name="ingest").start()
    else:
        threading.Thread(target=ingest_c6, args=(svc, args.stream_port, args.keydir), daemon=True, name="ingest").start()

    # ── hub self-registration ────────────────────────────────────────────────
    # The hub keeps a dynamic camera list (POST /api/v1/cameras); registering
    # here means adding a camera needs no hub restart or NN_CAMERAS edit.
    # NN_CAM_URL overrides the advertised base URL (defaults to this host's
    # control port as seen from the hub).
    def _register_loop():
        import socket as _sock
        hub = (args.hub_url or "").rstrip("/")
        if not hub:
            return
        cam_id = os.environ.get("NN_CAM_ID") or "cam0"
        name = os.environ.get("NN_CAM_NAME") or cam_id
        url = os.environ.get("NN_CAM_URL")
        if not url:
            host = _sock.gethostbyname(_sock.gethostname()) \
                if "127.0.0.1" not in hub else "127.0.0.1"
            url = f"http://{host}:{args.control_port}"
        while True:
            try:
                caps = {}
                if svc.edge_caps:
                    caps["infer"] = svc.edge_caps
                # A camera that HAS an edge engine but whose engine failed to
                # start reports why.  Forward it: without this the hub cannot
                # tell "no edge engine on this hardware" from "the edge engine
                # is broken", and the webapp silently drops the Edge section —
                # which reads as the former.  cam3's C7x sat wedged for a day
                # looking like a camera that never had inference (2026-08-19).
                _err = (getattr(svc, "cam_settings", None) or {}).get("infer_error")
                if _err and not svc.edge_caps:
                    caps["infer_error"] = str(_err)[:200]
                # The SERVICE engine has capabilities too — without this a
                # camera with no edge inference (the ESP32 cameras) offered
                # an empty class list, so its service policy could not be
                # configured at all.
                if getattr(svc, "service_caps", None):
                    caps["service_infer"] = svc.service_caps
                body = json.dumps({"id": cam_id, "name": name, "url": url,
                                   "caps": caps}).encode()
                req = _urlreq.Request(f"{hub}/api/v1/cameras", data=body,
                                      headers={"Content-Type": "application/json"},
                                      method="POST")
                _urlreq.urlopen(req, timeout=5).read()
            except Exception as e:
                print(f">> hub register failed: {e}", file=sys.stderr, flush=True)

            # Pull the inference policy on the same beat.  Only re-apply when
            # the version moved: rebuilding evaluators throws away the
            # aggregation windows, which would reset a detection in progress.
            try:
                r = _urlreq.urlopen(
                    f"{hub}/api/v1/cameras/{cam_id}/inference", timeout=5).read()
                doc = json.loads(r.decode()).get("policy") or {}
                ver = int(doc.get("version") or 0)
                if ver and ver != getattr(svc, "policy_version", 0):
                    svc.policy_doc = doc
                    svc.policy_version = ver
                    svc.apply_policy()
                    print(f">> inference policy v{ver} applied", flush=True)
            except Exception as e:
                print(f">> policy fetch failed: {e}", file=sys.stderr, flush=True)
            time.sleep(30)

    threading.Thread(target=_register_loop, daemon=True, name="hub-register").start()

    print(f">> control API on http://0.0.0.0:{args.control_port}")
    web.run_app(make_app(svc), host="0.0.0.0", port=args.control_port, print=None)


if __name__ == "__main__":
    main()
