"""
Hub video gateway — opens a real-time channel to the video streaming service
and re-publishes the live camera video to end users over WebRTC.

  C6 ─enc H.264─▶ video_service (GStreamer) ──RTP/UDP──▶ [hub: this module]
                                                              └─ aiortc ─▶ browser (WebRTC)

Flow:
  1. POST /api/v1/cameras/{cam}/stream  {protocol:"webrtc"}
       → the hub picks a local UDP port, tells the video streaming service to
         append an RTP/UDP branch aimed at that port (the "append a GST element"
         step), and starts ingesting the RTP/H.264 with aiortc (PyAV).
       → returns {sid, offer_url, ...}.
  2. Browser opens GET /webrtc/{sid}, creates a recvonly offer, and POSTs it to
       POST /api/v1/cameras/{cam}/stream/{sid}/offer
       → the hub answers; WebRTC media (H.264, passthrough — no transcode) flows.
  3. DELETE /api/v1/cameras/{cam}/stream/{sid}  → tears down the PC + branch.

WebRTC is done entirely on the hub (aiortc), NOT in GStreamer; the service↔hub
hop is plain RTP/UDP.  Mount into the hub aiohttp app via setup_video_routes(),
or run standalone (python -m hub.video_gateway).
"""
from __future__ import annotations
import argparse
import asyncio
import contextlib
import logging
import socket
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import os
from aiohttp import web, ClientSession, ClientTimeout
from aiortc import RTCPeerConnection, RTCSessionDescription
from aiortc.contrib.media import MediaPlayer, MediaRelay

log = logging.getLogger("hub.video")

_STATIC_DIR = Path(__file__).resolve().parent / "static"
_html_cache: dict[str, str] = {}


def _load_html(name: str) -> str:
    """Read a page from hub/static/, cached after first read."""
    if name not in _html_cache:
        _html_cache[name] = (_STATIC_DIR / name).read_text(encoding="utf-8")
    return _html_cache[name]

# One SDP per session describing the RTP/H.264 stream we receive from the service.
_SDP_TMPL = """v=0
o=- 0 0 IN IP4 {ip}
s=nn-camera
c=IN IP4 {ip}
t=0 0
m=video {port} RTP/AVP {pt}
a=rtpmap:{pt} H264/90000
a=fmtp:{pt} packetization-mode=1
"""


@dataclass
class StreamSession:
    sid: str
    cam: str
    udp_port: int
    branch_id: int
    sdp_path: str
    player: MediaPlayer
    relay: MediaRelay
    pcs: set = field(default_factory=set)


class VideoGateway:
    def __init__(self, service_url: str, recv_ip: str = "127.0.0.1"):
        self.service_url = service_url.rstrip("/")
        self.recv_ip = recv_ip
        self.sessions: dict[str, StreamSession] = {}

    @staticmethod
    def _free_udp_port() -> int:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("", 0))
        port = s.getsockname()[1]
        s.close()
        return port

    async def open_stream(self, cam: str, pt: int = 96) -> StreamSession:
        port = self._free_udp_port()
        sdp = _SDP_TMPL.format(ip=self.recv_ip, port=port, pt=pt)
        f = tempfile.NamedTemporaryFile("w", suffix=".sdp", delete=False)
        f.write(sdp)
        f.close()

        # reencode=True: the service HW-transcodes on the WAVE5 so the RTP is
        # standard-conformant H.264 that ffmpeg/aiortc/browsers decode cleanly
        # (the ESP32-P4 encoder's native bitstream only decodes on lenient HW
        # decoders — ffmpeg fails mid-slice → green).  Both dec+enc in silicon.
        async with ClientSession() as cs:
            async with cs.post(f"{self.service_url}/branch",
                               json={"host": self.recv_ip, "port": port, "pt": pt,
                                     "reencode": True}) as r:
                if r.status != 200:
                    raise web.HTTPBadGateway(text=f"service /branch failed: {r.status}")
                branch = await r.json()
        log.info("opened service branch %s (reencode) → %s:%d", branch.get("id"), self.recv_ip, port)

        # aiortc PASSTHROUGH (decode=False): the camera's H.264 is forwarded
        # to the browser unchanged — zero server-side decode/encode.  The
        # answer() path forces H264 codec preferences (no VP8/transcode).
        #
        # ROOT CAUSE of prior keyframe corruption was NOT here — it's the
        # aiortc RECEIVER jitter buffer (fixed 128-packet ring, ~166 KB max
        # frame; 1080p IDRs are up to ~230 pkts → the ring overwrites the
        # frame's own packets → green macroblocks).  Fixed by enlarging that
        # buffer (see _patch_jitterbuffer at import time).
        player = MediaPlayer(
            f.name, format="sdp", decode=False,
            options={"protocol_whitelist": "file,udp,rtp", "fflags": "nobuffer",
                     "max_delay": "500000", "buffer_size": "8388608",
                     "reorder_queue_size": "4096"},
        )
        sid = uuid.uuid4().hex[:12]
        sess = StreamSession(sid=sid, cam=cam, udp_port=port, branch_id=branch.get("id", -1),
                             sdp_path=f.name, player=player, relay=MediaRelay())
        self.sessions[sid] = sess
        return sess

    async def answer(self, sid: str, offer: RTCSessionDescription) -> RTCSessionDescription:
        sess = self.sessions[sid]
        pc = RTCPeerConnection()
        sess.pcs.add(pc)

        @pc.on("connectionstatechange")
        async def _on_state():
            log.info("[%s] pc %s", sid, pc.connectionState)
            if pc.connectionState in ("failed", "closed"):
                sess.pcs.discard(pc)
                with contextlib.suppress(Exception):
                    await pc.close()

        # PASSTHROUGH: feed the player's encoded track DIRECTLY.  MediaRelay's
        # shallow per-subscriber queues silently drop packets under momentary
        # lag — harmless for decoded frames, fatal for encoded H.264 (every
        # drop truncates a GOP → "top slices ok, green blocks below").  One
        # viewer per session by design: /watch mints a session per visit.
        pc.addTrack(sess.player.video)

        # H.264 passthrough: force H264 codec preferences BEFORE processing the
        # remote offer — aiortc computes the codec intersection during
        # setRemoteDescription, so preferences set later are ignored.  No
        # transcode fallback — if the peer can't take H264, fail loudly.
        from aiortc.rtcrtpsender import RTCRtpSender
        h264 = [c for c in RTCRtpSender.getCapabilities("video").codecs
                if c.mimeType.lower() == "video/h264"]
        if not h264:
            raise web.HTTPBadGateway(text="peer/stack offers no H264 support "
                                          "(passthrough-only, no transcode)")
        for t in pc.getTransceivers():
            if t.kind == "video":
                t.setCodecPreferences(h264)

        await pc.setRemoteDescription(offer)
        answer = await pc.createAnswer()
        await pc.setLocalDescription(answer)
        return pc.localDescription

    async def close_stream(self, sid: str):
        sess = self.sessions.pop(sid, None)
        if not sess:
            return False
        for pc in list(sess.pcs):
            with contextlib.suppress(Exception):
                await pc.close()
        with contextlib.suppress(Exception):
            sess.player.video.stop()
        # Ask the service to drop the branch.
        with contextlib.suppress(Exception):
            async with ClientSession() as cs:
                await cs.delete(f"{self.service_url}/branch/{sess.branch_id}")
        if sess.sdp_path:
            with contextlib.suppress(Exception):
                Path(sess.sdp_path).unlink()
        return True


# ── HTTP routes (mountable into the hub app) ────────────────────────────────
def setup_video_routes(app: web.Application, gw: VideoGateway, prefix: str = "/api/v1"):
    async def open_stream(request):
        cam = request.match_info["cam"]
        body = {}
        with contextlib.suppress(Exception):
            body = await request.json()
        proto = (body or {}).get("protocol", "webrtc")
        if proto != "webrtc":
            return web.json_response({"error": f"unsupported protocol {proto!r}"}, status=400)
        sess = await gw.open_stream(cam)
        return web.json_response({
            "sid": sess.sid, "cam": cam, "protocol": "webrtc",
            "offer_url": f"{prefix}/cameras/{cam}/stream/{sess.sid}/offer",
            "view_url": f"/webrtc/{sess.sid}?cam={cam}",
            "ingest": {"transport": "rtp/udp", "port": sess.udp_port, "branch": sess.branch_id},
        })

    async def offer(request):
        sid = request.match_info["sid"]
        if sid not in gw.sessions:
            return web.json_response({"error": "no such stream"}, status=404)
        params = await request.json()
        ans = await gw.answer(sid, RTCSessionDescription(sdp=params["sdp"], type=params["type"]))
        return web.json_response({"sdp": ans.sdp, "type": ans.type})

    async def close_stream(request):
        ok = await gw.close_stream(request.match_info["sid"])
        return web.json_response({"closed": ok}, status=200 if ok else 404)

    async def view_page(request):
        sid = request.match_info["sid"]
        if sid not in gw.sessions:
            return web.Response(status=404, text="no such stream")
        # served verbatim; the page self-configures from /webrtc/<sid>?cam=<cam>
        return web.Response(content_type="text/html", text=_load_html("webrtc.html"))

    async def watch(request):
        """Human live-view entry point now folds into the unified /devices
        webapp (jmuxer/HLS), so there is one web UI.  The WebRTC stream API
        (POST .../stream → /webrtc/{sid}) stays available for programmatic /
        low-latency use; this no longer opens a session just to redirect
        (which used to leak one per visit)."""
        cam = request.match_info["cam"]
        raise web.HTTPFound(f"/devices#/live/{cam}")

    app.add_routes([
        web.post(prefix + "/cameras/{cam}/stream", open_stream),
        web.post(prefix + "/cameras/{cam}/stream/{sid}/offer", offer),
        web.delete(prefix + "/cameras/{cam}/stream/{sid}", close_stream),
        web.get("/webrtc/{sid}", view_page),
        web.get("/watch/{cam}", watch),
    ])




# ── standalone runner (for testing without the full hub) ────────────────────
def setup_media_ota_proxy(app: web.Application) -> None:
    """If NN_MEDIA_OTA_URL is set, proxy the camera OTA to the media OTA service.

      POST /api/v1/cameras/{cam}/ota   one-shot: stage(both)+verify+arm+apply,
                                       returns {"result":"ok","c6":..,"p4":..}

    The media OTA service (host-side) owns the camera's encrypted control
    channel; this just forwards the trigger so the camera OTAs via the hub API.
    """
    svc = os.environ.get("NN_MEDIA_OTA_URL")
    if not svc:
        return

    async def proxy_ota(request):
        cam = request.match_info["cam"]
        try:
            async with ClientSession(timeout=ClientTimeout(total=320)) as cs:
                async with cs.post(f"{svc}/api/v1/media/{cam}/ota") as r:
                    return web.json_response(await r.json(), status=r.status)
        except Exception as e:  # service down / timeout
            return web.json_response({"error": str(e)}, status=502)

    app.router.add_post("/api/v1/cameras/{cam}/ota", proxy_ota)
    log.info("media OTA proxy mounted → %s", svc)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8769)
    ap.add_argument("--service-url", default="http://127.0.0.1:8899")
    ap.add_argument("--recv-ip", default="127.0.0.1")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    gw = VideoGateway(args.service_url, args.recv_ip)
    app = web.Application()
    setup_video_routes(app, gw)
    setup_media_ota_proxy(app)   # POST /api/v1/cameras/{cam}/ota (if NN_MEDIA_OTA_URL set)

    async def on_cleanup(app):
        for sid in list(gw.sessions):
            await gw.close_stream(sid)
    app.on_cleanup.append(on_cleanup)

    print(f">> hub video gateway on http://0.0.0.0:{args.port}  (service {args.service_url})")
    web.run_app(app, host="0.0.0.0", port=args.port, print=None)


if __name__ == "__main__":
    main()
