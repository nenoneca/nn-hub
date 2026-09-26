# Stream strategy: what a camera sacrifices when the link gets tight

Per camera, set in the webapp (camera → Settings → "Under congestion").

| mode | holds | slider selects | behaviour |
|---|---|---|---|
| **Keep motion** (`fps`) | frame rate | bitrate floor, 1 Mbps … last measured link rate | full fps; sheds bitrate first, and only once it reaches the floor does it start dropping frames.  Recovers frames BEFORE bitrate. |
| **Keep quality** (`quality`) | picture detail | frame rate, 5 fps … sensor max | pins fps at the chosen level and lets bitrate ride to whatever the link sustains. |

Slider stops are computed by the hub: `5 + i*(max_fps-5)/5` or
`1000 + i*(net_kbps-1000)/5` kbps, i in 0..5, where `net_kbps` is the
best rate the link has actually carried (published by the adapter).

## Where each piece lives
- `GET/PUT /api/v1/cameras/{cam}/stream-policy` — hub stores `{mode, level}`
  (setting `stream_policy:<cam>`) and returns the resolved stops/value.
- `AdaptController` in `media/host/video_service.py` polls that every 10 s and
  drives the device with the existing controls (CMD_BITRATE, CMD_FPS_DIV,
  CMD_GOP).  A hub hiccup keeps the last known policy — it must never change
  how a camera streams.
- The device applies commands as before; no firmware change was needed,
  because its own adapt loop yields whenever a host command is <5 s old.

## Verified 2026-08-17
Quality mode, level 1 → hub resolved 10 fps → adapter logged
`adapt[quality]: bitrate=6000000 fps=10` → camera reported `fps_div=3`.
FPS mode → `fps_div=1` (30 fps) on both P4 cameras, bitrate ramping.

## Caveat worth knowing
Measured on cam0: ~1.2 Mbps at 15 fps, ~2.96 Mbps at 30 fps — bitrate scales
almost exactly with frame count, i.e. bits-per-frame is constant.  That is a
QP-driven encoder, not a rate-targeted one.  So today "keep quality" buys
fewer frames and less bandwidth, but NOT a sharper picture; the sharpness
lever is the firmware's rate control (or an explicit QP control), which is
open with the camera session.
