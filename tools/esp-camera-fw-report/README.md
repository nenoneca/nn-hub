# ESP camera firmware reporting — READY TO FLASH, NOT YET DEPLOYED

cam0/cam1 (ESP32-P4 + C6) are the only devices whose running firmware
the hub cannot see.  This closes that, using the mechanism the rest of
the fleet already uses.

## Status

| piece | state |
|---|---|
| P4 firmware: `fw` + `img` in the periodic status record | written, **not built/flashed** |
| host `video_service.py`: forwards identity to the hub on change | **deployed** (OPi, cam0–cam2 services) |
| hub `PUT/GET /api/v1/cameras/{cam}/bundle` | already live (proven with a synthetic report) |
| webapp camera Settings firmware panel | already live (renders as soon as a report exists) |

So the moment a camera is flashed with the firmware change, its version
appears in the webapp with no further work.

## Why it stopped here

Flashing cam0/cam1 means rebuilding two ESP-IDF images and reflashing
cameras that are currently serving video, with the documented P4 reset
gotchas (a warm reboot desyncs the C6 — a cold power-cycle is required).
That is bench work with a human present, not an unattended change.

## The patches

`app_main.status_task.c.snippet` — the P4 status task including the
`esp_app_get_description()` fields (buffer already widened to 640).
`video_service.status_report.py.snippet` — the host-side forwarder.

The full sources live in the (non-git) media tree at
`media/nn-app-camera-esp32p4c6-spi-imx708/p4/main/app_main.c` and
`media/host/video_service.py`; these snippets exist so the change is
recoverable if that tree is lost.

## After flashing

Full camera OTA (staging + P4/C6 lockstep swap via
`media/host/media_ota_service.py`) is the next increment: the service
exists and was validated once during bring-up, but it takes a fixed
firmware bundle at startup and needs catalog integration before it can
be driven from the webapp like every other device.
