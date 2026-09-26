# media-host — the OPi camera/media services

The host-side media stack that runs beside the hub on the OrangePi:
per-camera ingest/HLS services, the shared WAVE5 transcoder, the shared
NPU inference daemon, and the control/OTA services.  Until 2026-08-19
these files lived ONLY on the host — every fix (the congestion-control
rework, HLS wall-clock anchoring, edge-caps forwarding) was deployed but
unversioned.  This directory is now the source of truth.

**Deploy direction is git → host, never host → git** (the hub tree was
once clobbered the other way; see repo history).  Target layout:

    /home/orangepi/nn_project_nowest/media/host/*.py     ← media-host/*.py
    systemd units                                        ← media-host/units/

Units reference absolute paths on the host and set PYTHONPATH to the hub
package; no credentials live in either — camera auth is key files under
--keydir, hub auth is nginx-level.

Service map (one line each):
- video_supervisor.py — nn-video: THE camera service (one unit).  Runs every
                        slot in /etc/nn-media/slots.yaml as a supervised
                        worker (video_service.py), restarts crashes with
                        backoff, kills hung pipelines, counts segfaults,
                        prefixes logs "[camN]", API 127.0.0.1:8898
                        (/health, /slots add/remove live, /reload).
- video_service.py    — per-slot worker: sectun ingest → GStreamer → HLS/WS,
                        AdaptController (delay-based congestion control),
                        snapshots, edge-caps + firmware forwarding to hub.
                        Exits loudly (43 thread exception, 44 gst error,
                        45 state timeout, 42 own HLS watchdog) so the
                        supervisor restarts it; /health for the supervisor.
                        Legacy per-slot units live in units/legacy/.
- nn_transcoded.py    — shared hardware transcode service (one WAVE5/CIX
                        session per camera, shm ring in, bounded tcp out).
- transcoded_client.py— shm-ring producer used by video_service.
- nn_inferd.py        — shared NPU inference daemon (one model, all cams).
- event_engine.py, infer_policy.py, inferd_client.py, yolox_detector.py
                      — detection policy + engines.
- media_ctrl_server.py, media_ota_service.py, media_ota_push.py
                      — device control channel + ESP camera OTA.
- secure_receiver.py, nn_sectun.py — sectun protocol.
- test_pipeline.py    — the 13-check service E2E gate.
