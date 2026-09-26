# HLS PROGRAM-DATE-TIME self-heal (media/host/video_service.py)

## The bug
ffmpeg anchors `EXT-X-PROGRAM-DATE-TIME` once at start and then advances it by
MEDIA time.  Every stall — camera reconnect, codec hiccup, a service restart
that waits for video — burns wall-clock time the date never regains.  Measured
2026-08-16: cam3 11m31s behind, cam0 299s, cam1 221s.  The live view shows that
date as the capture time, so the stream looks minutes stale while actually being
~3 s behind live.  The offset is a *step per outage*, not a rate error: measured
120.096 s of media time per 120 s of wall clock (rate is correct).

## Why not just `-use_wallclock_as_timestamps 1`
It is already used — but only on the non-transcoded path.  With nn-transcoded,
the connect-time buffered burst would get stamped "now" and push the playlist
date into the FUTURE.

## The fix
A watchdog inside the ffmpeg supervision loop: sample the newest PDT in the
playlist every 30 s and, when it lags wall clock by more than
`NN_HLS_PDT_MAX_SKEW` (default 45 s), terminate ffmpeg so the loop re-anchors it.
Costs one ~2 s segment gap, only when actually needed, and works on both paths.

Healthy skew after the change: cam0 0.6 s, cam1 -5.0 s, cam3 1.1 s.
(A slightly negative skew on the transcoded path is the known burst effect.)

Full source lives in the non-git media tree at media/host/video_service.py;
this snippet exists so the change is recoverable if that tree is lost.

## Follow-up: cam1 was ~26 s behind live (2026-08-17)

Three compounding causes, fixed in order:

1. **Unbounded PDT accumulation** on the nn-transcoded path, where
   `-use_wallclock_as_timestamps` was disabled: media time ran ~1.3 s/min
   slower than wall clock, so the date slid ~78 s/hour behind and the
   watchdog could only bound it (hence the ~40 s a viewer saw).
   `NN_HLS_WALLCLOCK=1` forces wall-clock stamping there too → accumulation
   stopped (26.8→28.1 s climbing became flat).
2. **Unbounded sink buffering**: `tcpserversink` defaults to
   `units-max = -1`, so one slow moment builds a backlog that never drains.
   Now capped at 2 s of media (`unit-format=time units-max=2000000000`,
   soft-max 1 s, `recover-policy=keyframe`, `sync-method=latest-keyframe`).
   26 s → 7 s.
3. **The shared transcoder itself**: moving cam1 to in-process transcode
   (`NN_TRANSCODED=0`) removed a whole decode/encode hop → 7 s → 5 s AND
   *lowered* host CPU (84.7 % → 93.0 % idle).  The shared service is worth
   using only where its CPU saving is real; for this camera it cost both.

Residual: cam1 sits at ~5 s with uneven segments (0.8–2.3 s) versus ~1 s and
steady on cam0/cam3.  What remains is bursty delivery from the camera itself,
which is a firmware/link question, not a host one.
