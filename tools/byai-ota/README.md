# byai-ota — the BeagleY camera's three-layer update system

Everything here was live-validated on cam3 (2026-08-13..15): six bundle
releases, a platform swap, and every failure path (broken bundle,
failing platform, lost frames) recovered automatically.

## Layers

| layer | artifact | size | switch | updated by |
|---|---|---|---|---|
| fixed | `nn-ota-launcher` + units | KBs | bench / rootfs A/B | `install-fixed-layer.sh` |
| bundle | tar.gz: `nn-camera`, `cam_run.sh`, `agent.sh`, manifest | ~32 KB | atomic symlink flip | agent, 10-min tick |
| platform | gzipped ext4 (slim edgeai container) | ~1.4 GB | container file swap | agent |

App state lives on the HOST at `/opt/nn-app` (releases, `current`
symlink, kv, logs), bind-mounted into the container at `/opt/nn`
(`lxc.mount.entry`) — platform swaps never touch it.  Set up once by
`externalize-app-state.sh`.

## Update flow (both layers)

hub catalog target != running → download from `gw_firmware` HTTP →
sha256 verify → stage → flip/swap → write `pending` → next tick
health-check → confirm, or roll back + blacklist the version.
Platform health additionally requires onnxruntime mapped in-process
(a broken platform streams happily with inference dead).
The agent re-execs itself from /tmp before a platform swap — it lives
on the filesystem being unmounted.

## Releasing

    ./release.sh bundle 0.1.7      # pack current board binaries + this agent.sh
    ./release.sh platform 1.0.3    # rebuild slim image from SDK original on hub host

Camera applies within its 10-min tick and self-confirms; watch
`GET /api/v1/cameras/cam3/bundle` (webapp: camera → Settings).

## Crash safety

The platform switch journals its intent (`/var/lib/nn-ota/plat-swap-journal`)
before the first rename and clears it after the second, so a power cut
mid-swap is replayed on the next tick: complete the swap if the new image
is staged, restore the previous image if it isn't, and refuse loudly if
neither exists.  `tools/e2e/test-recovery.sh` fault-injects all five
cases (it runs against scratch files — safe on any machine).
A `flock` guard keeps a long platform download from overlapping the
next tick.

## Hard-won rules

- NEVER build multi-GB artifacts on the board or ship them from it —
  its WiFi upload is pathological (~72 KB/s).  Build on the hub host.
- After the 4 GB gunzip the agent syncs + drops caches before swapping:
  starting the camera under writeback pressure correlated with
  intermittent early SIGSEGVs (cores stay armed via the fixed layer;
  a crash leaves `/opt/nn-app/log/core.*`).
- A leaked read-only loop mount of an image makes later rw mounts of
  the same file silently RO (shared superblock) — always check umount.
- The blocked-version file (`/var/lib/nn-ota/{blocked,plat-blocked}`)
  is the operator interface for "stop retrying that build".

## librproc_byname.so — remoteproc-by-name shim

TI's prebuilt `libtivision_apps.so` hardcodes `open("/dev/remoteproc0")`
to reach the C7x, but remoteproc indices are handed out in driver probe
order — a udev race.  On 2026-08-19 the offline MCU R5F won index 0 and
the TIDL network buffer bounced through swiotlb (256 KB per-mapping cap):
nn-camera segfault-looped.  The shim (source: `rproc_byname.c`) LD_PRELOADs
into nn-camera via `cam_run.sh` and redirects every `/dev/remoteprocN`
open to whichever index today NAMES the intended core, scanning
`/sys/class/remoteproc/*/name`.  Mapping via `NN_RPROC_MAP`
(default `0=7e000000.dsp,1=7e200000.dsp`); an unresolvable name falls
through to the original path.

Cross-build on the host (never on the board):

    aarch64-linux-gnu-gcc -O2 -fPIC -shared -o librproc_byname.so \
        rproc_byname.c -ldl -pthread

It ships inside the bundle (`release.sh` packs it from the board's
current release), so bundle OTAs keep it; `cam_run.sh` must keep the
`LD_PRELOAD=/opt/nn/current/librproc_byname.so` in its exec line.
Defence in depth on the same bug class, all on the board:
`/etc/modprobe.d/nn-rproc-order.conf` (softdep: DSP driver loads before
R5F) and the nn-camera boot gate drop-in (waits for the C7x cores and
rpmsg_ctrl devices before starting).  Do NOT force the rproc modules via
modules-load.d — loading them that early broke the R5F cluster probe
(`rproc_add -16`), measured 2026-08-19.

## nn-camera-recover — crash recovery ladder

A crashed nn-camera (SIGSEGV) skips the SIGINT handshake and can leave
the C7x wedged; nothing short of a reboot clears that (stop times out,
unbind leaks the mailbox channel on THIS kernel — every unbind, not just
wedged ones — and rmmod+modprobe then fails mbox_request_channel forever).
`StartLimitBurst=5/10min` + `OnFailure=nn-camera-recover.service` runs
the ladder: reap + one clean restart, verified against BOTH liveness
("up=1") and the inference verdict (streaming-only after a crash = still
wedged); if unhealthy, reboot — at most once per 6 h (stamp in
/var/lib/nn-recover), so a fault a reboot cannot fix degrades to
"camera down, humans notified" instead of a reboot loop.
