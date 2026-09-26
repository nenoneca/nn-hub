# e2e.sh — fleet end-to-end test

    ./e2e.sh                 # ~2 min, non-destructive
    ./e2e.sh --ota 0.0.27    # + a real OTA cycle on c6-s3 (~10 min, reboots it)
    SETTLE=60 ./e2e.sh       # longer pre-test quiet period

Covers: hub health · sealed field reads (session crypto hot path) ·
field write round-trip · D2D group-keyed cascade with cache freshness ·
image identity keying · catalog reachability · camera bundle+platform
reporting · camera stream.

## Reading the results

Every check prints its evidence.  Expected steady-state numbers:

| check | healthy | notes |
|---|---|---|
| sealed read | 60–200 ms | seconds means mesh congestion, not breakage |
| cascade freshness | +0–5 s | button_v edge → all LEDs → hub cache |
| camera report age | < 10 min | agent reports each tick |

Two deliberate tolerances, both matching how the system actually works:
a device mid-OTA is SKIPped (not failed), and one missed cascade edge or
one slow read gets a single retry — the mesh is lossy by nature and the
protocol retries by design.  A *second* consecutive miss is a real fail.

Run it after any hub deploy, firmware roll, or platform swap.
