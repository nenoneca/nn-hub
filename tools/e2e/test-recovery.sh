#!/bin/bash
# Fault-injection test for the agent's power-loss recovery.  Simulates a
# power cut at each point in the two-rename platform swap, using dummy
# image files (never the real 4 GB ones), and checks the agent replays
# the intent correctly.  Safe: the real platform image is untouched —
# we point the agent at a scratch EDGE directory.
set -u
AG=/tmp/agent7.sh
SCRATCH=/tmp/edgetest
STATE=/tmp/nn-ota-test
PASS=0; FAIL=0

ck() { if [ "$2" = 0 ]; then PASS=$((PASS+1)); echo "PASS  $1"; else FAIL=$((FAIL+1)); echo "FAIL  $1 — $3"; fi; }

# a copy of the agent whose paths point at the scratch area, with the
# real service calls stubbed out
prep() {
  rm -rf $SCRATCH $STATE /tmp/nn-app-test; mkdir -p $SCRATCH/root/etc $STATE /tmp/nn-app-test
  python3 - <<'PYEOF'
src = open("/tmp/agent7.sh").read()
# keep everything up to the recovery call, then override the functions
# that touch real services and invoke recovery on the scratch area
head = src[:src.index("recover_swap || exit 1")]
head = head.replace("EDGE=/opt/edgeai", "EDGE=/tmp/edgetest")
head = head.replace("STATE=/var/lib/nn-ota", "STATE=/tmp/nn-ota-test")
head = head.replace("NN=/opt/nn-app", "NN=/tmp/nn-app-test")
stub = """
start_stack() { echo "[stub] start_stack"; }
stop_stack()  { echo "[stub] stop_stack"; return 0; }
recover_swap; echo RECOVERY-RC=$?
exit 0
"""
open("/tmp/agent7-test.sh", "w").write(head + stub)
PYEOF
  chmod +x /tmp/agent7-test.sh
  echo "1.0.0" > $SCRATCH/root/etc/nn-platform-version
}

echo "── power-loss recovery fault injection ──"

# Case A: cut AFTER journal, BEFORE any rename (image still in place)
prep
echo "1.0.9" > $STATE/plat-swap-journal
echo "REAL-IMAGE" > $SCRATCH/edgeai-rootfs.ext4
echo "NEW-IMAGE"  > $SCRATCH/edgeai-rootfs-new.ext4
bash /tmp/agent7-test.sh >/tmp/rec.a 2>&1
[ -f $SCRATCH/edgeai-rootfs.ext4 ] && [ ! -f $STATE/plat-swap-journal ]
ck "A: cut before renames → image intact, journal cleared" $? "$(cat /tmp/rec.a)"

# Case B: cut BETWEEN the two renames (image missing, new staged)
prep
echo "1.0.9" > $STATE/plat-swap-journal
echo "OLD-IMAGE" > $SCRATCH/edgeai-rootfs-prev.ext4
echo "NEW-IMAGE" > $SCRATCH/edgeai-rootfs-new.ext4
bash /tmp/agent7-test.sh >/tmp/rec.b 2>&1
[ "$(cat $SCRATCH/edgeai-rootfs.ext4 2>/dev/null)" = "NEW-IMAGE" ] && [ ! -f $STATE/plat-swap-journal ]
ck "B: cut mid-swap → swap completed to new image" $? "$(cat /tmp/rec.b)"

# Case C: new image lost too (worst case) → restore previous
prep
echo "1.0.9" > $STATE/plat-swap-journal
echo "OLD-IMAGE" > $SCRATCH/edgeai-rootfs-prev.ext4
bash /tmp/agent7-test.sh >/tmp/rec.c 2>&1
[ "$(cat $SCRATCH/edgeai-rootfs.ext4 2>/dev/null)" = "OLD-IMAGE" ] && \
  [ "$(cat $STATE/plat-blocked 2>/dev/null)" = "1.0.9" ]
ck "C: new image lost → previous restored + blacklisted" $? "$(cat /tmp/rec.c)"

# Case D: no journal (normal boot) → no-op
prep
echo "REAL-IMAGE" > $SCRATCH/edgeai-rootfs.ext4
bash /tmp/agent7-test.sh >/tmp/rec.d 2>&1
grep -q "RECOVERY-RC=0" /tmp/rec.d && ! grep -q "recovering" /tmp/rec.d
ck "D: no journal → clean no-op" $? "$(cat /tmp/rec.d)"

# Case E: journal present, NOTHING recoverable → loud failure, not silence
prep
echo "1.0.9" > $STATE/plat-swap-journal
bash /tmp/agent7-test.sh >/tmp/rec.e 2>&1
grep -q "manual intervention required" /tmp/rec.e && grep -q "RECOVERY-RC=1" /tmp/rec.e
ck "E: unrecoverable → explicit error, agent stops" $? "$(cat /tmp/rec.e)"

rm -rf $SCRATCH $STATE /tmp/nn-app-test /tmp/agent7-test.sh
echo "── recovery: $PASS pass, $FAIL fail ──"
[ $FAIL = 0 ]
