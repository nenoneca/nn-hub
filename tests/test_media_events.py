"""event_engine pure logic: NAL classification, the GOP-aligned ring
buffer, motion hysteresis, and the per-class policy gate."""

from tests.media_helper import import_media

ee = import_media("event_engine")

SPS   = b"\x00\x00\x01\x67conf"
IDR   = b"\x00\x00\x01\x65" + b"I" * 40
SLICE = b"\x00\x00\x01\x41" + b"P" * 40
VID_START = 0x02


def test_first_nal():
    assert ee._first_nal(SPS) == 7
    assert ee._first_nal(IDR) == 5
    assert ee._first_nal(SLICE) == 1
    assert ee._first_nal(b"\x00\x00\x00\x01\x41x") == 1   # 4-byte start code
    assert ee._first_nal(b"garbage") is None or ee._first_nal(b"garbage") == 0 \
        or ee._first_nal(b"garbage") not in (1, 5, 7)


def test_ring_evicts_by_time_but_snapshot_starts_on_gop():
    r = ee.AVRingBuffer(keep_s=10)
    for i in range(30):                       # 30 s of 1 GOP/s
        ts = i * 1000
        r.append("V", ts, VID_START, SPS)     # GOP boundary
        r.append("V", ts + 10, 0, SLICE)
    # only ~keep_s seconds retained
    snap = r.snapshot_from(0)
    assert snap[0][1] >= (30 - 12) * 1000     # old GOPs evicted
    # snapshot from mid-GOP starts at that GOP's SPS, not mid-air
    snap = r.snapshot_from(28_500)
    assert snap[0][1] == 28_000
    assert snap[0][3] == SPS                  # decodable from record one


def test_ring_byte_cap_evicts_even_fresh_data():
    r = ee.AVRingBuffer(keep_s=3600, max_bytes=10_000)
    for i in range(100):
        r.append("V", i * 100, VID_START, SPS)
        r.append("V", i * 100 + 1, 0, b"\x00\x00\x01\x41" + b"P" * 500)
    assert r.bytes <= 11_500                  # cap held (± one open chunk)


def test_motion_before_policy_uses_plain_threshold():
    """A motion tick can arrive before the hub's policy doc — this used
    to crash on the unset _motion_pol (fixed in __init__)."""
    e = ee.EventEngine("http://hub", motion_thresh=0.02)
    assert not e._motion_active(0.001)
    assert e._motion_active(0.05)
    e.motion_thresh = 0.5                    # runtime-adjustable
    assert not e._motion_active(0.1)


def test_motion_pseudo_class_hysteresis():
    e = ee.EventEngine("http://hub")
    e.set_policy({"classes": {
        "motion": {"capture": True, "agg": 2, "start": 0.5, "stop": 0.2}}})
    assert not e._motion_active(0.9)         # window mean 0.45 < start
    assert e._motion_active(0.9)             # mean 0.9 → DETECT
    assert e._motion_active(0.0)             # mean 0.45 ≥ stop → holds
    assert not e._motion_active(0.0)         # mean 0 < stop → clears


def test_policy_gate_capture_false_never_fires():
    e = ee.EventEngine("http://hub")
    e.set_policy({"classes": {
        "person": {"capture": True,  "agg": 1, "start": 0.5, "stop": 0.4},
        "tv":     {"capture": False, "agg": 1, "start": 0.5, "stop": 0.4},
    }}, version=3)
    firing = e._policy_capturing([{"cls": "person", "score": 0.9},
                                  {"cls": "tv", "score": 0.9}])
    assert firing == ["person"]              # tv tracked, never fires
    # unknown class (no policy row) must not sneak through the gate
    assert e._policy_capturing([{"cls": "unicorn", "score": 0.99}]) == []
    # absent class fed 0 every frame → decays and clears
    assert e._policy_capturing([]) == []
