"""The hub handles and acks the first copy of a sensor's burst only."""
import struct
import types

from hub.proto_router import HubProtoRouter


def _router():
    r = types.SimpleNamespace(_burst_first={}, _rx_counts={},
                              BURST_COPY_WINDOW_S=HubProtoRouter.BURST_COPY_WINDOW_S)
    r.check = lambda did, p, t: HubProtoRouter._is_burst_copy(r, did, p, t)
    return r


def _p(cmd, tid):
    return struct.pack("<HI", cmd, tid) + b"body"


def test_copies_inside_window_dropped_and_counted():
    r = _router()
    assert r.check("d1", _p(0x40, 7), 100.000) is False
    assert r.check("d1", _p(0x40, 7), 100.030) is True
    assert r.check("d1", _p(0x40, 7), 100.061) is True
    assert r._rx_counts["d1"]["hub_burst_copies"] == 2


def test_retry_after_window_is_handled():
    r = _router()
    assert r.check("d1", _p(0x40, 7), 100.0) is False
    # the sensor's shortest retry timeout is 150 ms: a real retry
    assert r.check("d1", _p(0x40, 7), 100.2) is False


def test_other_tid_cmd_device_and_tid_zero_not_dropped():
    r = _router()
    assert r.check("d1", _p(0x40, 7), 100.0) is False
    assert r.check("d1", _p(0x40, 8), 100.01) is False
    assert r.check("d1", _p(0x41, 8), 100.02) is False
    assert r.check("d2", _p(0x41, 8), 100.02) is False
    assert r.check("d1", _p(0x45, 0), 100.03) is False
    assert r.check("d1", _p(0x45, 0), 100.04) is False
    assert r.check("d1", b"\x01", 100.05) is False
