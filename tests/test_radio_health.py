"""RADIO_STATS v1 parsing and the health ratios (hub/radio_health.py)."""
import struct

from hub import radio_health as rh


def body(ch=25, role=2, parent=0x2000, rssi=-40, lq=(3, 3), up=600, tx=(100, 30, 5, 1),
         mac=(140, 40, 500, 7), ip=(3, 0), mle=(1, 2, 0), flags=1):
    b = struct.pack("<BBBBHbBB", 1, flags, ch, role, parent, rssi, lq[0], lq[1])
    b += struct.pack("<11I", up, *tx, *mac, *ip)
    b += struct.pack("<3H", *mle)
    assert len(b) == rh.V1_LEN
    return b


def test_parse_v1_fields():
    d = rh.parse_v1(body())
    assert d["channel"] == 25 and d["role"] == "child" and d["parent"] == "0x2000"
    assert d["parent_rssi"] == -40 and d["tx_cca_busy"] == 30 and d["mac_tx_retry"] == 40
    assert d["ip_rx_fail"] == 3 and d["parent_changes"] == 1 and d["driver_counters"] is True


def test_parse_rejects_other_versions_and_short():
    assert rh.parse_v1(b"\x02" + bytes(58)) is None
    assert rh.parse_v1(body()[:40]) is None


def test_no_parent_and_rssi_sentinels():
    d = rh.parse_v1(body(parent=0xFFFF, rssi=127, role=1))
    assert d["parent"] is None and d["parent_rssi"] is None and d["role"] == "detached"


def _rep(ts, hub=(0, 0), **kw):
    d = rh.parse_v1(body(**kw)); d["ts"] = ts
    d["hub_rx"], d["hub_repeats"] = hub
    return d


def test_ratios_over_interval():
    a = _rep(1000, up=600, tx=(100, 30, 5, 0), mac=(140, 40, 500, 0), hub=(50, 5))
    b = _rep(1300, up=900, tx=(160, 90, 15, 0), mac=(210, 110, 600, 0), hub=(70, 9))
    r = rh.ratios(rh.delta(a, b))
    # attempts 60+60+10 = 130; busy 60/130
    assert r["busy_pct"] == round(100 * 60 / 130, 1)
    assert r["no_ack_pct"] == round(100 * 10 / 70, 1)
    # 70 frames + 70 retries on air -> half the attempts were retries
    assert r["mac_retry_pct"] == 50.0 and r["hub_repeat_pct"] == 20.0
    assert r["rebooted"] is False and r["seconds"] == 300


def test_reboot_counts_from_zero():
    a = _rep(1000, up=5000, tx=(900, 300, 50, 0))
    b = _rep(1300, up=200, tx=(40, 10, 2, 0))
    d = rh.delta(a, b)
    assert d["rebooted"] is True and d["tx_ok"] == 40 and d["tx_cca_busy"] == 10


def test_summarize_single_report_has_no_window():
    s = rh.summarize([_rep(1000)])
    assert s["reports"] == 1 and "window" not in s


def test_zero_attempts_gives_none_not_crash():
    a = _rep(1000, tx=(5, 0, 0, 0)); b = _rep(1300, tx=(5, 0, 0, 0))
    assert rh.ratios(rh.delta(a, b))["busy_pct"] is None
