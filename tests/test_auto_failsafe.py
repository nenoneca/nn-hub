"""Hub-side cascade failsafe (hub/auto_failsafe.py).

Fleet under test mirrors the bench: c6-s1's button drives the LEDs of
c6-s1, c6-s2 and c6-s3.  The trigger field is not pushed to the hub; the
rule's action on the trigger device's own LED is, and that is what tells the
hub the rule fired.
"""
import asyncio
import base64
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

from hub.auto_failsafe import AutoFailsafe
from hub.db import DB

pytestmark = pytest.mark.asyncio

YAML = """
automations:
  - id: s1_btn_all_leds
    trigger:
      - device: c6-s1
        field: button
        above: 0.5
    action:
      - device: c6-s1
        field: led
        value: 1
      - device: c6-s2
        field: led
        value: 1
      - device: c6-s3
        field: led
        value: 1
  - id: s1_btn_all_leds_off
    trigger:
      - device: c6-s1
        field: button
        below: 0.5
    action:
      - device: c6-s1
        field: led
        value: 0
      - device: c6-s2
        field: led
        value: 0
      - device: c6-s3
        field: led
        value: 0
"""
S1, S2, S3 = "744ba7e40c0054c5", "b9a96b8d4e6ad084", "e4f663849db42bc4"


class FakeRouter:
    def __init__(self):
        self.cache = {}
        self.listeners = []

    def add_field_event_listener(self, cb):
        self.listeners.append(cb)

    def get_field_cache(self, dev):
        return self.cache.get(dev, {})

    def push_event(self, dev, fld, v, ts):
        """What proto_router does on an AUTO_EVENT: cache, then observers."""
        self.cache.setdefault(dev, {})[fld] = {"v": v, "ts": ts}
        for cb in self.listeners:
            cb(dev, fld, v)


class Clock:
    def __init__(self, t=1_000.0):
        self.t = t

    def __call__(self):
        return self.t


def _fleet(tmp_path: Path):
    db = DB(tmp_path / "h.db")
    pub = base64.b64encode(b"\x01" * 32).decode()
    for did, name in ((S1, "c6-s1"), (S2, "c6-s2"), (S3, "c6-s3")):
        db.register_device(did, name, "sample_c6", "")
        db.set_provision_info(device_id=did, device_type="sample_c6",
                              enc_pubkey_b64=pub, gateway_id="gw")
    db.set_setting("automations_yaml", YAML)
    return db


def _make(tmp_path, writes, fail_for=()):
    db = _fleet(tmp_path)
    router = FakeRouter()
    clock = Clock()

    async def set_field(router_, dev, fld, value, dev_pub, enc_priv, timeout=10.0):
        writes.append((dev, fld, value))
        if dev in fail_for:
            raise asyncio.TimeoutError()
        return {"name": fld, "value": value}

    fs = AutoFailsafe(db, router, X25519PrivateKey.generate(), set_field, clock=clock)
    return db, router, clock, fs


async def test_lost_notify_is_written_after_the_delay(tmp_path):
    writes = []
    db, router, clock, fs = _make(tmp_path, writes)
    # s1 fired the "on" rule: its own LED event arrives; s3 follows, s2 is silent
    router.push_event(S1, "led", 1.0, clock.t)
    q = fs.status()["queue"]
    assert sorted(w["device"] for w in q) == ["c6-s2", "c6-s3"]
    clock.t += 1.0
    router.push_event(S3, "led", 1.0, clock.t)        # s3 reported: resolved by event
    assert [w["device"] for w in fs.status()["queue"]] == ["c6-s2"]
    await fs.tick()                                    # not due yet (3 s)
    assert writes == []
    clock.t += 2.5
    await fs.tick()
    assert writes == [(S2, "led", 1.0)], "s2 never reported -> the hub writes the action"
    assert fs.status()["queue"] == []
    st = fs.stats
    assert st["resolved_by_event"] == 1 and st["resolved_by_write"] == 1


async def test_write_failure_retries_with_backoff_until_it_lands(tmp_path):
    writes = []
    fail = {S2}
    db, router, clock, fs = _make(tmp_path, writes, fail_for=fail)
    router.push_event(S1, "led", 0.0, clock.t)
    router.push_event(S3, "led", 0.0, clock.t)
    clock.t += 3.5
    await fs.tick()                                    # attempt 1 fails
    assert len(writes) == 1 and fs.status()["queue"][0]["attempts"] == 1
    clock.t += 1.0
    await fs.tick()                                    # not due: backoff 3 s
    assert len(writes) == 1
    clock.t += 3.0
    await fs.tick()                                    # attempt 2 fails
    assert len(writes) == 2
    fail.clear()                                       # mesh recovers
    clock.t += 6.5
    await fs.tick()                                    # attempt 3 lands
    assert len(writes) == 3 and fs.status()["queue"] == []
    assert fs.stats["write_failures"] == 2 and fs.stats["resolved_by_write"] == 1


async def test_newer_edge_supersedes_the_pending_watch(tmp_path):
    writes = []
    db, router, clock, fs = _make(tmp_path, writes)
    router.push_event(S1, "led", 1.0, clock.t)         # on
    clock.t += 1.0
    router.push_event(S1, "led", 0.0, clock.t)         # off again before anyone caught up
    q = fs.status()["queue"]
    assert all(w["desired"] == 0.0 for w in q) and len(q) == 2
    assert fs.stats["superseded"] == 2
    clock.t += 3.5
    await fs.tick()
    assert sorted(writes) == sorted([(S2, "led", 0.0), (S3, "led", 0.0)])


async def test_target_that_already_reported_is_not_queued(tmp_path):
    writes = []
    db, router, clock, fs = _make(tmp_path, writes)
    router.push_event(S2, "led", 1.0, clock.t)         # s2 got the notify first
    router.push_event(S3, "led", 1.0, clock.t)
    router.push_event(S1, "led", 1.0, clock.t)         # s1's own event arrives last
    assert fs.status()["queue"] == [], "everyone already reports the desired value"
    clock.t += 5
    await fs.tick()
    assert writes == []


async def test_hub_own_write_does_not_retrigger(tmp_path):
    writes = []
    db, router, clock, fs = _make(tmp_path, writes)
    router.push_event(S1, "led", 1.0, clock.t)
    clock.t += 3.5
    await fs.tick()                                    # writes s2 and s3
    assert len(writes) == 2
    # the devices echo the hub's writes as events: must not be read as
    # "a rule fired on s2", which would queue s1 and s3 again
    router.push_event(S2, "led", 1.0, clock.t)
    router.push_event(S3, "led", 1.0, clock.t)
    assert fs.status()["queue"] == []


async def test_late_event_from_an_action_device_is_not_a_new_edge(tmp_path):
    """14:41:06 on the bench: the operator pressed on, then off; s3's 'on'
    event arrived after s1 had already reported 'off'.  Believing s3 would
    queue s1 and s2 back to 'on' — wrong.  Only the trigger device's own
    action counts as evidence of an edge."""
    writes = []
    db, router, clock, fs = _make(tmp_path, writes)
    router.push_event(S1, "led", 1.0, clock.t)         # on
    clock.t += 2
    router.push_event(S2, "led", 1.0, clock.t)
    clock.t += 1
    router.push_event(S1, "led", 0.0, clock.t)         # off, 3 s later
    clock.t += 1
    router.push_event(S3, "led", 1.0, clock.t)         # s3's LATE 'on' event
    q = fs.status()["queue"]
    assert q and all(w["desired"] == 0.0 for w in q), f"late 'on' must not reverse the edge: {q}"
    clock.t += 4
    await fs.tick()
    assert all(v == 0.0 for _, _, v in writes) and writes, writes


async def test_rule_without_trigger_action_uses_any_action_device(tmp_path):
    """A rule whose actions do not include the trigger device has no better
    evidence than its action devices' events."""
    writes = []
    db, router, clock, fs = _make(tmp_path, writes)
    db.set_setting("automations_yaml", """
automations:
  - id: s1_btn_others
    trigger:
      - device: c6-s1
        field: button
        above: 0.5
    action:
      - device: c6-s2
        field: led
        value: 1
      - device: c6-s3
        field: led
        value: 1
""")
    router.push_event(S3, "led", 1.0, clock.t)
    assert [w["device"] for w in fs.status()["queue"]] == ["c6-s2"]


async def test_hub_write_echo_is_remembered_long_enough(tmp_path):
    """The echo of a hub write took 10 s to arrive on the bench mesh."""
    writes = []
    db, router, clock, fs = _make(tmp_path, writes)
    router.push_event(S1, "led", 1.0, clock.t)
    clock.t += 3.5
    await fs.tick()                                    # hub writes s2, s3
    clock.t += 12                                      # slow echo
    router.push_event(S1, "led", 0.0, clock.t)         # operator pressed off meanwhile
    router.push_event(S2, "led", 1.0, clock.t)         # s2's echo of the OLD write
    q = fs.status()["queue"]
    assert q and all(w["desired"] == 0.0 for w in q), q


async def test_disabled_does_nothing(tmp_path):
    writes = []
    db, router, clock, fs = _make(tmp_path, writes)
    db.set_setting("auto_failsafe_enabled", "0")
    router.push_event(S1, "led", 1.0, clock.t)
    clock.t += 10
    await fs.tick()
    assert writes == [] and fs.status()["queue"] == []


async def test_status_and_settings_over_api(tmp_path):
    from aiohttp.test_utils import TestClient, TestServer
    from hub.api import make_app
    from hub.log_store import LogStore
    db = _fleet(tmp_path)
    router = FakeRouter()
    fs = AutoFailsafe(db, router, X25519PrivateKey.generate(),
                      lambda *a, **k: None, clock=Clock())
    router.auto_failsafe = fs
    app = make_app(db, LogStore(tmp_path / "logs", rotate_interval_sec=3600),
                   X25519PrivateKey.generate(), proto_router=router)
    async with TestClient(TestServer(app)) as cli:
        r = await cli.get("/api/v1/auto/failsafe")
        d = await r.json()
        assert r.status == 200 and d["enabled"] is True and d["delay_s"] == 3.0
        assert [x["id"] for x in d["rules"]] == ["s1_btn_all_leds", "s1_btn_all_leds_off"]
        r = await cli.put("/api/v1/auto/failsafe", json={"delay_s": 5, "enabled": False})
        d = await r.json()
        assert d["enabled"] is False and d["delay_s"] == 5.0
        r = await cli.put("/api/v1/auto/failsafe", json={"delay_s": 0})
        assert r.status == 400
