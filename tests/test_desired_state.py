"""Desired-state (pending write) tracking in HubProtoRouter's field cache.

The webapp moves a switch only on the device's own report; until then it
shows the intent.  These pin the lifecycle: set -> confirmed / superseded /
rejected / expired, twin names, and the merged cache shape."""

import json
from pathlib import Path

import pytest

from hub.db import DB
from hub.proto_router import HubProtoRouter

pytestmark = pytest.mark.asyncio
DEV = "aa00bb11cc22dd33"


class _Srv:
    def is_gateway_online(self, gw_id):
        return False

    async def send_to_gateway(self, gw_id, frame):
        return True


class _Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def time(self):
        return self.t


@pytest.fixture
def router(tmp_path: Path, monkeypatch):
    clk = _Clock()
    import hub.proto_router as pr
    monkeypatch.setattr(pr.time, "time", clk.time)
    r = HubProtoRouter(DB(tmp_path / "h.db"), _Srv(),
                       hub_privkey_path=tmp_path / "priv.bin")
    r.clk = clk
    return r


async def push(r, name, v):
    await r._on_auto_event(DEV, 0, json.dumps({"t": "fld", "n": name, "v": v}).encode())


async def test_pending_is_merged_into_cache(router):
    await push(router, "led", 0)
    router.set_desired(DEV, "led", 1)
    e = router.get_field_cache(DEV)["led"]
    assert e["v"] == 0 and e["desired"] == 1 and e["desired_by"] == "operator"


async def test_confirmed_by_device_push(router):
    await push(router, "led", 0)
    router.set_desired(DEV, "led", 1)
    router.clk.t += 10
    await push(router, "led", 1)
    assert "desired" not in router.get_field_cache(DEV)["led"]


async def test_confirmed_by_answered_write(router):
    router.set_desired(DEV, "led", 1)
    router.remember_field(DEV, "led", 1.0)
    assert router.get_field_cache(DEV)["led"] == {"v": 1.0, "ts": int(router.clk.t)}


async def test_late_echo_does_not_cancel(router):
    router.set_desired(DEV, "led", 1)
    router.clk.t += 1                       # an earlier push landing late
    await push(router, "led", 0)
    assert router.get_field_cache(DEV)["led"]["desired"] == 1


async def test_superseded_by_a_later_different_report(router):
    router.set_desired(DEV, "led", 1)
    router.clk.t += 20                      # a rule / the button won
    await push(router, "led", 0)
    e = router.get_field_cache(DEV)["led"]
    assert e["v"] == 0 and "desired" not in e


async def test_twin_name_maps_to_base(router):
    router.set_desired(DEV, "button_v", 1)
    assert router.get_field_cache(DEV)["button"]["desired"] == 1
    router.remember_field(DEV, "button_v", 1.0)
    assert "desired" not in router.get_field_cache(DEV)["button"]


async def test_expires_unconfirmed(router):
    router.set_desired(DEV, "led", 1)
    router.clk.t += HubProtoRouter.DESIRED_TTL_S + 1
    assert "led" not in router.get_field_cache(DEV)


async def test_rejected_clears(router):
    router.set_desired(DEV, "led", 1)
    router.clear_desired(DEV, "led", "rejected: out_of_range")
    assert "led" not in router.get_field_cache(DEV)


async def test_cache_untouched_without_pending(router):
    await push(router, "temperature", 21.5)
    c = router.get_field_cache(DEV)
    assert c is router._field_cache[DEV]    # no copy on the hot path


async def test_no_pending_when_already_at_value(router):
    await push(router, "led", 1)
    router.set_desired(DEV, "led", 1, "s1_btn_all_leds")
    assert "desired" not in router.get_field_cache(DEV)["led"]
