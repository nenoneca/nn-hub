"""ChannelManager: scan job, vote, migrate + verify, automatic trigger."""
import asyncio
import struct
import time
from pathlib import Path

import pytest

from hub import channel_manager as cmod
from hub import network as net_mod
from hub import proto
from hub.db import DB



def _scan_bytes(loud: dict, base=-98):
    return bytes(((loud.get(ch, base)) & 0xff) for ch in range(11, 27))


class _Srv:
    def __init__(self, online):
        self.online = set(online)

    def is_gateway_online(self, gid):
        return gid in self.online


class _Router:
    def __init__(self, online, gw_scan, dev_scans, dataset_after=None):
        self._server = _Srv(online)
        self.gw_scan, self.dev_scans = gw_scan, dev_scans
        self.sets, self.dataset_after = [], dataset_after
        self.dev_calls = []

    async def gateway_request(self, gid, cmd, body=b"", timeout=10, attempts=2):
        if cmd == proto.Cmd.GW_CHANNEL_SCAN:
            return 0, bytes([25]) + self.gw_scan
        if cmd == proto.Cmd.GW_CHANNEL_SET:
            self.sets.append((gid, body[0], struct.unpack_from("<H", body, 1)[0]))
            return 0, b""
        if cmd == proto.Cmd.GW_DATASET_GET:
            return 0, self.dataset_after
        raise AssertionError(cmd)

    async def request_h2d(self, did, cmd, body, expect, **kw):
        self.dev_calls.append(did)
        if did not in self.dev_scans:
            raise asyncio.TimeoutError()
        return bytes([0, 25]) + self.dev_scans[did]


def _db(tmp_path: Path, channel=25):
    db = DB(tmp_path / "h.db")
    net_mod.get_or_create_network(db, channel=channel)
    db.register_gateway("aa" * 8, "gw-a", "x", "")
    db.register_device("d1" * 8, "s1", "sample_c6", "")
    db.register_device("d2" * 8, "s2", "sample_c6", "")
    now = int(time.time())
    for did in ("d1" * 8, "d2" * 8, "dead" * 4):          # "dead": not registered
        db.add_radio_stats(did, now - 60, {"uptime_s": 100, "tx_ok": 0})
    db.set_setting("radio.channel.sensor_gap_s", "0")
    return db


def test_dataset_channel():
    tlvs = bytes([14, 8] + [0] * 8 + [0, 3, 0, 0, 20])
    assert cmod.dataset_channel(tlvs) == 20
    assert cmod.dataset_channel(b"\x0e\x08\x00") is None


@pytest.mark.asyncio
async def test_scan_job_votes_and_reports_missing_firmware(tmp_path):
    db = _db(tmp_path)
    r = _Router({"aa" * 8}, _scan_bytes({25: -50}),       # gateway hears ch25 busy
                {"d1" * 8: _scan_bytes({20: -99, 25: -70})})   # s2 has old firmware
    cm = cmod.ChannelManager(db, r)
    job = cm.start_scan()
    await cm._job_task
    assert job["state"] == "done", job["error"]
    voters = {s["voter"]: s for s in job["scans"]}
    assert voters["gw-a"]["dbm"][25] == -50
    assert "no reply" in voters["s2"]["error"]
    assert "dead" * 4 not in r.dev_calls                  # unregistered id never asked
    top = job["ranking"][0]
    assert top["channel"] != 25
    assert job["decision"]["move"] is True        # 25 is vetoed by the gateway
    assert cm.status()["last_job"]["state"] == "done"
    cm2 = cmod.ChannelManager(db, r)          # after a hub restart
    assert cm2.get_job(job["id"])["decision"] == job["decision"]


@pytest.mark.asyncio
async def test_migrate_confirms_and_stores_dataset(tmp_path, monkeypatch):
    db = _db(tmp_path)
    tlvs = bytes([0, 3, 0, 0, 20, 14, 8] + [0] * 8)
    r = _Router({"aa" * 8}, b"", {}, dataset_after=tlvs)
    cm = cmod.ChannelManager(db, r)
    db.set_setting("radio.channel.delay_s", "60")
    with pytest.raises(ValueError):
        await cm.migrate(25)                      # already there
    dry = await cm.migrate(20, dry_run=True)
    assert dry["dry_run"] and not r.sets
    res = await cm.migrate(20)
    assert r.sets == [("aa" * 8, 20, 60)] and res["channel"] == 20
    with pytest.raises(RuntimeError):
        await cm.migrate(15)                      # one move at a time
    cm._verify_task.cancel()
    pend = cm._state()["pending"]
    pend["effective_at"] = time.time() - 200      # skip the wait
    monkeypatch.setattr(cmod.asyncio, "sleep", _nosleep)
    await cm._verify(pend)
    st = cm.status()
    assert st["channel"] == 20 and st["pending"] is None
    assert st["history"][0]["ok"] is True
    assert net_mod.get_dataset_tlvs(db) == tlvs


async def _nosleep(*a, **k):
    return None


@pytest.mark.asyncio
async def test_auto_cooldown_and_evaluate(tmp_path):
    db = _db(tmp_path)
    now = int(time.time())
    # two reports per device, 30 min apart, busy 50 % of attempts
    for did in ("d1" * 8, "d2" * 8):
        db.add_radio_stats(did, now - 1790, {"uptime_s": 1000, "tx_ok": 100, "tx_cca_busy": 100})
        db.add_radio_stats(did, now - 10, {"uptime_s": 2800, "tx_ok": 200, "tx_cca_busy": 200})
    cm = cmod.ChannelManager(db, _Router(set(), b"", {}))
    assert cm.settings()["auto.threshold_pct"] == 60.0     # default
    assert cm.evaluate(now)["breach"] is False              # 50 % < 60 %
    db.set_setting("radio.channel.auto.threshold_pct", "40")
    ev = cm.evaluate(now)
    assert ev["breach"] and sorted(ev["over"]) == ["s1", "s2"] and ev["values"]["s1"] == 50.0
    db.set_setting("radio.channel.auto.threshold_pct", "60")
    assert cm.evaluate(now)["breach"] is False


@pytest.mark.asyncio
async def test_settings_validation(tmp_path):
    cm = cmod.ChannelManager(_db(tmp_path), _Router(set(), b"", {}))
    assert cm.put_settings({"allowed": "15,20,25", "k": 2})["k"] == 2
    for bad in ({"allowed": "5"}, {"nope": 1}, {"auto.metric": "x"}, {"delay_s": 5}):
        with pytest.raises(ValueError):
            cm.put_settings(bad)


@pytest.mark.asyncio
async def test_auto_loop_scans_once_then_waits(tmp_path, monkeypatch):
    db = _db(tmp_path)
    now = int(time.time())
    for did in ("d1" * 8, "d2" * 8):
        db.add_radio_stats(did, now - 1790, {"uptime_s": 1000, "tx_ok": 100, "tx_cca_busy": 100})
        db.add_radio_stats(did, now - 10, {"uptime_s": 2800, "tx_ok": 200, "tx_cca_busy": 200})
    quiet = _scan_bytes({})
    r = _Router({"aa" * 8}, quiet, {"d1" * 8: quiet, "d2" * 8: quiet})
    cm = cmod.ChannelManager(db, r)
    cm.put_settings({"auto.enabled": 1, "auto.threshold_pct": 40})
    ticks = {"n": 0}

    async def fake_sleep(sec):
        if sec < 60:                  # gaps between sensor scans
            return
        ticks["n"] += 1               # one per auto-check tick
        if ticks["n"] > 3:
            raise asyncio.CancelledError

    monkeypatch.setattr(cmod.asyncio, "sleep", fake_sleep)
    with pytest.raises(asyncio.CancelledError):
        await cm.run()
    st = cm.status()
    assert st["auto"]["breach"] is True
    assert st["auto"]["action"] == "waiting to rescan"     # 2nd+ tick: no new scan
    assert r.sets == []                                    # 25 clear everywhere: stay
    assert st["last_job"]["decision"]["move"] is False
