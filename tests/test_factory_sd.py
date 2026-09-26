"""Factory: whole-disk (micro-SD) flashing.

Pure pieces (disk filter, origin check, helper-output folding) are tested
against an ASSUMED `lsblk -J -b` shape (util-linux 2.38: JSON booleans,
`mountpoints` arrays) plus the older string/`mountpoint` variants, and the
API is tested end-to-end with lsblk + the root helper faked.
"""
from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from hub import api as hub_api
from hub.api import (_disk_candidates, _origin_trusted, _sdflash_feed,
                     _SD_FORMAT, make_app)
from hub.db import DB
from hub.log_store import LogStore


# ── fixtures: the lsblk shape we assume ─────────────────────────────────────

def _disk(name, *, size, rm=False, hotplug=False, tran="usb", model="",
          serial="", children=(), typ="disk", vendor=""):
    return {"name": name, "path": f"/dev/{name}", "size": size, "rm": rm,
            "hotplug": hotplug, "tran": tran, "type": typ, "model": model,
            "vendor": vendor, "serial": serial, "mountpoints": [None],
            "children": list(children)}


def _part(name, *mounts, size=1 << 30):
    return {"name": name, "path": f"/dev/{name}", "size": size, "type": "part",
            "mountpoints": list(mounts) or [None]}


OPI_ROOT = _disk("nvme0n1", size=1024209543168, tran="nvme", model="Lexar SSD",
                 children=[_part("nvme0n1p1", "/boot"), _part("nvme0n1p2", "/")])
READER_WITH_CARD = _disk("sda", size=63 * 10**9, rm=True, model="MassStorageClass",
                         vendor="Generic", serial="000000001234",
                         children=[_part("sda1", "/media/orangepi/BOOT"),
                                   _part("sda3", "/media/orangepi/rootfs")])
READER_EMPTY = _disk("sdb", size=0, rm=True, model="USB  SD Reader")
USB_HOLDING_SYSTEM = _disk("sdc", size=500 * 10**9, rm=True, model="Backup SSD",
                           children=[_part("sdc1", "/srv/backup")])
SD_SLOT = _disk("mmcblk1", size=32 * 10**9, hotplug=True, tran=None)
SATA_FIXED = _disk("sdd", size=2 * 10**12, tran="sata", model="Internal HDD")


def test_disk_candidates_only_removable_usb_or_sd_without_system_mounts():
    out = _disk_candidates({"blockdevices": [OPI_ROOT, READER_WITH_CARD, READER_EMPTY,
                                             USB_HOLDING_SYSTEM, SD_SLOT, SATA_FIXED]})
    assert [d["path"] for d in out] == ["/dev/sda", "/dev/sdb", "/dev/mmcblk1"]
    card = out[0]
    assert card["has_media"] and card["model"] == "MassStorageClass"
    assert card["mounted"] == ["/media/orangepi/BOOT", "/media/orangepi/rootfs"]
    assert card["partitions"] == 2 and card["tran"] == "usb"
    assert out[1]["has_media"] is False
    assert out[2]["tran"] == "mmc"


def test_disk_candidates_never_returns_the_root_disk_even_if_flagged_removable():
    weird_root = dict(OPI_ROOT, rm=True, tran="usb")       # e.g. booted from USB
    assert _disk_candidates({"blockdevices": [weird_root]}) == []


def test_disk_candidates_tolerates_old_lsblk_strings_and_mountpoint_singular():
    old = {"name": "sda", "path": "/dev/sda", "size": "8000000000", "rm": "1",
           "hotplug": "0", "tran": "usb", "type": "disk", "model": "Card Reader",
           "mountpoint": None,
           "children": [{"name": "sda1", "type": "part", "mountpoint": "/media/u/CARD"}]}
    out = _disk_candidates({"blockdevices": [old]})
    assert len(out) == 1 and out[0]["size_bytes"] == 8000000000
    assert out[0]["mounted"] == ["/media/u/CARD"]
    sys_old = dict(old, children=[{"name": "sda1", "type": "part", "mountpoint": "/home"}])
    assert _disk_candidates({"blockdevices": [sys_old]}) == []


def test_disk_candidates_empty_and_partitions_ignored():
    assert _disk_candidates({}) == []
    assert _disk_candidates({"blockdevices": [_part("sda1", "/media/x")]}) == []


def test_origin_trusted_loopback_or_bearer_only():
    assert _origin_trusted(False, "127.0.0.1")
    assert _origin_trusted(False, "::1")
    assert _origin_trusted(False, "::ffff:127.0.0.1")
    assert not _origin_trusted(False, "192.0.2.10")      # the open device port
    assert not _origin_trusted(False, "")
    assert _origin_trusted(True, "192.0.2.10")           # bearer already verified


def test_sdflash_feed_parses_dd_progress_phases_and_result():
    job = {"log": "", "progress": {"bytes": 0, "total": 100, "phase": "starting"}}
    _sdflash_feed(job, b"PHASE unmount\nPHASE write\n")
    assert job["progress"]["phase"] == "write"
    # dd separates progress lines with \r, not \n
    _sdflash_feed(job, b"4194304 bytes (4.2 MB, 4.0 MiB) copied, 1 s, 4.2 MB/s\r"
                       b"8388608 bytes (8.4 MB, 8.0 MiB) copied, 2 s, 4.2 MB/s\r")
    assert job["progress"]["bytes"] == 8388608
    assert "8.0 MiB" in job["progress"]["line"]
    _sdflash_feed(job, b"PHASE verify\nNN_SDFLASH_RESULT "
                       b'{"ok":true,"verified":true,"raw_sha256":"ab"}\n')
    assert job["result"] == {"ok": True, "verified": True, "raw_sha256": "ab"}
    assert "[verify]" in job["log"]


def test_sdflash_feed_bad_result_json_becomes_error():
    job = {"log": "", "progress": {"bytes": 0, "total": 0, "phase": ""}}
    _sdflash_feed(job, b"NN_SDFLASH_RESULT {not json")
    assert job["result"]["ok"] is False


# ── image validation knows the card format, and streams big files ───────────

def test_verify_image_accepts_xz_card_image_and_rejects_wrong_magic(tmp_path):
    import hashlib
    from hub.firmware_sources.base import (ManifestError, verify_image_bytes,
                                           verify_image_file, XZ_MAGIC)
    blob = XZ_MAGIC + b"\x00" * 100
    sha = hashlib.sha256(blob).hexdigest()
    verify_image_bytes(blob, sha, len(blob), _SD_FORMAT)          # ok
    p = tmp_path / "image.signed.bin"
    p.write_bytes(blob)
    verify_image_file(p, sha, len(blob), _SD_FORMAT)               # streaming twin ok
    with pytest.raises(ManifestError, match="not xz"):
        verify_image_bytes(b"\x1f\x8b" + blob[2:], hashlib.sha256(b"\x1f\x8b" + blob[2:]).hexdigest(),
                           len(blob), _SD_FORMAT)
    # the A/B system slot image (byai_system) is the same xz stream shape
    verify_image_bytes(blob, sha, len(blob), "rootfs.ext4.xz")
    with pytest.raises(ManifestError, match="not xz"):
        verify_image_bytes(b"\x1f\x8b" + blob[2:], hashlib.sha256(b"\x1f\x8b" + blob[2:]).hexdigest(),
                           len(blob), "rootfs.ext4.xz")
    with pytest.raises(ManifestError, match="MCUboot"):            # default format still strict
        verify_image_file(p, sha, len(blob))
    with pytest.raises(ManifestError, match="size mismatch"):
        verify_image_file(p, sha, len(blob) + 1, _SD_FORMAT)
    with pytest.raises(ManifestError, match="sha256 mismatch"):
        verify_image_file(p, "0" * 64, len(blob), _SD_FORMAT)


@pytest.mark.asyncio
async def test_local_source_caches_card_image_by_streaming(tmp_path, monkeypatch):
    """A local byai_sdcard release is discovered and cached without read_bytes()."""
    import hashlib
    from hub.firmware_sources.base import XZ_MAGIC
    from hub.firmware_sources.local import LocalDirSource
    blob = XZ_MAGIC + bytes(range(256)) * 64
    d = tmp_path / "src" / "byai_sdcard" / "1"
    d.mkdir(parents=True)
    (d / "image.signed.bin").write_bytes(blob)
    (d / "manifest.json").write_text(json.dumps(dict(
        SD_MANIFEST, version="1", sha256=hashlib.sha256(blob).hexdigest(), size_bytes=len(blob))))
    src = LocalDirSource("t", tmp_path / "src")
    descs = await src.list_available()
    assert [(x.device_type, x.version) for x in descs] == [("byai_sdcard", "1")]
    monkeypatch.setattr(Path, "read_bytes", lambda self: (_ for _ in ()).throw(AssertionError("read_bytes used")))
    out = await src.fetch(descs[0], tmp_path / "cache" / "img.bin")
    assert out == tmp_path / "cache" / "img.bin"
    assert out.stat().st_size == len(blob)
    assert not (tmp_path / "cache" / "img.bin.tmp").exists()


# ── API: lsblk + helper faked ───────────────────────────────────────────────

SD_MANIFEST = {"schema": 1, "device_type": "byai_sdcard", "version": "20260721",
               "format": _SD_FORMAT, "sha256": "0" * 64, "size_bytes": 4760494100,
               "raw_size_bytes": 31415336960, "raw_sha256": "f" * 64,
               "mcuboot": {}, "build_meta": {}}
FW_MANIFEST = {"schema": 1, "device_type": "nn-app-ncp-esp32c6", "version": "0.0.9",
               "sha256": "1" * 64, "size_bytes": 10, "mcuboot": {}, "build_meta": {}}


class _FakeCatalog:
    _cache_dir = "/nonexistent"

    def __init__(self, img: Path):
        self.img = img
        self.calls = []

    async def ensure_cached(self, dtype, version, source):
        self.calls.append((dtype, version, source))
        return self.img


@pytest_asyncio.fixture
async def sd_env(tmp_path: Path, monkeypatch):
    db = DB(tmp_path / "h.db")
    db.upsert_catalog_entry(device_type="byai_sdcard", version="20260721",
                            source_name="bench-local", asset_uri=str(tmp_path / "image.signed.bin"),
                            manifest_json=json.dumps(SD_MANIFEST), sha256="0" * 64,
                            size_bytes=4760494100)
    db.upsert_catalog_entry(device_type="nn-app-ncp-esp32c6", version="0.0.9",
                            source_name="bench-local", asset_uri=str(tmp_path / "ncp.bin"),
                            manifest_json=json.dumps(FW_MANIFEST), sha256="1" * 64,
                            size_bytes=10)
    img = tmp_path / "image.signed.bin"
    img.write_bytes(b"xz")
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    app = make_app(db, LogStore(tmp_path / "logs", rotate_interval_sec=3600),
                   X25519PrivateKey.generate(), auth_token=None)
    cat = _FakeCatalog(img)
    app["firmware_catalog"] = cat

    state = {"lsblk": {"blockdevices": [OPI_ROOT, READER_WITH_CARD, READER_EMPTY]},
             "trusted": True}
    monkeypatch.setattr(hub_api, "_lsblk_json", lambda: state["lsblk"])
    monkeypatch.setattr(hub_api, "_request_trusted", lambda req: state["trusted"])
    # fake root helper: records its argv, emits progress like the real one
    helper = tmp_path / "nn-sdflash"
    helper.write_text("#!/bin/bash\necho \"$@\" > " + str(tmp_path / "argv") + "\n"
                      "echo PHASE write\n"
                      "echo '1000 bytes (1.0 kB) copied, 1 s, 1 kB/s' >&2\n"
                      "echo PHASE verify\n"
                      "echo 'NN_SDFLASH_RESULT {\"ok\":true,\"verified\":true}'\n")
    helper.chmod(helper.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setattr(hub_api, "_SDFLASH_HELPER", str(helper))
    # 'sudo -n <helper>' → run the fake directly (no sudo in CI)
    real_exec = hub_api.asyncio.create_subprocess_exec

    async def fake_exec(*argv, **kw):
        assert argv[:2] == ("sudo", "-n"), argv
        return await real_exec(*argv[2:], **kw)
    monkeypatch.setattr(hub_api.asyncio, "create_subprocess_exec", fake_exec)
    hub_api._flash_jobs.clear()
    async with TestClient(TestServer(app)) as cli:
        yield cli, state, cat, tmp_path


@pytest.mark.asyncio
async def test_catalog_marks_card_images_flashable_with_target(sd_env):
    cli, *_ = sd_env
    rows = await (await cli.get("/api/v1/firmware/catalog")).json()
    by = {r["device_type"]: r for r in rows}
    assert by["byai_sdcard"]["flashable"] is True
    assert by["byai_sdcard"]["flash_target"] == "sdcard"
    assert by["byai_sdcard"]["format"] == _SD_FORMAT
    assert by["nn-app-ncp-esp32c6"]["flash_target"] == "serial"
    assert by["nn-app-ncp-esp32c6"]["flashable"] is False     # no flash.sh beside it


@pytest.mark.asyncio
async def test_disks_lists_only_candidates(sd_env):
    cli, state, *_ = sd_env
    j = await (await cli.get("/api/v1/factory/disks")).json()
    assert [d["path"] for d in j["disks"]] == ["/dev/sda", "/dev/sdb"]
    assert j["helper_installed"] is True and j["trusted_origin"] is True


@pytest.mark.asyncio
async def test_flash_disk_refused_from_untrusted_origin(sd_env):
    cli, state, *_ = sd_env
    state["trusted"] = False
    r = await cli.post("/api/v1/factory/flash-disk", json={
        "disk": "/dev/sda", "device_type": "byai_sdcard", "version": "20260721"})
    assert r.status == 403
    assert "authenticated" in (await r.json())["err"]


@pytest.mark.asyncio
async def test_flash_disk_refuses_bad_targets(sd_env):
    cli, state, *_ = sd_env
    body = {"device_type": "byai_sdcard", "version": "20260721"}
    r = await cli.post("/api/v1/factory/flash-disk", json=dict(body, disk="/dev/nvme0n1"))
    assert r.status == 409 and "refusing" in (await r.json())["err"]
    r = await cli.post("/api/v1/factory/flash-disk", json=dict(body, disk="/dev/sdb"))
    assert r.status == 409 and "no card" in (await r.json())["err"]
    r = await cli.post("/api/v1/factory/flash-disk", json=dict(body, disk="/dev/sda",
                                                                device_type="nn-app-ncp-esp32c6",
                                                                version="0.0.9"))
    assert r.status == 422 and "not a card image" in (await r.json())["err"]
    small = dict(READER_WITH_CARD, size=16 * 10**9)
    state["lsblk"] = {"blockdevices": [small]}
    r = await cli.post("/api/v1/factory/flash-disk", json=dict(body, disk="/dev/sda"))
    assert r.status == 409 and "too small" in (await r.json())["err"]
    r = await cli.post("/api/v1/factory/flash-disk", json=dict(body, disk="/dev/sda",
                                                                version="nope"))
    assert r.status == 404


@pytest.mark.asyncio
async def test_flash_disk_runs_helper_with_manifest_facts_and_tracks_progress(sd_env):
    cli, state, cat, tmp = sd_env
    r = await cli.post("/api/v1/factory/flash-disk", json={
        "disk": "/dev/sda", "device_type": "byai_sdcard", "version": "20260721"})
    assert r.status == 200, await r.text()
    jid = (await r.json())["id"]
    for _ in range(50):
        job = await (await cli.get(f"/api/v1/factory/flash/{jid}")).json()
        if job["state"] != "running":
            break
        await hub_api.asyncio.sleep(0.05)
    assert job["state"] == "done", job
    assert job["kind"] == "sdcard" and job["disk"] == "/dev/sda"
    assert job["result"] == {"ok": True, "verified": True}
    assert job["progress"]["phase"] == "done"
    assert job["progress"]["total"] == 31415336960
    argv = (tmp / "argv").read_text().split()
    assert argv[:3] == ["write", "/dev/sda", str(tmp / "image.signed.bin")]
    assert argv[3:] == ["--raw-size", "31415336960", "--raw-sha256", "f" * 64]
    assert cat.calls == [("byai_sdcard", "20260721", "bench-local")]
    # a second write to the same disk while one runs is refused
    hub_api._flash_jobs[jid]["state"] = "running"
    r = await cli.post("/api/v1/factory/flash-disk", json={
        "disk": "/dev/sda", "device_type": "byai_sdcard", "version": "20260721"})
    assert r.status == 409 and "already" in (await r.json())["err"]


@pytest.mark.asyncio
async def test_flash_disk_helper_missing_is_503(sd_env, monkeypatch):
    cli, *_ = sd_env
    monkeypatch.setattr(hub_api, "_SDFLASH_HELPER", "/nonexistent/nn-sdflash")
    r = await cli.post("/api/v1/factory/flash-disk", json={
        "disk": "/dev/sda", "device_type": "byai_sdcard", "version": "20260721"})
    assert r.status == 503 and "install-sdflash" in (await r.json())["err"]


# ── retiring a catalog entry ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_delete_catalog_entry_removes_rows_and_cache(sd_env, tmp_path):
    cli, state, cat, tmp = sd_env
    db = cli.server.app["db"]
    cached = tmp / "cache-byai.bin"; cached.write_bytes(b"x")
    db._conn.execute("UPDATE firmware_catalog SET local_path=?, downloaded_at=1 "
                     "WHERE device_type='byai_sdcard'", (str(cached),)); db._conn.commit()
    r = await cli.delete("/api/v1/firmware/catalog/byai_sdcard/20260721")
    assert r.status == 200, await r.text()
    j = await r.json()
    assert j["rows_removed"] == 1 and j["cache_removed"] == [str(cached)]
    assert not cached.exists()
    rows = await (await cli.get("/api/v1/firmware/catalog?device_type=byai_sdcard")).json()
    assert rows == []
    r = await cli.delete("/api/v1/firmware/catalog/byai_sdcard/20260721")
    assert r.status == 404


@pytest.mark.asyncio
async def test_delete_catalog_entry_refuses_active_target_unless_forced(sd_env):
    cli, *_ = sd_env
    db = cli.server.app["db"]
    db.set_firmware_target("nn-app-ncp-esp32c6", "0.0.9", "/x/ncp.bin", 10, "1" * 64)
    r = await cli.delete("/api/v1/firmware/catalog/nn-app-ncp-esp32c6/0.0.9")
    assert r.status == 409 and "active OTA target" in (await r.json())["err"]
    r = await cli.delete("/api/v1/firmware/catalog/nn-app-ncp-esp32c6/0.0.9?force=1")
    assert r.status == 200 and (await r.json())["was_active_target"] is True


# ── a brand-new camera slot becomes a free (awaiting-device) slot ───────────

@pytest.mark.asyncio
async def test_camera_free_marks_new_slot_awaiting_device(sd_env, monkeypatch):
    cli, *_ = sd_env
    db = cli.server.app["db"]
    from hub import cameras as _cm
    class _C:  # what a video service's registration looks like to load_cameras
        def __init__(self, i): self.id = i; self.name = i; self.url = "http://127.0.0.1:8904"
    monkeypatch.setattr(_cm, "load_cameras",
                        lambda db=None, include_blocked=False: [_C("cam4")])
    r = await cli.post("/api/v1/cameras/cam4/free")
    assert r.status == 200 and (await r.json())["free"] == "cam4"
    assert json.loads(db.get_setting("cameras_blocked")) == ["cam4"]
    r = await cli.post("/api/v1/cameras/cam4/free")
    assert r.status == 409
    r = await cli.post("/api/v1/cameras/nope/free")
    assert r.status == 404
    # adopt re-admits it, as after an unregister
    r = await cli.post("/api/v1/cameras/cam4/adopt")
    assert r.status == 200 and json.loads(db.get_setting("cameras_blocked")) == []


def test_load_cameras_hides_blocked_self_registered_slot(tmp_path):
    """A blocked id must not appear live even when its video service keeps
    registering it (DB row) — the env-only filter missed this."""
    from hub import cameras as _cm
    db = DB(tmp_path / "c.db")
    db.upsert_camera("cam4", "BeagleY cam4", "http://127.0.0.1:8904", "{}")
    assert "cam4" in [c.id for c in _cm.load_cameras(db)]
    db.set_setting("cameras_blocked", json.dumps(["cam4"]))
    assert "cam4" not in [c.id for c in _cm.load_cameras(db)]          # hidden
    assert "cam4" in [c.id for c in _cm.load_cameras(db, include_blocked=True)]


# ── Linux camera classification by FW_NAME prefix ───────────────────────────

def test_camera_platform_and_kind_alias():
    from hub.api import _camera_platform, _normalize_prov_kind
    assert _camera_platform("nn-camera-byai") == "linux"
    assert _camera_platform("NN-Camera-BYAI") == "linux"
    assert _camera_platform("nn-app-camera-esp32p4-wifi6-sdio-ov5647") == "esp"
    assert _camera_platform("") == "esp"
    assert _normalize_prov_kind("camera_linux") == ("camera_esp", "linux")
    assert _normalize_prov_kind("camera_esp") == ("camera_esp", "esp")
    assert _normalize_prov_kind("sensor") == ("sensor", "")


# ── unregister keeps the SLOT, forgets the DEVICE (2026-09-14) ───────────────
#
# A self-registered slot's video-service URL lives only in its cameras row, and
# the service's heartbeat cannot recreate that row while the id is blocked.
# Deleting the row on unregister therefore made the next provision into the
# same slot fail with "'cam3' is not a camera slot", and leaving camera_addr#
# behind routed a wiped board straight back to its old slot.

@pytest.mark.asyncio
async def test_unregister_keeps_slot_row_and_clears_device_binding(sd_env, monkeypatch):
    cli, *_ = sd_env
    db = cli.server.app["db"]
    from hub import cameras as _cm
    from hub.api import _slot_for_addr, _free_camera_slots
    db.upsert_camera("cam3", "BeagleY", "http://127.0.0.1:8903", "{}")
    db.set_setting("camera_addr#cam3", "10:CA:BF:DA:35:B7")
    db.set_setting("camera_name#cam3", "cam3 label")
    db.set_setting("camera_bundle:cam3", "0.1.1")
    assert _slot_for_addr(db, "10:ca:bf:da:35:b7") == "cam3"
    r = await cli.post("/api/v1/cameras/cam3/unregister", json={"force": True})
    assert r.status == 200, await r.text()
    rows = {row["id"]: row for row in db.list_cameras()}
    assert "cam3" in rows and rows["cam3"]["url"] == "http://127.0.0.1:8903"   # slot plumbing kept
    assert "cam3" not in [c.id for c in _cm.load_cameras(db)]                   # hidden while blocked
    assert "cam3" in _free_camera_slots(db)                                      # offered to the wizard
    assert _slot_for_addr(db, "10:ca:bf:da:35:b7") == ""                        # board no longer owns it
    assert not db.get_setting("camera_bundle:cam3") and not db.get_setting("camera_name#cam3")


def test_free_slot_forgets_previous_board(tmp_path):
    """Marking a slot 'awaiting a device' must also drop its board binding."""
    from hub.db import DB as _DB
    db = _DB(tmp_path / "f.db")
    db.set_setting("camera_addr#cam4", "AA:BB:CC:DD:EE:FF")
    db.set_setting("camera_bundle:cam4", "0.1.1")
    from hub.api import _slot_for_addr, _forget_slot_device
    _forget_slot_device(db, "cam4")             # what /free and /unregister call
    assert _slot_for_addr(db, "aa:bb:cc:dd:ee:ff") == ""
    assert db.get_setting("camera_addr#cam4", None) is None   # deleted, not blanked
    assert db.get_setting("camera_bundle:cam4", None) is None


def test_stale_binding_on_unregistered_slot_never_routes(tmp_path):
    """Data written BEFORE unregister cleared bindings: cam4 (unregistered,
    free) still names the BeagleY that is now cam3.  Provisioning that board
    with no target_cam must not be routed back into cam4 (seen 2026-09-14)."""
    from hub.db import DB as _DB
    from hub.api import (_slot_for_addr, _set_blocked_cameras,
                         _sweep_stale_slot_devices)
    db = _DB(tmp_path / "s.db")
    db.upsert_camera("cam3", "BeagleY", "http://127.0.0.1:8903", "{}")
    db.upsert_camera("cam4", "cam4", "http://127.0.0.1:8904", "{}")
    db.set_setting("camera_addr#cam4", "10:CA:BF:DA:35:B7")   # stale
    db.set_setting("camera_bundle:cam4", "0.1.0")
    _set_blocked_cameras(db, {"cam4"})
    assert _slot_for_addr(db, "10:ca:bf:da:35:b7") == ""      # blocked slot never routes
    db.set_setting("camera_addr#cam3", "10:CA:BF:DA:35:B7")   # live slot does
    assert _slot_for_addr(db, "10:ca:bf:da:35:b7") == "cam3"
    # the startup sweep removes the stale keys and leaves live slots alone
    assert _sweep_stale_slot_devices(db) == ["cam4"]
    assert db.get_setting("camera_addr#cam4", None) is None
    assert db.get_setting("camera_bundle:cam4", None) is None
    assert db.get_setting("camera_addr#cam3") == "10:CA:BF:DA:35:B7"
    assert _sweep_stale_slot_devices(db) == []                # idempotent


@pytest.mark.asyncio
async def test_unregister_reports_video_service_reason(sd_env, monkeypatch):
    """A Linux board with no camera app is 'already silent'; the old error
    blamed firmware that predates CTRL_CLEAR for every unconfirmed clear."""
    from aiohttp import web
    cli, *_ = sd_env
    db = cli.server.app["db"]

    async def clear(_req):
        return web.json_response({"ok": True, "sent": "clear_user_data",
                                  "applied": False,
                                  "reason": "device was already silent before "
                                            "the command — cannot confirm it "
                                            "was received"})
    vs = web.Application(); vs.router.add_post("/api/camera/clear", clear)
    runner = web.AppRunner(vs); await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0); await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        db.upsert_camera("cam3", "BeagleY", f"http://127.0.0.1:{port}", "{}")
        r = await cli.post("/api/v1/cameras/cam3/unregister", json={})
        body = await r.json()
        assert r.status == 409, body
        assert "already silent" in body["err"] and "camera app" in body["err"]
        assert not body["err"].startswith("camera firmware ignored")   # not the old headline
        assert "cam3" not in __import__("hub.api", fromlist=["x"])._blocked_cameras(db)
    finally:
        await runner.cleanup()


def test_unresolvable_slot_url_error_is_honest():
    """The old text claimed the slot did not exist while listing it as free."""
    import inspect
    from hub import api as hub_api
    msg = hub_api._MSG_SLOT_NO_URL.format(slot="cam3")
    assert "exists but has no video-service URL" in msg and "target_url" in msg
    src = inspect.getsource(hub_api._prov_job_run)
    assert "_MSG_SLOT_NO_URL" in src            # the runner uses it for a known slot
    assert "is not a camera slot" in src        # and keeps the old text for unknown names


# ── /gw_firmware/<type>/<ver>/meta carries the manifest's agent gates ─────────

@pytest.mark.asyncio
async def test_firmware_meta_serves_manifest_extras(tmp_path):
    """nn-sysupd reads raw_sha256 / requires from /meta before it writes a
    slot; the meta endpoint must therefore expose the promoted entry's
    manifest fields, not only the byte digest."""
    import json, hashlib
    from aiohttp.test_utils import TestClient, TestServer
    from hub.db import DB as _DB
    from hub import firmware_http
    db = _DB(tmp_path / "m.db")
    blob = b"\xfd7zXZ\x00" + b"\x00" * 64
    f = tmp_path / "image.signed.bin"; f.write_bytes(blob)
    sha = hashlib.sha256(blob).hexdigest()
    man = {"device_type": "byai_system", "version": "20260915-1", "sha256": sha,
           "size_bytes": len(blob), "format": "rootfs.ext4.xz",
           "raw_size_bytes": 1572864000, "raw_sha256": "ab" * 32,
           "requires": {"layout": "ab-v1", "boot_chain_min": "2026.01"},
           "kernel": "6.12.57-ti-arm64-r64bt", "git": "a0855a7"}
    db.upsert_catalog_entry("byai_system", "20260915-1", "local", str(f),
                            json.dumps(man), sha, len(blob))
    db.set_firmware_target("byai_system", "20260915-1", str(f), len(blob), sha)
    async with TestClient(TestServer(firmware_http.make_app(db))) as cli:
        r = await cli.get("/gw_firmware/byai_system/20260915-1/meta")
        m = await r.json()
        assert r.status == 200
        assert m["sha256"] == sha and m["format"] == "rootfs.ext4.xz"
        assert m["raw_sha256"] == "ab" * 32 and m["raw_size_bytes"] == 1572864000
        assert m["requires"] == {"layout": "ab-v1", "boot_chain_min": "2026.01"}
        assert m["kernel"].startswith("6.12.57")
        # a target uploaded without a catalog entry still answers, minimally
        db.set_firmware_target("other", "1", str(f), len(blob), sha)
        m2 = await (await cli.get("/gw_firmware/other/1/meta")).json()
        assert set(m2) == {"type", "version", "size", "sha256"}


def test_implicit_default_camera_yields_to_registered_cameras(tmp_path, monkeypatch):
    """The built-in cam0 fallback (no NN_CAMERAS, no NN_VIDEO_SERVICE_URL)
    shows on a fresh box, but must not shadow rows the media service
    registered — with the pipelines service, cam0 pinned to a dead :8899
    hid the real cam0 at :8880/cam/cam0."""
    import hub.cameras as _cm
    from hub.db import DB
    monkeypatch.delenv("NN_CAMERAS", raising=False); monkeypatch.delenv("NN_VIDEO_SERVICE_URL", raising=False)
    db = DB(tmp_path / "h.db")
    ids = [c.id for c in _cm.load_cameras(db)]
    assert ids == ["cam0"] and _cm.load_cameras(db)[0].url.endswith(":8899"), "fresh box: the fallback"
    db.upsert_camera("cam0", "Camera 1", "http://127.0.0.1:8880/cam/cam0", "{}")
    cams = _cm.load_cameras(db)
    assert [c.id for c in cams] == ["cam0"] and cams[0].url == "http://127.0.0.1:8880/cam/cam0", "registered row wins"
    db.upsert_camera("cam3", "cam3", "http://127.0.0.1:8880/cam/cam3", "{}")
    assert sorted(c.id for c in _cm.load_cameras(db)) == ["cam0", "cam3"]
    monkeypatch.setenv("NN_VIDEO_SERVICE_URL", "http://127.0.0.1:9999")
    assert _cm.load_cameras(db)[0].url == "http://127.0.0.1:9999", "an explicit pin is still authoritative"
