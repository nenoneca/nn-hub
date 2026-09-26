"""
SQLite database: device registry, per-device configs, firmware targets, event log.

All writes are synchronous (sqlite3 is not async-safe with asyncio without
care, so we keep DB on the main thread and call it from the server via
run_in_executor when needed — or just accept the brief block for now since
all writes are tiny).
"""

from __future__ import annotations
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


SCHEMA = """
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS provision_info (
    device_id      TEXT    PRIMARY KEY,
    device_type    TEXT    NOT NULL,     -- "sample_c6" | "esp_tbr" | ...
    enc_pubkey_b64 TEXT    NOT NULL DEFAULT '',  -- device X25519 pub (base64)
    mdns_addr      TEXT    NOT NULL DEFAULT '',  -- "<name>.local"
    ml_eid         TEXT    NOT NULL DEFAULT '',  -- device current Thread ML-EID for D2D
    gateway_id     TEXT    NOT NULL DEFAULT '',  -- associated border-router device_id
    ble_addr       TEXT    NOT NULL DEFAULT '',  -- BLE MAC from provisioning
    eui64          TEXT    NOT NULL DEFAULT '',  -- Thread EUI-64 (16 hex chars)
    capabilities   TEXT    NOT NULL DEFAULT '[]', -- JSON array of field descriptors
    provisioned_at INTEGER NOT NULL,
    last_info_at   INTEGER              -- timestamp of last successful GET_INFO
);

CREATE TABLE IF NOT EXISTS telemetry (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT    NOT NULL,
    ts        INTEGER NOT NULL,
    payload   TEXT    NOT NULL   -- JSON blob
);

CREATE INDEX IF NOT EXISTS telemetry_device_ts
    ON telemetry (device_id, ts DESC);

CREATE TABLE IF NOT EXISTS devices (
    id            TEXT    PRIMARY KEY,
    name          TEXT    NOT NULL DEFAULT '',
    type          TEXT    NOT NULL,       -- 'end_device' | 'gateway'
    pubkey_b64    TEXT    NOT NULL,
    registered_at INTEGER NOT NULL,
    last_seen     INTEGER
);

CREATE TABLE IF NOT EXISTS configs (
    device_id  TEXT    PRIMARY KEY,
    version    INTEGER NOT NULL DEFAULT 1,
    payload    TEXT    NOT NULL DEFAULT '{}',   -- JSON blob
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS firmware_targets (
    device_type    TEXT    PRIMARY KEY,
    target_version TEXT    NOT NULL,
    firmware_path  TEXT    NOT NULL,
    size_bytes     INTEGER NOT NULL DEFAULT 0,
    sha256         TEXT    NOT NULL DEFAULT '',
    updated_at     INTEGER NOT NULL
);

-- One row per (device_type, version, source) discovered by a FirmwareSource.
-- `firmware_targets` references one row from here per device_type as the
-- active OTA target.
CREATE TABLE IF NOT EXISTS firmware_catalog (
    device_type    TEXT    NOT NULL,
    version        TEXT    NOT NULL,
    source_name    TEXT    NOT NULL,
    asset_uri      TEXT    NOT NULL,
    manifest_json  TEXT    NOT NULL DEFAULT '{}',
    sha256         TEXT    NOT NULL DEFAULT '',
    size_bytes     INTEGER NOT NULL DEFAULT 0,
    downloaded_at  INTEGER,            -- null until fetch + verify succeed
    local_path     TEXT,               -- null until cached locally
    first_seen_at  INTEGER NOT NULL,
    last_seen_at   INTEGER NOT NULL,
    PRIMARY KEY (device_type, version, source_name)
);

CREATE INDEX IF NOT EXISTS firmware_catalog_device_type
    ON firmware_catalog (device_type);

CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT,
    type      TEXT    NOT NULL,
    detail    TEXT    NOT NULL DEFAULT '',
    ts        INTEGER NOT NULL
);

-- Gateways are first-class brokers between hub and devices over the
-- nn_proto wire format.  Each gateway registers a P-256 (secp256r1)
-- public key with the hub; the hub uses it to verify outer-frame
-- signatures and to authorize TCP connections.
CREATE TABLE IF NOT EXISTS gateways (
    id            TEXT    PRIMARY KEY,                   -- 8B hex (= SHA256(pubkey)[:8])
    name          TEXT    NOT NULL DEFAULT '',
    pubkey_b64    TEXT    NOT NULL,                       -- 65B uncompressed P-256, base64
    mdns_addr     TEXT    NOT NULL DEFAULT '',            -- "<name>.local"
    registered_at INTEGER NOT NULL,
    last_seen     INTEGER,
    -- Phase-2 Thread state mirror, populated by D2G GATEWAY_THREAD_STATE.
    role                  INTEGER NOT NULL DEFAULT 0,    -- spinel net role: 0..4
    rloc16                INTEGER NOT NULL DEFAULT 0,    -- 16-bit
    mleid_hex             TEXT    NOT NULL DEFAULT '',   -- 32 hex chars (16 B IPv6)
    last_thread_state_at  INTEGER
);

-- Small key/value store for hub-wide runtime settings (e.g. the webapp
-- client-debug flag).  Values are opaque TEXT — JSON when structured.
CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Self-registered cameras.  A video_service POSTs /api/v1/cameras at
-- startup and on a heartbeat; the hub proxies its endpoints and shows it
-- on /devices.  Static NN_CAMERAS entries are merged on top of these
-- (env always wins on id collision) — see hub/cameras.py.
CREATE TABLE IF NOT EXISTS cameras (
    id            TEXT    PRIMARY KEY,
    name          TEXT    NOT NULL DEFAULT '',
    url           TEXT    NOT NULL,
    caps_json     TEXT    NOT NULL DEFAULT '{}',   -- e.g. {"infer": {...}}
    policy_json   TEXT    NOT NULL DEFAULT '{}',   -- inference policy (see
                                                   -- INFERENCE_POLICY_DESIGN)
    registered_at INTEGER NOT NULL,
    last_seen     INTEGER NOT NULL
);

-- The hub manages a single Thread (OT) network shared by all
-- registered gateways and devices.  Generated automatically on first
-- gateway provisioning.  See hub/network.py.
CREATE TABLE IF NOT EXISTS network (
    id                    INTEGER PRIMARY KEY CHECK (id = 1),
    channel               INTEGER NOT NULL,
    panid                 INTEGER NOT NULL,                  -- 16-bit
    extpanid_hex          TEXT    NOT NULL,                  -- 16 hex chars (8 B)
    network_name          TEXT    NOT NULL,
    network_key_hex       TEXT    NOT NULL,                  -- 32 hex chars (16 B)
    mesh_local_prefix_hex TEXT    NOT NULL,                  -- 16 hex chars (8 B = /64)
    pskc_hex              TEXT    NOT NULL,
    dataset_tlvs_hex      TEXT    NOT NULL,                  -- canonical OT TLV blob
    created_at            INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS archived_devices (
    -- Unregistered devices, kept as a permanent historical record (the
    -- device erases its identity key on clear, so a re-provisioned unit
    -- returns under a NEW id and never collides with its archive entry).
    device_id     TEXT    PRIMARY KEY,
    name          TEXT    NOT NULL,
    device_type   TEXT    NOT NULL DEFAULT '',
    archived_at   INTEGER NOT NULL,
    reason        TEXT    NOT NULL DEFAULT 'unregister',
    snapshot_json TEXT    NOT NULL DEFAULT '{}'
);
"""


@dataclass
class Device:
    id: str
    name: str
    type: str
    pubkey_b64: str
    registered_at: int
    last_seen: Optional[int]


@dataclass
class Config:
    device_id: str
    version: int
    payload: dict
    updated_at: int


@dataclass
class ProvisionInfo:
    device_id: str
    device_type: str
    enc_pubkey_b64: str
    mdns_addr: str
    ml_eid: str
    gateway_id: str
    ble_addr: str
    eui64: str
    capabilities: str   # JSON array of field descriptors
    provisioned_at: int
    last_info_at: Optional[int]


@dataclass
class FirmwareTarget:
    device_type: str
    target_version: str
    firmware_path: str
    size_bytes: int
    sha256: str
    updated_at: int


@dataclass
class CatalogEntry:
    device_type: str
    version: str
    source_name: str
    asset_uri: str
    manifest_json: str
    sha256: str
    size_bytes: int
    downloaded_at: Optional[int]
    local_path: Optional[str]
    first_seen_at: int
    last_seen_at: int

    @property
    def is_cached(self) -> bool:
        return self.downloaded_at is not None and self.local_path is not None


@dataclass
class Gateway:
    id: str                # 8B hex
    name: str
    pubkey_b64: str        # 65B uncompressed P-256, base64
    mdns_addr: str
    registered_at: int
    last_seen: Optional[int]
    role: int = 0                            # spinel net role 0..4
    rloc16: int = 0
    mleid_hex: str = ""
    last_thread_state_at: Optional[int] = None


# Spinel net-role names (keep in sync with device/apps/.../spinel.h).
ROLE_NAMES = {
    0: "detached",
    1: "child",
    2: "router",
    3: "leader",
    4: "disabled",
}


def role_name(role: int) -> str:
    return ROLE_NAMES.get(role, f"role-{role}")


class DB:
    def __init__(self, path: Path):
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._migrate()
        self._conn.commit()

    def _migrate(self) -> None:
        """Idempotent column additions for existing hub.db files predating
        the current schema.  Each addition is guarded by a column-name
        check so this is safe to call on fresh DBs too."""
        cols = {
            row[1] for row in self._conn.execute(
                "PRAGMA table_info(gateways)"
            ).fetchall()
        }
        if "role" not in cols:
            self._conn.execute(
                "ALTER TABLE gateways ADD COLUMN role INTEGER NOT NULL DEFAULT 0"
            )
        if "rloc16" not in cols:
            self._conn.execute(
                "ALTER TABLE gateways ADD COLUMN rloc16 INTEGER NOT NULL DEFAULT 0"
            )
        if "mleid_hex" not in cols:
            self._conn.execute(
                "ALTER TABLE gateways ADD COLUMN mleid_hex TEXT NOT NULL DEFAULT ''"
            )
        if "last_thread_state_at" not in cols:
            self._conn.execute(
                "ALTER TABLE gateways ADD COLUMN last_thread_state_at INTEGER"
            )

        cam_cols = {
            row[1] for row in self._conn.execute(
                "PRAGMA table_info(cameras)"
            ).fetchall()
        }
        if cam_cols and "policy_json" not in cam_cols:
            self._conn.execute(
                "ALTER TABLE cameras ADD COLUMN policy_json TEXT NOT NULL"
                " DEFAULT '{}'"
            )

        pi_cols = {
            row[1] for row in self._conn.execute(
                "PRAGMA table_info(provision_info)"
            ).fetchall()
        }
        if "coap_addr" in pi_cols and "ml_eid" not in pi_cols:
            self._conn.execute(
                "ALTER TABLE provision_info RENAME COLUMN coap_addr TO ml_eid"
            )

    # ── devices ──────────────────────────────────────────────────────────────

    def register_device(self, id: str, name: str, type: str,
                        pubkey_b64: str) -> Device:
        now = int(time.time())
        self._conn.execute(
            "INSERT OR REPLACE INTO devices VALUES (?,?,?,?,?,?)",
            (id, name, type, pubkey_b64, now, None),
        )
        self._conn.commit()
        return Device(id, name, type, pubkey_b64, now, None)

    def get_device(self, id: str) -> Optional[Device]:
        row = self._conn.execute(
            "SELECT * FROM devices WHERE id=?", (id,)
        ).fetchone()
        return Device(**dict(row)) if row else None

    def get_device_by_name(self, name: str) -> Optional[Device]:
        row = self._conn.execute(
            "SELECT * FROM devices WHERE name=?", (name,)
        ).fetchone()
        return Device(**dict(row)) if row else None

    def list_devices(self, type: Optional[str] = None) -> list[Device]:
        if type:
            rows = self._conn.execute(
                "SELECT * FROM devices WHERE type=? ORDER BY registered_at", (type,)
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM devices ORDER BY registered_at"
            ).fetchall()
        return [Device(**dict(r)) for r in rows]

    def set_device_gateway_if_empty(self, device_id: str, gateway_id: str) -> bool:
        """Record the gateway a device's traffic arrives through when
        provisioning left none (the add-device wizard sends no gateway).
        Never overwrites a recorded one.  Returns True when it wrote."""
        cur = self._conn.execute(
            "UPDATE provision_info SET gateway_id = ? "
            "WHERE device_id = ? AND (gateway_id IS NULL OR gateway_id = '')",
            (gateway_id, device_id))
        self._conn.commit()
        return cur.rowcount > 0

    def list_devices_for_gateway(self, gateway_id: str) -> list[Device]:
        """Devices whose provision_info.gateway_id matches.

        Joins devices with provision_info on device id.  Devices without
        a provision_info row (none currently, but possible) are excluded.
        """
        rows = self._conn.execute(
            "SELECT d.* FROM devices d "
            "JOIN provision_info p ON p.device_id = d.id "
            "WHERE p.gateway_id = ? "
            "ORDER BY d.registered_at",
            (gateway_id,),
        ).fetchall()
        return [Device(**dict(r)) for r in rows]

    def touch_device(self, id: str):
        self._conn.execute(
            "UPDATE devices SET last_seen=? WHERE id=?", (int(time.time()), id)
        )
        self._conn.commit()

    # ── gateways ─────────────────────────────────────────────────────────────

    def register_gateway(self, id: str, name: str, pubkey_b64: str,
                         mdns_addr: str = "") -> Gateway:
        now = int(time.time())
        self._conn.execute(
            "INSERT INTO gateways "
            "(id, name, pubkey_b64, mdns_addr, registered_at, last_seen) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "  name = excluded.name, "
            "  pubkey_b64 = excluded.pubkey_b64, "
            "  mdns_addr = excluded.mdns_addr, "
            "  registered_at = excluded.registered_at, "
            "  last_seen = excluded.last_seen",
            (id, name, pubkey_b64, mdns_addr, now, None),
        )
        self._conn.commit()
        return Gateway(id, name, pubkey_b64, mdns_addr, now, None)

    def update_gateway_thread_state(self, id: str, role: int, rloc16: int,
                                    mleid_hex: str) -> None:
        self._conn.execute(
            "UPDATE gateways SET role=?, rloc16=?, mleid_hex=?, "
            "  last_thread_state_at=?, last_seen=? WHERE id=?",
            (int(role), int(rloc16), mleid_hex, int(time.time()),
             int(time.time()), id),
        )
        self._conn.commit()

    def get_gateway(self, id: str) -> Optional[Gateway]:
        row = self._conn.execute(
            "SELECT * FROM gateways WHERE id=?", (id,)
        ).fetchone()
        return Gateway(**dict(row)) if row else None

    def list_gateways(self) -> list[Gateway]:
        rows = self._conn.execute(
            "SELECT * FROM gateways ORDER BY registered_at"
        ).fetchall()
        return [Gateway(**dict(r)) for r in rows]

    def delete_gateway(self, id: str) -> bool:
        cur = self._conn.execute("DELETE FROM gateways WHERE id=?", (id,))
        self._conn.commit()
        return cur.rowcount > 0

    # ── settings (hub-wide runtime flags) ────────────────────────────────────
    def get_setting(self, key: str, default: str | None = None) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set_setting(self, key: str, value: str):
        self._conn.execute(
            "INSERT INTO settings (key, value, updated_at) VALUES (?,?,?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value,"
            " updated_at=excluded.updated_at",
            (key, value, int(time.time())))
        self._conn.commit()

    def delete_setting(self, key: str) -> None:
        self._conn.execute("DELETE FROM settings WHERE key=?", (key,))
        self._conn.commit()

    def settings_with_prefix(self, prefix: str) -> dict[str, str]:
        """{key: value} for every setting whose key starts with prefix."""
        rows = self._conn.execute(
            "SELECT key, value FROM settings WHERE substr(key, 1, ?) = ?",
            (len(prefix), prefix)).fetchall()
        return {r["key"]: r["value"] for r in rows}

    # ── cameras (self-registered) ────────────────────────────────────────────
    def upsert_camera(self, id: str, name: str, url: str, caps_json: str = "{}"):
        now = int(time.time())
        # A service that restarts registers before its camera reconnects, so
        # its first POST carries no capabilities.  Treat empty as "unknown"
        # and keep what we already learned, or the UI loses the class list
        # for 30 s on every restart.
        if caps_json in ("", "{}", "null"):
            prev = self._conn.execute(
                "SELECT caps_json FROM cameras WHERE id=?", (id,)).fetchone()
            if prev and prev["caps_json"] not in ("", "{}", "null"):
                caps_json = prev["caps_json"]
        self._conn.execute(
            "INSERT INTO cameras (id, name, url, caps_json, registered_at, last_seen)"
            " VALUES (?,?,?,?,?,?)"
            " ON CONFLICT(id) DO UPDATE SET name=excluded.name, url=excluded.url,"
            " caps_json=excluded.caps_json, last_seen=excluded.last_seen",
            (id, name, url, caps_json, now, now))
        self._conn.commit()

    def list_cameras(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM cameras ORDER BY registered_at").fetchall()
        return [dict(r) for r in rows]

    def get_camera_policy(self, id: str) -> str:
        row = self._conn.execute(
            "SELECT policy_json FROM cameras WHERE id=?", (id,)).fetchone()
        return (row["policy_json"] if row else "") or "{}"

    def set_camera_policy(self, id: str, policy_json: str):
        """Store the policy doc.  Upserts a row so a policy can be written
        for a statically-configured (NN_CAMERAS) camera that has never
        self-registered."""
        now = int(time.time())
        self._conn.execute(
            "INSERT INTO cameras (id, name, url, caps_json, policy_json,"
            " registered_at, last_seen) VALUES (?,?,?,?,?,?,0)"
            " ON CONFLICT(id) DO UPDATE SET policy_json=excluded.policy_json",
            (id, id, "", "{}", policy_json, now))
        self._conn.commit()

    def delete_camera(self, id: str):
        self._conn.execute("DELETE FROM cameras WHERE id=?", (id,))
        self._conn.commit()

    def touch_gateway(self, id: str):
        self._conn.execute(
            "UPDATE gateways SET last_seen=? WHERE id=?", (int(time.time()), id)
        )
        self._conn.commit()

    # ── configs ──────────────────────────────────────────────────────────────

    def get_config(self, device_id: str) -> Optional[Config]:
        row = self._conn.execute(
            "SELECT * FROM configs WHERE device_id=?", (device_id,)
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["payload"] = json.loads(d["payload"])
        return Config(**d)

    def set_config(self, device_id: str, payload: dict) -> Config:
        now      = int(time.time())
        existing = self.get_config(device_id)
        version  = (existing.version + 1) if existing else 1
        self._conn.execute(
            "INSERT OR REPLACE INTO configs VALUES (?,?,?,?)",
            (device_id, version, json.dumps(payload), now),
        )
        self._conn.commit()
        return Config(device_id, version, payload, now)

    # ── firmware targets ──────────────────────────────────────────────────────

    def set_firmware_target(self, device_type: str, version: str,
                             path: str, size: int, sha256: str) -> FirmwareTarget:
        now = int(time.time())
        self._conn.execute(
            "INSERT OR REPLACE INTO firmware_targets VALUES (?,?,?,?,?,?)",
            (device_type, version, path, size, sha256, now),
        )
        self._conn.commit()
        return FirmwareTarget(device_type, version, path, size, sha256, now)

    def get_firmware_target(self, device_type: str) -> Optional[FirmwareTarget]:
        row = self._conn.execute(
            "SELECT * FROM firmware_targets WHERE device_type=?", (device_type,)
        ).fetchone()
        return FirmwareTarget(**dict(row)) if row else None

    def list_firmware_targets(self) -> list[FirmwareTarget]:
        rows = self._conn.execute(
            "SELECT * FROM firmware_targets ORDER BY updated_at DESC"
        ).fetchall()
        return [FirmwareTarget(**dict(r)) for r in rows]

    # ── firmware catalog (sync layer) ─────────────────────────────────────────

    def upsert_catalog_entry(self, device_type: str, version: str,
                              source_name: str, asset_uri: str,
                              manifest_json: str, sha256: str,
                              size_bytes: int) -> CatalogEntry:
        """Insert-or-update a catalog row.  Preserves downloaded_at / local_path
        on update so a re-discovery doesn't lose the cached copy."""
        now = int(time.time())
        existing = self._conn.execute(
            "SELECT downloaded_at, local_path, first_seen_at "
            "FROM firmware_catalog "
            "WHERE device_type=? AND version=? AND source_name=?",
            (device_type, version, source_name),
        ).fetchone()
        if existing:
            self._conn.execute(
                "UPDATE firmware_catalog SET "
                " asset_uri=?, manifest_json=?, sha256=?, size_bytes=?, "
                " last_seen_at=? "
                "WHERE device_type=? AND version=? AND source_name=?",
                (asset_uri, manifest_json, sha256, size_bytes, now,
                 device_type, version, source_name),
            )
            first_seen = existing["first_seen_at"]
            downloaded_at = existing["downloaded_at"]
            local_path = existing["local_path"]
        else:
            self._conn.execute(
                "INSERT INTO firmware_catalog "
                "(device_type, version, source_name, asset_uri, manifest_json, "
                " sha256, size_bytes, downloaded_at, local_path, "
                " first_seen_at, last_seen_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (device_type, version, source_name, asset_uri, manifest_json,
                 sha256, size_bytes, None, None, now, now),
            )
            first_seen = now
            downloaded_at = None
            local_path = None
        self._conn.commit()
        return CatalogEntry(device_type, version, source_name, asset_uri,
                             manifest_json, sha256, size_bytes,
                             downloaded_at, local_path, first_seen, now)

    def mark_catalog_downloaded(self, device_type: str, version: str,
                                 source_name: str, local_path: str) -> None:
        self._conn.execute(
            "UPDATE firmware_catalog SET downloaded_at=?, local_path=? "
            "WHERE device_type=? AND version=? AND source_name=?",
            (int(time.time()), local_path,
             device_type, version, source_name),
        )
        self._conn.commit()

    def get_catalog_entry(self, device_type: str, version: str,
                           source_name: Optional[str] = None
                           ) -> Optional[CatalogEntry]:
        if source_name is None:
            # Prefer a cached entry (downloaded_at NOT NULL) if one exists.
            row = self._conn.execute(
                "SELECT * FROM firmware_catalog "
                "WHERE device_type=? AND version=? "
                "ORDER BY (downloaded_at IS NULL), last_seen_at DESC LIMIT 1",
                (device_type, version),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT * FROM firmware_catalog "
                "WHERE device_type=? AND version=? AND source_name=?",
                (device_type, version, source_name),
            ).fetchone()
        return CatalogEntry(**dict(row)) if row else None

    def delete_catalog_entry(self, device_type: str, version: str) -> int:
        """Retire catalog rows for one image version (all sources).  Returns the
        number of rows removed.  Caller decides about the cached file."""
        cur = self._conn.execute(
            "DELETE FROM firmware_catalog WHERE device_type=? AND version=?",
            (device_type, version))
        self._conn.commit()
        return cur.rowcount

    def list_catalog(self, device_type: Optional[str] = None
                      ) -> list[CatalogEntry]:
        if device_type:
            rows = self._conn.execute(
                "SELECT * FROM firmware_catalog WHERE device_type=? "
                "ORDER BY version DESC, source_name ASC",
                (device_type,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM firmware_catalog "
                "ORDER BY device_type ASC, version DESC, source_name ASC"
            ).fetchall()
        return [CatalogEntry(**dict(r)) for r in rows]

    # ── provision_info ────────────────────────────────────────────────────────

    def set_provision_info(self, device_id: str, device_type: str,
                           enc_pubkey_b64: str = "", mdns_addr: str = "",
                           ml_eid: str = "", gateway_id: str = "",
                           ble_addr: str = "",
                           eui64: str = "",
                           capabilities: str = "[]") -> ProvisionInfo:
        now = int(time.time())
        self._conn.execute(
            "INSERT OR REPLACE INTO provision_info "
            "(device_id, device_type, enc_pubkey_b64, mdns_addr, ml_eid, "
            " gateway_id, ble_addr, eui64, capabilities, "
            " provisioned_at, last_info_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (device_id, device_type, enc_pubkey_b64, mdns_addr, ml_eid,
             gateway_id, ble_addr, eui64, capabilities, now, None),
        )
        self._conn.commit()
        return ProvisionInfo(device_id, device_type, enc_pubkey_b64, mdns_addr,
                             ml_eid, gateway_id, ble_addr, eui64,
                             capabilities, now, None)

    def get_provision_info(self, device_id: str) -> Optional[ProvisionInfo]:
        row = self._conn.execute(
            "SELECT * FROM provision_info WHERE device_id=?", (device_id,)
        ).fetchone()
        return ProvisionInfo(**dict(row)) if row else None

    def update_capabilities(self, device_id: str, capabilities_json: str) -> None:
        self._conn.execute(
            "UPDATE provision_info SET capabilities=? WHERE device_id=?",
            (capabilities_json, device_id),
        )
        self._conn.commit()

    def update_device_info_sync(self, device_id: str,
                                capabilities_json: Optional[str] = None,
                                eui64: Optional[str] = None) -> bool:
        """Persist the device-authoritative parts of a config sync
        (INFO_REPLY): field descriptors → capabilities, eui64, and stamp
        last_info_at.  Returns False when the device has no provision_info
        row yet (nothing to update — the sighting stays 'pending')."""
        sets = ["last_info_at=?"]
        args: list = [int(time.time())]
        if capabilities_json is not None:
            sets.append("capabilities=?")
            args.append(capabilities_json)
        if eui64:
            sets.append("eui64=?")
            args.append(eui64)
        args.append(device_id)
        cur = self._conn.execute(
            f"UPDATE provision_info SET {', '.join(sets)} WHERE device_id=?",
            args,
        )
        self._conn.commit()
        return cur.rowcount > 0

    def get_capabilities(self, device_name: str) -> list[dict]:
        """Get field descriptors for a device by name."""
        row = self._conn.execute(
            "SELECT p.capabilities FROM provision_info p "
            "JOIN devices d ON d.id = p.device_id "
            "WHERE d.name = ?", (device_name,)
        ).fetchone()
        if row and row[0]:
            import json
            return json.loads(row[0])
        return []

    def update_eui64(self, device_id: str, eui64: str) -> None:
        self._conn.execute(
            "UPDATE provision_info SET eui64=? WHERE device_id=?",
            (eui64, device_id),
        )
        self._conn.commit()

    def get_eui64_by_name(self, name: str) -> str | None:
        """Look up EUI-64 by device name (via devices + provision_info join)."""
        row = self._conn.execute(
            "SELECT p.eui64 FROM provision_info p "
            "JOIN devices d ON d.id = p.device_id "
            "WHERE d.name = ?", (name,)
        ).fetchone()
        return row[0] if row and row[0] else None

    def update_ml_eid(self, device_id: str, ml_eid: str) -> None:
        self._conn.execute(
            "UPDATE provision_info SET ml_eid=? WHERE device_id=?",
            (ml_eid, device_id),
        )
        self._conn.commit()

    def touch_last_info(self, device_id: str) -> None:
        self._conn.execute(
            "UPDATE provision_info SET last_info_at=? WHERE device_id=?",
            (int(time.time()), device_id),
        )
        self._conn.commit()

    # ── telemetry ─────────────────────────────────────────────────────────────

    def store_telemetry(self, device_id: str, payload: dict) -> None:
        self._conn.execute(
            "INSERT INTO telemetry (device_id, ts, payload) VALUES (?,?,?)",
            (device_id, int(time.time()), json.dumps(payload)),
        )
        self._conn.commit()

    def recent_telemetry(self, device_id: Optional[str] = None,
                         limit: int = 50) -> list[dict]:
        if device_id:
            rows = self._conn.execute(
                "SELECT * FROM telemetry WHERE device_id=? "
                "ORDER BY ts DESC LIMIT ?",
                (device_id, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM telemetry ORDER BY ts DESC LIMIT ?",
                (limit,),
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["payload"] = json.loads(d["payload"])
            result.append(d)
        return result

    # ── events ─────────────────────────────────────────────────────────────────

    # ── archive (device unregister) ───────────────────────────────────
    def archive_device(self, device_id: str, name: str, device_type: str,
                       snapshot: dict, reason: str = "unregister"):
        """Archive = MOVE, not copy: capture a snapshot, then delete the
        live rows so every existing consumer (grid, OTA fleet, auto
        compiler, session table) excludes the device with zero query
        changes.  telemetry/events stay until purge."""
        import json as _json, time as _time
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO archived_devices "
                "(device_id, name, device_type, archived_at, reason, snapshot_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (device_id, name, device_type, int(_time.time()), reason,
                 _json.dumps(snapshot)))
            self._conn.execute("DELETE FROM devices WHERE id = ?", (device_id,))
            self._conn.execute("DELETE FROM provision_info WHERE device_id = ?",
                               (device_id,))

    def list_archived(self) -> list[dict]:
        import json as _json
        rows = self._conn.execute(
            "SELECT device_id, name, device_type, archived_at, reason, "
            "snapshot_json FROM archived_devices ORDER BY archived_at DESC"
        ).fetchall()
        out = []
        for r in rows:
            try:
                snap = _json.loads(r[5])
            except Exception:
                snap = {}
            out.append({"device_id": r[0], "name": r[1], "device_type": r[2],
                        "archived_at": r[3], "reason": r[4], "snapshot": snap})
        return out

    def get_archived(self, device_id: str) -> Optional[dict]:
        for d in self.list_archived():
            if d["device_id"] == device_id or d["name"] == device_id:
                return d
        return None

    def purge_archived(self, device_id: str) -> bool:
        """The archived-device DELETE: purge ALL historic hub data."""
        a = self.get_archived(device_id)
        if not a:
            return False
        did = a["device_id"]
        with self._conn:
            self._conn.execute("DELETE FROM archived_devices WHERE device_id = ?", (did,))
            self._conn.execute("DELETE FROM telemetry WHERE device_id = ?", (did,))
            self._conn.execute("DELETE FROM events WHERE device_id = ?", (did,))
            self._conn.execute("DELETE FROM configs WHERE device_id = ?", (did,))
            # ID-scoped only.  The legacy name-keyed row is NOT deleted
            # here: names are reusable, so "card_cfg:<name>" may belong to
            # a LIVE device that replaced this one (observed 2026-08-21 —
            # purging the old c6-s3 would take the new c6-s3's config).
            self._conn.execute("DELETE FROM settings WHERE key = ?", ("card_cfg#" + did,))
            self._conn.execute("DELETE FROM settings WHERE key = ?", ("unregister_op:" + did,))
        return True

    def log_event(self, device_id: Optional[str], type: str, detail: str = ""):
        self._conn.execute(
            "INSERT INTO events (device_id, type, detail, ts) VALUES (?,?,?,?)",
            (device_id, type, detail, int(time.time())),
        )
        self._conn.commit()

    # ── hourly camera metric reports (phase 5 of nn-video pipelines) ────
    def add_metric_report(self, period_start: int, period_end: int, report_json: str) -> int:
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS camera_metric_reports ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT, period_start INTEGER NOT NULL,"
            " period_end INTEGER NOT NULL, report_json TEXT NOT NULL)")
        cur = self._conn.execute(
            "INSERT INTO camera_metric_reports (period_start, period_end, report_json) VALUES (?,?,?)",
            (int(period_start), int(period_end), report_json))
        self._conn.commit()
        return int(cur.lastrowid)

    def list_metric_reports(self, limit: int = 24) -> list[dict]:
        try:
            rows = self._conn.execute(
                "SELECT id, period_start, period_end, report_json FROM camera_metric_reports "
                "ORDER BY period_start DESC LIMIT ?", (int(limit),)).fetchall()
        except Exception:
            return []
        out = []
        for r in rows:
            d = {"id": r["id"], "period_start": r["period_start"], "period_end": r["period_end"]}
            try:
                d["report"] = json.loads(r["report_json"])
            except Exception:
                d["report"] = None
            out.append(d)
        return out

    def get_metric_report(self, rid: int) -> Optional[dict]:
        try:
            r = self._conn.execute("SELECT id, period_start, period_end, report_json FROM camera_metric_reports WHERE id=?",
                                   (int(rid),)).fetchone()
        except Exception:
            return None
        if not r:
            return None
        return {"id": r["id"], "period_start": r["period_start"], "period_end": r["period_end"],
                "report": json.loads(r["report_json"])}

    def prune_metric_reports(self, keep: int) -> None:
        try:
            self._conn.execute(
                "DELETE FROM camera_metric_reports WHERE id NOT IN "
                "(SELECT id FROM camera_metric_reports ORDER BY period_start DESC LIMIT ?)", (int(keep),))
            self._conn.commit()
        except Exception:
            pass

    # ── RADIO_STATS reports (hub/radio_health.py) ────────────────────────
    RADIO_STATS_KEEP_S = 7 * 86400

    def _radio_table(self) -> None:
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS radio_stats ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT NOT NULL,"
            " ts INTEGER NOT NULL, report_json TEXT NOT NULL)")
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS radio_stats_dev_ts ON radio_stats (device_id, ts)")

    def add_radio_stats(self, device_id: str, ts: int, report: dict) -> None:
        self._radio_table()
        self._conn.execute(
            "INSERT INTO radio_stats (device_id, ts, report_json) VALUES (?,?,?)",
            (device_id, int(ts), json.dumps(report, separators=(",", ":"))))
        self._conn.execute("DELETE FROM radio_stats WHERE ts < ?",
                           (int(ts) - self.RADIO_STATS_KEEP_S,))
        self._conn.commit()

    def radio_stats_since(self, device_id: Optional[str], since_ts: int) -> list[dict]:
        """Reports newer than since_ts, oldest first (all devices if None)."""
        self._radio_table()
        q = "SELECT device_id, ts, report_json FROM radio_stats WHERE ts >= ?"
        args: list = [int(since_ts)]
        if device_id:
            q += " AND device_id = ?"
            args.append(device_id)
        rows = self._conn.execute(q + " ORDER BY ts", args).fetchall()
        out = []
        for r in rows:
            try:
                d = json.loads(r["report_json"])
            except Exception:
                continue
            d["ts"] = r["ts"]
            d["device_id"] = r["device_id"]
            out.append(d)
        return out

    def recent_events(self, limit: int = 50) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM events ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
