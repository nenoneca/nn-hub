"""
Wire protocol: newline-delimited JSON over WebSocket.

All messages have a "type" field.  End device messages are wrapped by the
gateway in a DeviceEnvelope so hub always sees a device_id regardless of
whether the message came from an end node or the gateway itself.

Auth flow (on every new gateway connection):
  1. Gateway → Hub:  HelloMsg      (proves gateway identity)
  2. Hub    → Gateway: WelcomeMsg  (proves hub identity, completes handshake)

After handshake hub immediately sends HubStatusMsg so gateway can broadcast
hub-online state into the Thread mesh.
"""

from __future__ import annotations
from dataclasses import dataclass, field, asdict
import json
import time


# ── helpers ─────────────────────────────────────────────────────────────────

def encode(msg) -> str:
    return json.dumps(asdict(msg))

def decode(raw: str) -> dict:
    return json.loads(raw)


# ── auth handshake ───────────────────────────────────────────────────────────

@dataclass
class HelloMsg:
    """Gateway → Hub.  Gateway signs its own nonce to prove key ownership."""
    type: str        = "hello"
    gateway_id: str  = ""     # pre-registered gateway ID
    pubkey_b64: str  = ""     # Ed25519 public key (base64url)
    nonce: str       = ""     # random 32 bytes (base64), freshness
    sig: str         = ""     # Ed25519(privkey, nonce)


@dataclass
class WelcomeMsg:
    """Hub → Gateway.  Hub signs (gateway_nonce ‖ hub_nonce) to prove identity."""
    type: str       = "welcome"
    hub_id: str     = ""
    pubkey_b64: str = ""      # hub public key so gateway can verify future pushes
    nonce: str      = ""      # hub's random 32 bytes
    sig: str        = ""      # Ed25519(hub_privkey, gateway_nonce || hub_nonce)


@dataclass
class AuthErrorMsg:
    type: str   = "auth_error"
    reason: str = ""


# ── hub status ───────────────────────────────────────────────────────────────

@dataclass
class HubStatusMsg:
    """Hub → Gateway.  Gateway broadcasts this to every end node over Thread."""
    type: str   = "hub_status"
    online: bool = True
    hub_id: str  = ""
    ts: int      = 0

    def __post_init__(self):
        if not self.ts:
            self.ts = int(time.time())


# ── device ↔ hub (proxied through gateway) ───────────────────────────────────

@dataclass
class ConfigQueryMsg:
    """Device → Hub.  Device asks whether hub has a newer config."""
    type: str            = "config_query"
    device_id: str       = ""
    current_version: int = 0


@dataclass
class ConfigResponseMsg:
    """Hub → Device.
    payload=None  → device is already up to date (version field still set).
    payload=dict  → full config the device should apply.
    """
    type: str       = "config_response"
    device_id: str  = ""
    version: int    = 0
    payload: dict | None = None


@dataclass
class FirmwareQueryMsg:
    """Device → Hub.  Device reports what it's running; hub replies if there's a newer target."""
    type: str            = "firmware_query"
    device_id: str       = ""
    device_type: str     = ""
    running_version: str = ""


@dataclass
class FirmwareResponseMsg:
    """Hub → Device.  Tells device what version it should be running.
    If target_version == running_version, device does nothing.
    Otherwise device calls firmware_chunk_req in a loop.
    """
    type: str            = "firmware_response"
    device_id: str       = ""
    device_type: str     = ""
    target_version: str  = ""
    download_token: str  = ""   # short-lived signed token authorising download
    size_bytes: int      = 0
    sha256: str          = ""


@dataclass
class FirmwareChunkReqMsg:
    """Device → Hub.  Request one chunk of firmware."""
    type: str        = "firmware_chunk_req"
    device_id: str   = ""
    device_type: str = ""
    version: str     = ""
    token: str       = ""
    offset: int      = 0
    length: int      = 4096


@dataclass
class FirmwareChunkMsg:
    """Hub → Device.  One chunk.  Device reassembles and verifies SHA-256 at the end."""
    type: str       = "firmware_chunk"
    device_id: str  = ""
    version: str    = ""
    offset: int     = 0
    data_b64: str   = ""
    last: bool      = False


@dataclass
class CommandMsg:
    """Hub → Device.  Arbitrary command."""
    type: str        = "command"
    device_id: str   = ""
    cmd: str         = ""     # "reboot" | "reset_config" | ...
    args: dict       = field(default_factory=dict)


@dataclass
class AckMsg:
    """Device → Hub.  Acknowledge a command or sync."""
    type: str       = "ack"
    device_id: str  = ""
    ref_type: str   = ""      # type of the message being acked
    ok: bool        = True
    error: str      = ""


# ── telemetry (device → hub) ─────────────────────────────────────────────────

@dataclass
class TelemetryMsg:
    """Device → Hub.  Sensor reading or any structured uplink payload.

    Delivered either:
      - directly over CoAP POST /telemetry  (Thread devices → hub)
      - proxied over WebSocket by a software gateway
    """
    type: str       = "telemetry"
    device_id: str  = ""
    ts: int         = 0          # Unix timestamp; hub fills in if 0
    data: dict      = field(default_factory=dict)


@dataclass
class AlertMsg:
    """Hub → Device (or operator log).  Inference result or threshold breach."""
    type: str       = "alert"
    device_id: str  = ""
    ts: int         = 0
    level: str      = "info"     # "info" | "warn" | "critical"
    message: str    = ""
    data: dict      = field(default_factory=dict)
