"""
WebSocket server.  Gateway connects to hub (hub is the TCP server).

This keeps the design NAT-friendly: gateway always initiates the outbound
connection whether hub is on a cloud VM, a local RPi, or a developer's laptop.

One gateway session at a time (single-hub, single-gateway assumption for now).
If the gateway reconnects while a session is live, the old session is dropped.

Message routing:
  gateway → hub:   raw JSON with device_id field (end node messages are
                   forwarded by gateway transparently, they just add device_id)
  hub → device:    hub sends JSON with device_id; gateway delivers to the
                   correct end node over the Thread mesh
  hub → gateway:   same channel, device_id="" or device_id == gateway_id
"""

from __future__ import annotations
import asyncio
import base64
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import websockets
from websockets.server import WebSocketServerProtocol

from . import protocol as P
from .auth import hub_id as get_hub_id, pubkey_b64 as get_pubkey_b64
from .auth import random_nonce, sign, verify
from .db import DB
from .inference import InferenceEngine

log = logging.getLogger("hub.server")

CHUNK_SIZE = 4096


class GatewaySession:
    def __init__(self, ws: WebSocketServerProtocol, gateway_id: str):
        self.ws           = ws
        self.gateway_id   = gateway_id
        self.connected_at = int(time.time())

    async def send(self, msg) -> None:
        await self.ws.send(P.encode(msg))


class HubServer:
    def __init__(self, db: DB, privkey, host: str, port: int, data_dir: Path,
                 inference: InferenceEngine | None = None):
        self.db        = db
        self.privkey   = privkey
        self.host      = host
        self.port      = port
        self.data_dir  = data_dir
        self.inference = inference or InferenceEngine()
        self._session: Optional[GatewaySession] = None

    @property
    def hub_id(self) -> str:
        return get_hub_id(self.privkey)

    # ── WebSocket entry point ─────────────────────────────────────────────────

    async def _handler(self, ws: WebSocketServerProtocol):
        log.info("Connection from %s", ws.remote_address)
        try:
            session = await self._handshake(ws)
            if session is None:
                return

            # Drop any stale session
            if self._session:
                log.warning("Replacing stale session for gateway '%s'",
                            self._session.gateway_id)
                await self._session.ws.close()
            self._session = session

            log.info("Gateway '%s' authenticated", session.gateway_id)
            self.db.log_event(session.gateway_id, "gateway_connected")

            # Tell gateway (and through it, the mesh) that hub is online
            await session.send(P.HubStatusMsg(
                online=True, hub_id=self.hub_id
            ))

            await self._serve(session)

        except websockets.exceptions.ConnectionClosed:
            log.info("Gateway disconnected")
        finally:
            if self._session and self._session.ws is ws:
                self._session = None
                self.db.log_event(None, "gateway_disconnected")

    # ── Auth handshake ────────────────────────────────────────────────────────

    async def _handshake(self, ws: WebSocketServerProtocol
                         ) -> Optional[GatewaySession]:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
        except asyncio.TimeoutError:
            await ws.send(P.encode(P.AuthErrorMsg(reason="handshake timeout")))
            return None

        msg = P.decode(raw)
        if msg.get("type") != "hello":
            await ws.send(P.encode(P.AuthErrorMsg(reason="expected hello")))
            return None

        gateway_id = msg["gateway_id"]
        gw_pubkey  = msg["pubkey_b64"]
        gw_nonce   = msg["nonce"]
        gw_sig     = msg["sig"]

        # Gateway must have signed its own nonce
        if not verify(gw_pubkey, base64.b64decode(gw_nonce), gw_sig):
            await ws.send(P.encode(P.AuthErrorMsg(reason="invalid gateway signature")))
            log.warning("Auth failure for gateway '%s'", gateway_id)
            return None

        # Check gateway is registered; on first connect auto-register (policy TBD)
        device = self.db.get_device(gateway_id)
        if device is None:
            self.db.register_device(gateway_id, gateway_id, "gateway", gw_pubkey)
            log.info("Auto-registered new gateway '%s'", gateway_id)
        elif device.pubkey_b64 != gw_pubkey:
            await ws.send(P.encode(P.AuthErrorMsg(reason="pubkey mismatch")))
            return None

        # Hub proves its identity: sign (gateway_nonce ‖ hub_nonce)
        hub_nonce  = random_nonce()
        hub_sig    = sign(
            self.privkey,
            base64.b64decode(gw_nonce) + base64.b64decode(hub_nonce),
        )
        await ws.send(P.encode(P.WelcomeMsg(
            hub_id     = self.hub_id,
            pubkey_b64 = get_pubkey_b64(self.privkey),
            nonce      = hub_nonce,
            sig        = hub_sig,
        )))

        self.db.touch_device(gateway_id)
        return GatewaySession(ws, gateway_id)

    # ── Main receive loop ─────────────────────────────────────────────────────

    async def _serve(self, session: GatewaySession):
        async for raw in session.ws:
            try:
                msg = P.decode(raw)
            except Exception:
                log.warning("Malformed JSON from gateway")
                continue

            mtype     = msg.get("type")
            device_id = msg.get("device_id", "")
            log.debug("← %-22s  device=%s", mtype, device_id or "—")

            if mtype == "telemetry":
                await self._on_telemetry(session, msg)
            elif mtype == "config_query":
                await self._on_config_query(session, msg)
            elif mtype == "firmware_query":
                await self._on_firmware_query(session, msg)
            elif mtype == "firmware_chunk_req":
                await self._on_firmware_chunk(session, msg)
            elif mtype == "ack":
                self.db.log_event(device_id, "ack", msg.get("ref_type", ""))
            else:
                log.debug("Unhandled type: %s", mtype)

    # ── Handlers ──────────────────────────────────────────────────────────────

    async def _on_telemetry(self, session: GatewaySession, msg: dict):
        device_id = msg.get("device_id", "")
        data      = msg.get("data", {})
        ts        = msg.get("ts") or int(time.time())

        dev = self.db.get_device(device_id)
        if dev is None:
            self.db.register_device(device_id, device_id, "end_device", "")
            log.info("Auto-registered end device '%s'", device_id)
        else:
            self.db.touch_device(device_id)

        self.db.store_telemetry(device_id, {"ts": ts, **data})
        log.info("telemetry  device=%-20s  fields=%s", device_id, list(data.keys()))

        alerts = self.inference.process(device_id, data)
        if alerts:
            self.db.log_event(device_id, "inference_alert",
                              json.dumps(alerts[0]))
            await session.send(P.AlertMsg(
                device_id=device_id,
                ts=int(time.time()),
                level=alerts[0]["level"],
                message=alerts[0]["message"],
                data=alerts[0].get("data", {}),
            ))

    async def _on_config_query(self, session: GatewaySession, msg: dict):
        device_id = msg["device_id"]
        current_v = msg.get("current_version", 0)
        self.db.touch_device(device_id)

        cfg = self.db.get_config(device_id)
        hub_v = cfg.version if cfg else 0

        await session.send(P.ConfigResponseMsg(
            device_id = device_id,
            version   = hub_v,
            payload   = cfg.payload if (cfg and hub_v > current_v) else None,
        ))
        log.info("config_query  device=%-20s  cur=%-4d  hub=%d",
                 device_id, current_v, hub_v)

    async def _on_firmware_query(self, session: GatewaySession, msg: dict):
        device_id   = msg["device_id"]
        device_type = msg["device_type"]
        running_ver = msg.get("running_version", "")

        target = self.db.get_firmware_target(device_type)
        if target is None or target.target_version == running_ver:
            return  # nothing to do

        # Issue a download token: signed "{device_id}:{type}:{version}:{ts}"
        token_plain = f"{device_id}:{device_type}:{target.target_version}:{int(time.time())}"
        token       = sign(self.privkey, token_plain.encode())

        await session.send(P.FirmwareResponseMsg(
            device_id      = device_id,
            device_type    = device_type,
            target_version = target.target_version,
            download_token = token,
            size_bytes     = target.size_bytes,
            sha256         = target.sha256,
        ))
        log.info("firmware_query  device=%-20s  type=%-24s  target=%s",
                 device_id, device_type, target.target_version)

    async def _on_firmware_chunk(self, session: GatewaySession, msg: dict):
        device_id   = msg["device_id"]
        device_type = msg["device_type"]
        version     = msg["version"]
        offset      = msg["offset"]
        length      = min(msg.get("length", CHUNK_SIZE), CHUNK_SIZE)

        target = self.db.get_firmware_target(device_type)
        if target is None or target.target_version != version:
            log.warning("firmware_chunk_req for unknown %s/%s", device_type, version)
            return

        fw_path = Path(target.firmware_path)
        if not fw_path.exists():
            log.error("Firmware file missing: %s", fw_path)
            return

        with open(fw_path, "rb") as f:
            f.seek(offset)
            data = f.read(length)

        last = (offset + len(data)) >= target.size_bytes
        await session.send(P.FirmwareChunkMsg(
            device_id = device_id,
            version   = version,
            offset    = offset,
            data_b64  = base64.b64encode(data).decode(),
            last      = last,
        ))

    # ── Hub-initiated operations (called from CLI) ────────────────────────────

    async def push_command(self, device_id: str, cmd: str,
                           args: dict | None = None) -> None:
        """Send a command to a specific device through the gateway."""
        session = self._require_session()
        await session.send(P.CommandMsg(
            device_id=device_id, cmd=cmd, args=args or {}
        ))
        log.info("push_command  device=%s  cmd=%s", device_id, cmd)

    async def push_config(self, device_id: str) -> None:
        """Push current config to a device without waiting for it to query."""
        session = self._require_session()
        cfg = self.db.get_config(device_id)
        if cfg is None:
            raise ValueError(f"No config stored for device '{device_id}'")
        await session.send(P.ConfigResponseMsg(
            device_id=device_id, version=cfg.version, payload=cfg.payload
        ))
        log.info("push_config  device=%s  version=%d", device_id, cfg.version)

    def _require_session(self) -> GatewaySession:
        if self._session is None:
            raise RuntimeError("No gateway connected")
        return self._session

    # ── Start ──────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        log.info("Listening on ws://%s:%d", self.host, self.port)
        async with websockets.serve(self._handler, self.host, self.port):
            await asyncio.Future()  # run forever
