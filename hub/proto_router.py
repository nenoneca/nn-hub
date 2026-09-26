"""nn_proto routing core on the hub side.

Owns:
  - device_id → gateway_id mapping (which gateway forwarded a device's
    most recent D2H — used to route H2D back).
  - D2H telemetry dispatch.  For Phase 3 this just logs the inbound
    frame; Phase 5 will plumb it into the existing telemetry/inference
    engines (currently fed by hub.coap_server).
  - H2D send: looks up the device's gateway, builds + signs an H2D
    frame with the hub's ECDSA P-256 key, and enqueues it on the right
    gateway TCP conn.
"""

from __future__ import annotations

import asyncio
import base64
import contextvars
import hashlib
import logging
import secrets
import struct
import time
from pathlib import Path
from typing import Optional

from cryptography.hazmat.primitives.asymmetric import ec

from . import proto
from .db import DB

# What handling one D2H frame did, for its RX log line (set by the paths
# in _on_d2h and the handlers it calls; one context per frame).
_rx_note: contextvars.ContextVar = contextvars.ContextVar("rx_note", default="")


def _note(text: str) -> None:
    _rx_note.set(text)


log = logging.getLogger("hub.proto_router")


class HubProtoRouter:
    def __init__(self, db: DB, server, hub_privkey_path: Optional[Path] = None):
        self._db = db
        self._server = server
        # device_id (hex) → gateway_id (hex) it was last seen via.
        self._device_route: dict[str, str] = {}
        # Phase 6 FIELD_OP correlation: tid (int) → asyncio.Future[bytes]
        self._pending_field: dict[int, "asyncio.Future"] = {}
        # Phase 7 generic request_h2d: tid → expected reply cmd (for gating)
        self._pending_expect_cmd: dict[int, int] = {}
        # Phase 7 control-plane: cmd → async handler(device_id_hex, tid, body)
        self._d2h_dispatch: dict[int, "callable"] = {}
        # Unknown-device sightings: device_id_hex →
        #   {"first_seen": int, "last_seen": int, "via_gateway": str,
        #    "frame_count": int}
        # Populated by _on_d2h when an inbound frame's device_id isn't
        # in the devices table.  Capped at PENDING_MAX entries (LRU evict
        # by last_seen).  Exposed via get_pending_devices() for the REST
        # /api/v1/devices/pending endpoint.
        self._pending_devices: dict[str, dict] = {}

        # In-flight OTA progress per device, populated by the OTA_CHECK
        # and OTA_BLOCK_REQ handlers.  Keyed by device_id_hex.  Exposed
        # via get_ota_status() for `GET /api/v1/devices/{dev}/ota`.
        # Entry shape:
        #   {
        #     "running_version":  str (last seen via OTA_CHECK)
        #     "target_version":   str (from the firmware target row)
        #     "target_size":      int (bytes)
        #     "total_blocks":     int (ceil(size / OTA_BLOCK_DEFAULT_SIZE))
        #     "highest_block":    int (max block_num seen in BLOCK_REQ)
        #     "last_block_at":    int (unix ts of most recent BLOCK_REQ)
        #     "started_at":       int (unix ts of OTA_CHECK that opened the run)
        #     "in_flight":        bool
        #   }
        self._ota_state: dict[str, dict] = {}
        # Fleet-view subscribers: asyncio.Queues fed a {"device_id",
        # "running"} event whenever a device's self-reported firmware
        # version CHANGES (INFO_REPLY or OTA_CHECK).  Backs /fleet/ws.
        self._fleet_subs: set = set()
        # Phase 2 symmetric sessions: device_id_hex → {"sess": Session,
        # "verified": bool}.  Established per device-boot via the salt
        # handshake (device advertises its salt in INFO_REPLY, hub replies
        # SESS_INIT with its own, both derive from the static X25519 ECDH).
        self._sessions: dict[str, dict] = {}
        self._field_event_listeners: list = []
        self.group_key_pushes: dict[str, dict] = {}    # device -> {epoch, at, reason, pushes}
        self.hb_uptime: dict = {}
        # Hub long-term X25519 private key (the ECIES/session root).
        # Injected by serve() — set_enc_key() — since it's loaded there.
        self._enc_priv = None
        self._hub_priv = self._load_or_create_hub_p256(hub_privkey_path)
        pub_blob = proto.pubkey_to_uncompressed(self._hub_priv.public_key())
        self._hub_id = hashlib.sha256(pub_blob).digest()[:8]
        log.info("hub nn_proto P-256 id = %s", self._hub_id.hex())

        # Register built-in sensor↔hub control-plane handlers.
        self.register_d2h_handler(proto.Cmd.TIME_QUERY,    self._on_time_query)
        self.register_d2h_handler(proto.Cmd.LOG_LINE,      self._on_log_line)
        self.register_d2h_handler(proto.Cmd.OTA_CHECK,     self._on_ota_check)
        self.register_d2h_handler(proto.Cmd.OTA_BLOCK_REQ, self._on_ota_block_req)
        self.register_d2h_handler(proto.Cmd.OTA_CHUNKSUMS_REQ,
                                  self._on_ota_chunksums_req)
        self.register_d2h_handler(proto.Cmd.OTA_PATCH_REQ,
                                  self._on_ota_patch_req)
        self.register_d2h_handler(proto.Cmd.OTA_READY,     self._on_ota_ready)
        self.register_d2h_handler(proto.Cmd.AUTO_EVENT,    self._on_auto_event)
        self.register_d2h_handler(proto.Cmd.RADIO_STATS,   self._on_radio_stats)
        self.register_d2h_handler(proto.Cmd.SESS_PROBE,    self._on_sess_probe)
        self.register_d2h_handler(proto.Cmd.SESS_HELLO,    self._on_sess_hello)

        # Live field cache: dev_id → {field: {"v": float, "ts": epoch}}.
        # Fed by fire-and-forget AUTO_EVENT pushes the device emits when
        # an ACTUATOR value changes from any source (hub write, local
        # rule action, D2D notify).  Lets the webapp cards track
        # automation outcomes by polling the hub instead of the mesh.
        self._field_cache: dict[str, dict] = {}
        # Desired state per (device, base field): what an operator write
        # or an automation asked for, until the device reports it
        # (confirmed), reports something else later (superseded), refuses
        # it (rejected) or DESIRED_TTL_S passes.  Served merged into the
        # field cache so every viewer shows "turning on…" and moves the
        # switch only on the device's own word.
        self._desired: dict[str, dict] = {}
        self._rx_seen: dict[str, list] = {}   # device -> [tid, repeats, last_rx]
        self._gw_state_seen: dict[str, tuple] = {}
        # per device: messages received and how many were REPEATs, snapshotted
        # into every RADIO_STATS report (hub/radio_health.py)
        self._rx_counts: dict[str, dict] = {}
        # per device: (cmd, tid, first arrival) of the last message, to drop
        # the sensor's burst copies of one send (_is_burst_copy)
        self._burst_first: dict[str, tuple] = {}
        # hub -> gateway requests (H2G) waiting for their D2G reply, by tid
        self._pending_gw: dict[int, "asyncio.Future"] = {}
        # recent hub -> device frames by (device, cmd, tid): re-sent once via
        # another gateway when the first one reports GW_H2D_UNDELIVERABLE
        self._recent_h2d: dict[tuple, tuple] = {}
        # devices whose provision row already has a gateway (checked once)
        self._gw_recorded: set[str] = set()

    # ── identity ────────────────────────────────────────────────────────

    def _load_or_create_hub_p256(self, path: Optional[Path]) -> ec.EllipticCurvePrivateKey:
        path = path or Path.home() / ".nn-hub" / "proto_p256_priv.bin"
        if path.exists():
            raw = path.read_bytes()
            if len(raw) != 32:
                raise RuntimeError(f"{path}: bad private key length {len(raw)}")
            priv_int = int.from_bytes(raw, "big")
            return ec.derive_private_key(priv_int, proto.CURVE)
        path.parent.mkdir(parents=True, exist_ok=True)
        key = ec.generate_private_key(proto.CURVE)
        path.write_bytes(key.private_numbers().private_value.to_bytes(32, "big"))
        path.chmod(0o600)
        log.info("generated fresh hub P-256 keypair at %s", path)
        return key

    @property
    def hub_id(self) -> bytes:
        return self._hub_id

    @property
    def hub_pubkey_bytes(self) -> bytes:
        return proto.pubkey_to_uncompressed(self._hub_priv.public_key())

    # ── control-plane dispatch registry ────────────────────────────────
    #
    # Each handler receives (device_id_hex, tid, body_bytes).  For
    # request/reply cmds, the handler builds the reply body and calls
    # self._send_h2d_reply(device_id_hex, reply_cmd, tid, body).
    # For fire-and-forget D2H (LOG_LINE/AUTO_EVENT) the handler just
    # processes and returns.

    def register_d2h_handler(self, cmd: int, handler) -> None:
        self._d2h_dispatch[cmd] = handler
        log.debug("d2h handler: 0x%04x → %s", cmd, getattr(handler, "__qualname__", handler))

    async def _send_h2d_reply(self, device_id_hex: str,
                              reply_cmd: int, tid: int,
                              body: bytes) -> bool:
        """Build [reply_cmd|tid|body] inner payload and send as H2D."""
        inner = (
            int(reply_cmd).to_bytes(2, "little") +
            int(tid).to_bytes(4, "little") +
            (body or b"")
        )
        return await self.send_h2d(device_id_hex, inner)

    # ── generic H2D request → D2H reply (hub-originated) ───────────────
    #
    # Used by every hub-originated request/reply pattern: FIELD_OP,
    # INFO_QUERY, AUTO_PUSH, OTA_*, etc.  Two retry layers, both tunable
    # per call:
    #
    #   1. INNER (burst): each `attempt` sends the same H2D frame
    #      `burst_count` times at `burst_gap_s` spacing.  Combats single-
    #      fragment 6LoWPAN loss — the sensor's handler dedups by tid so
    #      duplicates are harmless.
    #   2. OUTER (exponential backoff): if no reply arrives within
    #      `timeout`, sleep `backoff_base * 2**(attempt-1)` (capped at
    #      `backoff_max`) and re-attempt.  Up to `max_attempts` total.
    #
    # Default budget: 5 attempts × 10 s timeout + (1+2+4+8) s backoff
    # = ~65 s wall time.  Empirically pushes per-call success on a
    # flaky Thread mesh from ~10–20 % to ~60–70 %.
    #
    # `expected_reply_cmd` gates the dispatch: only D2H frames whose
    # inner cmd matches resolve this future (off-cmd replies are
    # dropped at _on_d2h).
    async def request_h2d(self, device_id: str, req_cmd: int,
                          body: bytes, expected_reply_cmd: int,
                          timeout: float = 10.0,
                          max_attempts: int = 5,
                          backoff_base: float = 1.0,
                          backoff_max: float = 16.0,
                          burst_count: int = 1,
                          burst_gap_s: float = 0.030,
                          body_fn=None) -> bytes:
        """Reliable H2D request.

        ONE tid for the whole logical request (congestion-collapse fix):
        the device signs every reply with ECDSA on a 160 MHz core and
        drains its reply slots on a multi-second retry cycle, so under
        load a reply routinely lands AFTER the attempt that solicited
        it.  The old fresh-tid-per-attempt scheme discarded those
        stragglers, retried with a new tid, and made the device sign yet
        another reply — each retry ADDED load, and five spaced requests
        could collapse the link for minutes (measured 2026-08-11).  With
        a stable tid any straggler resolves the still-registered future,
        and a re-send is a true duplicate the device's reliable layer
        dedups instead of re-serving.  burst_count now defaults to 1 for
        the same reason: each burst copy used to cost the device a full
        crypto round trip.
        """
        loop = asyncio.get_event_loop()
        last_exc: Exception = asyncio.TimeoutError()

        while True:
            tid = secrets.randbits(32)
            if tid not in self._pending_field:
                break
        fut: "asyncio.Future" = loop.create_future()
        self._pending_field[tid] = fut
        self._pending_expect_cmd[tid] = expected_reply_cmd

        # body_fn: build the body AFTER tid allocation (session-sealed
        # requests bind the tid into their AAD).  Built ONCE — re-sends
        # ship the identical bytes so the device's replay window dedups.
        if body_fn is not None:
            body = body_fn(tid)
        inner = (
            int(req_cmd).to_bytes(2, "little") +
            int(tid).to_bytes(4, "little") +
            (body or b"")
        )
        try:
            for attempt in range(1, max_attempts + 1):
                try:
                    for i in range(burst_count):
                        if fut.done():
                            break
                        ok = await self.send_h2d(device_id, inner)
                        if not ok:
                            raise RuntimeError(
                                "send_h2d failed (no route / gateway "
                                "offline / bad device_id)")
                        if i + 1 < burst_count:
                            await asyncio.sleep(burst_gap_s)
                    # shield: a per-attempt timeout must NOT cancel the
                    # future — a straggler reply from THIS tid can still
                    # resolve it during a later attempt's wait.
                    return await asyncio.wait_for(asyncio.shield(fut),
                                                  timeout=timeout)
                except (asyncio.TimeoutError, RuntimeError) as e:
                    last_exc = e
                if fut.done():
                    # resolved between timeout and here — use it
                    return fut.result()
                if attempt >= max_attempts:
                    break
                backoff = min(backoff_base * (2 ** (attempt - 1)),
                              backoff_max)
                log.info("[%s] %s attempt %d/%d no reply yet (%s); "
                         "re-sending same tid in %.1fs",
                         device_id, proto.Cmd(req_cmd).name,
                         attempt, max_attempts, last_exc, backoff)
                await asyncio.sleep(backoff)
            raise last_exc
        finally:
            self._pending_field.pop(tid, None)
            self._pending_expect_cmd.pop(tid, None)

    # ── control-plane handler: TIME_QUERY ─────────────────────────────

    async def _on_time_query(self, device_id_hex: str,
                             tid: int, body: bytes) -> None:
        """Reply with current epoch in ms as little-endian u64."""
        import time
        epoch_ms = int(time.time() * 1000)
        body_out = epoch_ms.to_bytes(8, "little")
        ok = await self._send_h2d_reply(device_id_hex,
                                        proto.Cmd.TIME_REPLY, tid, body_out)
        log.info("[%s] TIME_QUERY → epoch=%d ms (sent=%s)",
                 device_id_hex, epoch_ms, ok)

    # ── control-plane handler: LOG_LINE ─────────────────────────────
    #
    # Fire-and-forget D2H carrying a single formatted log line (UTF-8).
    async def _on_auto_event(self, device_id_hex: str,
                             tid: int, body: bytes) -> None:
        """Fire-and-forget device telemetry.  `{"t":"fld","n":...,"v":...}`
        = an actuator changed value (any source) — update the live field
        cache the webapp cards poll.  Other event kinds are logged and
        ignored for now."""
        import json as _json
        raw = body.decode("utf-8", errors="replace")
        try:
            doc = _json.loads(raw or "{}")
        except Exception:
            # Say so: a firmware built without float printf sent
            # {"v":*float*} for a whole afternoon and nothing logged it.
            log.warning("[%s] AUTO_EVENT not JSON, dropped: %r",
                        device_id_hex, raw[:120])
            return
        if not isinstance(doc, dict):
            log.warning("[%s] AUTO_EVENT not an object, dropped: %r",
                        device_id_hex, raw[:120])
            return
        if doc.get("t") == "fld" and isinstance(doc.get("n"), str):
            try:
                v = float(doc.get("v"))
            except (TypeError, ValueError):
                log.warning("[%s] field event %s has no numeric value: %r",
                            device_id_hex, doc.get("n"), doc.get("v"))
                return
            self._field_cache.setdefault(device_id_hex, {})[doc["n"]] = {
                "v": v, "ts": int(time.time()),
            }
            self._settle_desired(device_id_hex, doc["n"], v)
            _note(f"{doc['n']}={v:g} -> field cache")
            log.debug("[%s] field event: %s=%s", device_id_hex, doc["n"], v)
            # observers (the automation failsafe watches actuator changes)
            for cb in list(self._field_event_listeners):
                try:
                    cb(device_id_hex, doc["n"], v)
                except Exception as e:                       # noqa: BLE001
                    log.warning("field event listener failed: %s", e)
        else:
            log.info("[%s] AUTO_EVENT: %s", device_id_hex, doc)

    async def _on_radio_stats(self, device_id_hex: str, tid: int, body: bytes) -> None:
        """Periodic cumulative radio counters (hub/radio_health.py)."""
        from . import radio_health
        d = radio_health.parse_v1(bytes(body))
        if d is None:
            log.warning("[%s] RADIO_STATS: unknown format (%d B, v%s)", device_id_hex,
                        len(body), body[0] if body else "-")
            _note("radio stats: unknown format")
            return
        d.update(self._rx_counts.get(device_id_hex, {"hub_rx": 0, "hub_repeats": 0}))
        try:
            self._db.add_radio_stats(device_id_hex, int(time.time()), d)
        except Exception as e:                               # noqa: BLE001
            log.warning("[%s] RADIO_STATS store failed: %s", device_id_hex, e)
        busy_att = d["tx_ok"] + d["tx_cca_busy"] + d["tx_no_ack"] + d["tx_other"]
        _note("radio stats ch=%d %s parent=%s rssi=%s busy=%d/%d since boot"
              % (d["channel"], d["role"], d["parent"], d["parent_rssi"],
                 d["tx_cca_busy"], busy_att))

    def add_field_event_listener(self, cb) -> None:
        """cb(device_id_hex, field, value) on every pushed actuator change."""
        self._field_event_listeners.append(cb)

    def remember_field(self, device_id_hex: str, name: str, v: float) -> None:
        """Cache a value learned from a hub-initiated read/write (api.py),
        same shape as a device push.  Twin names (`X_v`) are the device's
        writable view of sensor X: store under the base name the cards map
        to."""
        base = name[:-2] if name.endswith("_v") else name
        self._field_cache.setdefault(device_id_hex, {})[base] = {
            "v": float(v), "ts": int(time.time()),
        }
        self._settle_desired(device_id_hex, base, float(v))

    # ── desired state ─────────────────────────────────────────────────
    DESIRED_TTL_S = 180        # past the failsafe's own retries; then drop
    SUPERSEDE_GRACE_S = 3      # a differing report this soon is a late echo

    def set_desired(self, device_id_hex: str, name: str, v: float,
                    by: str = "operator") -> None:
        base = name[:-2] if name.endswith("_v") else name
        cur = self._field_cache.get(device_id_hex, {}).get(base)
        if cur is not None and abs(cur["v"] - float(v)) < 1e-6:
            # already there: nothing is pending (a retransmitted trigger
            # event makes the failsafe re-assert values already reached)
            self._desired.get(device_id_hex, {}).pop(base, None)
            return
        self._desired.setdefault(device_id_hex, {})[base] = {
            "d": float(v), "at": time.time(), "by": by}

    def clear_desired(self, device_id_hex: str, name: str,
                      reason: str = "") -> None:
        base = name[:-2] if name.endswith("_v") else name
        e = self._desired.get(device_id_hex, {}).pop(base, None)
        if e and reason:
            log.info("[%s] desired %s=%s cleared: %s", device_id_hex, base,
                     e["d"], reason)

    def _settle_desired(self, device_id_hex: str, base: str, v: float) -> None:
        e = self._desired.get(device_id_hex, {}).get(base)
        if not e:
            return
        if abs(e["d"] - v) < 1e-6:
            self._desired[device_id_hex].pop(base, None)       # confirmed
        elif time.time() - e["at"] > self.SUPERSEDE_GRACE_S:
            # the device reported another value after our write settled:
            # something else (a rule, the button, another viewer) won
            self.clear_desired(device_id_hex, base, f"superseded by {v}")

    def get_field_cache(self, device_id_hex: str) -> dict:
        """Live field values the device reported (pushes and answered
        reads/writes), each with any pending desired state merged in:
        {"v", "ts"} + {"desired", "desired_at", "desired_by"}.  An entry
        may carry only the desired part when the device has not reported
        the field yet."""
        now = time.time()
        des = self._desired.get(device_id_hex, {})
        for k in [k for k, e in des.items() if now - e["at"] > self.DESIRED_TTL_S]:
            self.clear_desired(device_id_hex, k, "expired unconfirmed")
        cache = self._field_cache.get(device_id_hex, {})
        if not des:
            return cache
        out = {k: dict(e) for k, e in cache.items()}
        for k, e in des.items():
            out.setdefault(k, {}).update(
                desired=e["d"], desired_at=int(e["at"]), desired_by=e["by"])
        return out

    # ── Phase 2: symmetric session bootstrap ──────────────────────────────
    def set_enc_key(self, enc_priv) -> None:
        """Inject the hub's long-term X25519 private key (the session /
        ECIES root).  Called by serve() after the key is loaded."""
        self._enc_priv = enc_priv

    def _device_x25519_pub(self, device_id_hex: str) -> Optional[bytes]:
        pi = self._db.get_provision_info(device_id_hex)
        if not pi or not pi.enc_pubkey_b64:
            return None
        import base64
        return base64.b64decode(pi.enc_pubkey_b64)

    async def _establish_session(self, device_id_hex: str,
                                 dev_salt: bytes) -> None:
        """Device advertised its per-boot salt (in INFO_REPLY).  Mint a hub
        salt, derive the hub-side session from the static ECDH, remember it,
        and send SESS_INIT back so the device derives the mirror."""
        if self._enc_priv is None:
            return
        dev_pub = self._device_x25519_pub(device_id_hex)
        if dev_pub is None:
            log.debug("[%s] session: no device pubkey yet", device_id_hex)
            return
        import os
        from . import nn_session as ns
        try:
            ecdh = ns.static_ecdh(self._enc_priv, dev_pub)
        except Exception as e:
            log.warning("[%s] session ECDH failed: %s", device_id_hex, e)
            return
        hub_salt = os.urandom(ns.CTR_LEN)
        sess = ns.Session(ecdh, dev_salt, hub_salt, is_device=False)
        self._sessions[device_id_hex] = {"sess": sess, "verified": False,
                                         "hub_salt": hub_salt,
                                         "dev_salt": dev_salt}
        log.info("[%s] session derived (hub side), sending SESS_INIT",
                 device_id_hex)
        await self._send_sess_init(device_id_hex, hub_salt)
        # SESS_INIT is a single H2D frame over a mesh that drops 5-30 % per
        # hop.  Until 2026-09-18 a lost one left the device "derived but
        # never verified" until the next hub restart: no sealed ops, and no
        # group-key rotations, so after two daily rotations the device could
        # no longer open cascade notifies (c6-s2, then c6-s3, went dark that
        # way).  Re-send a few times while unverified; the device treats a
        # repeat INIT with the same hub salt as "re-probe", never re-derive.
        asyncio.create_task(self._sess_init_retry(device_id_hex, hub_salt))

    async def _send_sess_init(self, device_id_hex: str, hub_salt: bytes) -> None:
        inner = (int(proto.Cmd.SESS_INIT).to_bytes(2, "little")
                 + (0).to_bytes(4, "little") + hub_salt)
        await self.send_h2d(device_id_hex, inner)

    # delays (s) between SESS_INIT re-sends while the session stays unverified
    sess_init_retry_s: tuple = (8.0, 20.0, 45.0)

    async def _sess_init_retry(self, device_id_hex: str, hub_salt: bytes) -> None:
        for delay in self.sess_init_retry_s:
            await asyncio.sleep(delay)
            st = self._sessions.get(device_id_hex)
            if not st or st.get("hub_salt") != hub_salt or st.get("verified"):
                return                      # verified, or superseded by a new handshake
            log.info("[%s] session still unverified after %.0fs — re-sending SESS_INIT",
                     device_id_hex, delay)
            await self._send_sess_init(device_id_hex, hub_salt)

    async def _on_sess_hello(self, device_id_hex: str,
                             tid: int, body: bytes) -> None:
        """Small-frame salt carrier: the 537B INFO_REPLY that used to
        deliver the salt dies to 6LoWPAN fragment loss on weak links, so
        the device announces its 8-byte salt in a ~90B frame every few
        seconds until the handshake completes.  A repeat HELLO with the
        salt we already know means our SESS_INIT was lost — re-send it
        (same hub salt, so the device's idempotence guard re-probes
        instead of re-deriving)."""
        if len(body) != 8:
            return
        st = self._sessions.get(device_id_hex)
        if st and st.get("dev_salt") == body and st.get("sess") is not None:
            hub_salt = st.get("hub_salt")
            if hub_salt:
                log.info("[%s] SESS_HELLO repeat — re-sending SESS_INIT",
                         device_id_hex)
                await self._send_sess_init(device_id_hex, hub_salt)
            return
        if st and st.get("dev_salt") == body:
            return          # establish already in flight
        log.info("[%s] SESS_HELLO: new salt — establishing session",
                 device_id_hex)
        self._sessions[device_id_hex] = {"sess": None, "verified": False,
                                         "dev_salt": body}
        await self._establish_session(device_id_hex, body)

    def _group_key(self) -> tuple[int, bytes]:
        """Phase-4 cascade group key: minted once, persisted in settings.
        Rotation = bump epoch + new key (future: hook /auto/compile)."""
        import os
        epoch_s = self._db.get_setting("group_key_epoch")
        key_hex = self._db.get_setting("group_key_hex")
        if epoch_s and key_hex:
            return int(epoch_s), bytes.fromhex(key_hex)
        epoch, key = 1, os.urandom(32)
        self._db.set_setting("group_key_epoch", str(epoch))
        self._db.set_setting("group_key_hex", key.hex())
        log.info("group key minted (epoch %d)", epoch)
        return epoch, key

    # The cascade group key travels as ONE sealed H2D frame the device never
    # acknowledges (SESS_GROUP_KEY has no ack; the device logs "group key
    # installed" at INF, which is below the uploaded log level).  On the
    # bench mesh (5-30 % loss per hop) a device that missed the push kept a
    # stale epoch and logged "AUTO_NOTIFY_S open rv=-22" on every cascade
    # until the next rotation (c6-s3, 2026-09-20).  Three defences, all
    # idempotent on the device (re-delivery of the current epoch is a no-op):
    #   1. a push is a burst of GROUP_KEY_BURST frames, GROUP_KEY_GAP_S apart;
    #   2. every verified device is re-pushed every group_key_repush_s
    #      (setting, default 300 s), bounding a stale epoch to that window;
    #   3. an uploaded "AUTO_NOTIFY_S open rv=-22" / "GROUP_KEY open rv=" log
    #      line from a device triggers an immediate re-push to it.
    GROUP_KEY_BURST = 3
    GROUP_KEY_GAP_S = 0.4
    GROUP_KEY_LOG_REPUSH_MIN_S = 20.0

    async def _push_group_key(self, device_id_hex: str,
                              reason: str = "verified") -> None:
        """Deliver the group key SEALED over the device's verified
        session (tid=0; AAD = cmd+tid like every sealed cmd)."""
        st = self._sessions.get(device_id_hex)
        if not st or not st.get("verified") or st.get("sess") is None:
            return
        epoch, key = self._group_key()
        payload = epoch.to_bytes(4, "little") + key
        aad = struct.pack("<HI", proto.Cmd.SESS_GROUP_KEY, 0)
        ok = False
        for i in range(self.GROUP_KEY_BURST):
            if i:
                await asyncio.sleep(self.GROUP_KEY_GAP_S)
            sealed = st["sess"].seal(aad, payload)      # fresh counter per frame
            inner = (int(proto.Cmd.SESS_GROUP_KEY).to_bytes(2, "little")
                     + (0).to_bytes(4, "little") + sealed)
            ok = await self.send_h2d(device_id_hex, inner) or ok
        rec = self.group_key_pushes.setdefault(device_id_hex, {"pushes": 0})
        rec.update({"epoch": epoch, "at": time.time(), "reason": reason,
                    "pushes": rec.get("pushes", 0) + 1})
        log.info("[%s] group key pushed (epoch %d, x%d, %s) ok=%s",
                 device_id_hex, epoch, self.GROUP_KEY_BURST, reason, ok)

    async def repush_group_keys(self, reason: str = "periodic") -> int:
        """Re-deliver the current group key to every verified device."""
        n = 0
        for dev, st in list(self._sessions.items()):
            if st.get("verified") and st.get("sess") is not None:
                await self._push_group_key(dev, reason)
                n += 1
        return n

    async def group_key_repush_loop(self) -> None:
        """Bound the age of a missed push: setting group_key_repush_s
        (default 300 s; 0 disables), re-read each cycle."""
        while True:
            try:
                period = int(self._db.get_setting("group_key_repush_s", "300") or "300")
            except (TypeError, ValueError):
                period = 300
            if period <= 0:
                await asyncio.sleep(60)
                continue
            await asyncio.sleep(period)
            try:
                n = await self.repush_group_keys("periodic")
                log.info("group key re-pushed to %d verified device(s)", n)
            except Exception as e:                           # noqa: BLE001
                log.warning("group key re-push failed: %s", e)

    def _log_line_wants_group_key(self, device_id_hex: str, text: str) -> bool:
        if "AUTO_NOTIFY_S open rv=-22" not in text and "GROUP_KEY open rv=" not in text:
            return False
        rec = self.group_key_pushes.get(device_id_hex) or {}
        return time.time() - rec.get("at", 0) >= self.GROUP_KEY_LOG_REPUSH_MIN_S

    async def rotate_group_key(self) -> int:
        """Mint a NEW cascade group key (epoch+1), persist it, and push
        it to every device with a verified session.  Called on
        /auto/compile so a rules change doubles as a key rotation; the
        devices keep the previous epoch alive, so a mid-rotation fleet
        still cascades.  Returns the new epoch."""
        import os
        epoch = int(self._db.get_setting("group_key_epoch") or "0") + 1
        key = os.urandom(32)
        self._db.set_setting("group_key_epoch", str(epoch))
        self._db.set_setting("group_key_hex", key.hex())
        log.info("group key ROTATED (epoch %d)", epoch)
        for dev, st in list(self._sessions.items()):
            if st.get("verified"):
                await self._push_group_key(dev, "rotation")
        # a second round a little later catches devices whose whole burst
        # fell into one bad radio moment
        async def _second_round():
            await asyncio.sleep(15)
            await self.repush_group_keys("rotation+15s")
        asyncio.create_task(_second_round())
        return epoch

    async def _send_sess_probe_ack(self, device_id_hex: str,
                                   tid: int) -> None:
        inner = (int(proto.Cmd.SESS_PROBE_ACK).to_bytes(2, "little")
                 + int(tid).to_bytes(4, "little"))
        await self.send_h2d(device_id_hex, inner)

    async def _on_sess_probe(self, device_id_hex: str,
                             tid: int, body: bytes) -> None:
        """Device sealed a fixed probe under its freshly derived session.
        Open it with our mirror; matching plaintext proves both ends agree
        on keys over the real mesh.  Ack success (the device's reliable
        layer retransmits the SAME record until acked, so ack replays of
        an already-verified probe as well)."""
        st = self._sessions.get(device_id_hex)
        if not st or st.get("sess") is None:
            log.warning("[%s] SESS_PROBE but no hub session", device_id_hex)
            return
        aad = int(proto.Cmd.SESS_PROBE).to_bytes(2, "little")
        try:
            pt = st["sess"].open(aad, body)
        except ValueError as e:
            if "replay" in str(e) and st.get("verified"):
                await self._send_sess_probe_ack(device_id_hex, tid)
                # the device's reliable layer replays the probe until our
                # ack lands; one key burst per 20 s is plenty (s3 got
                # three bursts in 9 s on 2026-09-21 without this)
                rec = self.group_key_pushes.get(device_id_hex) or {}
                if time.time() - rec.get("at", 0) >= self.GROUP_KEY_LOG_REPUSH_MIN_S:
                    await self._push_group_key(device_id_hex, "probe-replay")
            else:
                log.warning("[%s] SESS_PROBE open FAILED: %s", device_id_hex, e)
            return
        except Exception as e:
            log.warning("[%s] SESS_PROBE open FAILED: %s — key mismatch",
                        device_id_hex, e)
            return
        ok = (pt == b"nn-sess-probe!!!")
        st["verified"] = ok
        log.info("[%s] SESSION VERIFIED over mesh: %s (%d B probe)",
                 device_id_hex, ok, len(pt))
        if ok:
            await self._send_sess_probe_ack(device_id_hex, tid)
            await self._push_group_key(device_id_hex)

    def session_status(self, device_id_hex: str) -> dict:
        st = self._sessions.get(device_id_hex)
        out = {"established": False} if not st else \
              {"established": True, "verified": st.get("verified", False)}
        rec = self.group_key_pushes.get(device_id_hex)
        if rec:
            out["group_key"] = {"epoch": rec.get("epoch"), "pushed_at": int(rec.get("at", 0)),
                                "pushes": rec.get("pushes", 0), "reason": rec.get("reason")}
        try:
            out["hub_group_key_epoch"] = int(self._db.get_setting("group_key_epoch") or "0")
        except (TypeError, ValueError):
            pass
        return out

    # We feed it into the device-log store (the same store the legacy
    # /log CoAP resource used to write to).
    async def _on_log_line(self, device_id_hex: str,
                           tid: int, body: bytes) -> None:
        try:
            text = body.decode("utf-8", errors="replace").rstrip()
        except Exception:
            return
        if not text:
            return
        # closed loop: a device that cannot open its neighbours' sealed
        # cascade notifies is telling us it missed the group key
        if self._log_line_wants_group_key(device_id_hex, text):
            log.warning("[%s] reports an unknown group-key epoch (%s) — re-pushing",
                        device_id_hex, text[:60])
            asyncio.create_task(self._push_group_key(device_id_hex, "device-log"))
        store = getattr(self, "_log_store", None)
        if store is None:
            # log_store is injected by serve() — fall back to stdout
            log.info("[%s] %s", device_id_hex, text)
            return
        try:
            store.append_text(device_id=device_id_hex, text=text)
        except Exception as e:
            log.warning("[%s] log_store append failed: %s", device_id_hex, e)

    def attach_log_store(self, log_store) -> None:
        """Called by serve() once log_store is constructed.  Hub-side
        consumers of LOG_LINE land in this store."""
        self._log_store = log_store

    # ── control-plane handler: OTA_CHECK / OTA_BLOCK_REQ ───────────────
    #
    # Replaces the legacy CoAP /ota/check + /ota/image Block2 endpoints
    # that lived in hub.coap_server.  Sensor sends OTA_CHECK with a JSON
    # body {"type":..,"version":..}; hub looks up the registered
    # firmware target for that type and replies with OTA_MANIFEST.  If a
    # newer version is available, the sensor follows up with N
    # OTA_BLOCK_REQ [u32 block_num | u16 size] and hub replies with
    # OTA_BLOCK [u32 block_num | raw bytes].

    OTA_BLOCK_DEFAULT_SIZE = 512   # was 1024; less 6LoWPAN fragmentation
    OTA_BLOCK_MAX_SIZE     = 1024

    # ── applying-state watchdog ─────────────────────────────────────────
    #
    # The device's post-OTA-reboot unsolicited INFO_REPLY is a single
    # fire-and-forget UDP datagram (4+ 6LoWPAN fragments) and gets lost
    # on the mesh often enough that the hub can sit in 'applying'
    # forever even though the device swapped + booted the target image
    # long ago.  This watchdog actively polls devices stuck in
    # 'applying' with INFO_QUERY via the retrying request_h2d transport;
    # the INFO_REPLY (paired this time, and retried up to 5×) feeds
    # _consume_info_reply which flips the state to done.
    #
    # Works for ALL firmware versions — even ones that never send the
    # unsolicited INFO_REPLY at all.

    OTA_APPLY_POLL_INTERVAL_S = 60
    OTA_APPLY_POLL_MAX_AGE_S  = 3600   # stop polling after 1h in applying

    async def ota_applying_watchdog(self) -> None:
        """Background task: periodically INFO_QUERY devices whose OTA
        state says all blocks delivered but running != target."""
        while True:
            await asyncio.sleep(self.OTA_APPLY_POLL_INTERVAL_S)
            now = int(time.time())
            for did, st in list(self._ota_state.items()):
                # Two pollable situations:
                #   a) auto-apply in flight: all blocks delivered, waiting
                #      for the reboot + INFO_REPLY (legacy/auto path).
                #   b) operator apply sent to an armed device: the ACK or
                #      the post-boot INFO_REPLY may be lost on the mesh —
                #      poll until the new version confirms.
                # Parked armed devices (no apply sent) are NOT polled;
                # the operator may take days to confirm.
                apply_pending = (st.get("armed")
                                 and st.get("apply_sent_at"))
                if apply_pending:
                    if (now - int(st["apply_sent_at"])) \
                            > self.OTA_APPLY_POLL_MAX_AGE_S:
                        continue
                else:
                    if not st.get("in_flight"):
                        continue
                    if st.get("armed"):
                        continue  # parked, awaiting operator
                    total = int(st.get("total_blocks", 0))
                    if total <= 0 or \
                            int(st.get("highest_block", -1)) < total - 1:
                        continue  # still downloading
                    last_at = int(st.get("last_block_at", 0))
                    if last_at and (now - last_at) \
                            > self.OTA_APPLY_POLL_MAX_AGE_S:
                        continue  # surface as stalled via get_ota_status
                try:
                    await self.request_h2d(
                        did,
                        req_cmd=proto.Cmd.INFO_QUERY,
                        body=b"",
                        expected_reply_cmd=proto.Cmd.INFO_REPLY,
                        timeout=8.0,
                        max_attempts=2,
                    )
                    # reply body already consumed by _consume_info_reply
                    # via the handle-frame path before future resolution.
                except (asyncio.TimeoutError, Exception) as e:
                    log.debug("[%s] applying-watchdog INFO_QUERY: %s",
                              did, e)

    def _consume_info_reply(self, device_id_hex: str, body: bytes) -> None:
        """Parse the JSON body of an INFO_REPLY (paired or unsolicited) and
        update per-device state — primarily the OTA state-machine's
        `running_version`, which otherwise only gets refreshed during an
        OTA_CHECK.  This is what unblocks the 'applying → done' transition
        after an OTA reboot, where the device boots the new image and emits
        an unsolicited INFO_REPLY with `firmware` set to the new version.

        Best-effort: a bad JSON body is logged + dropped.
        """
        import json
        try:
            doc = json.loads(body.decode("utf-8", errors="replace") or "{}")
        except Exception as e:
            log.debug("[%s] INFO_REPLY bad json: %s", device_id_hex, e)
            return
        if not isinstance(doc, dict):
            return
        # Armed state piggybacks on the config sync so the hub learns it
        # even when the fire-and-forget OTA_READY frame is lost.  An
        # empty string means "not armed" (e.g. cleared after an apply).
        armed = doc.get("armed")
        if isinstance(armed, str):
            st = self._ota_state.setdefault(device_id_hex, {})
            if armed:
                if not st.get("armed") or st.get("armed_version") != armed:
                    log.info("[%s] armed for %s (via INFO_REPLY)",
                             device_id_hex, armed)
                st["armed"] = True
                st["armed_version"] = armed
                st.setdefault("armed_at", int(time.time()))
                st["in_flight"] = False
            elif st.get("armed"):
                st["armed"] = False
                st.pop("armed_version", None)
                st.pop("apply_sent_at", None)

        # Image identity: the firmware self-reports WHAT image it runs
        # (build-time app name).  Stored per device; every OTA catalog
        # lookup prefers this over the registration-era device_type, so
        # one image name keys the catalog no matter how the device was
        # provisioned (see the s1 end_device vs sample_c6 split).
        img = doc.get("img")
        if isinstance(img, str) and img:
            key = f"device_image:{device_id_hex}"
            if self._db.get_setting(key) != img:
                self._db.set_setting(key, img)
                log.info("[%s] image identity: %s", device_id_hex, img)

        # Phase 2: device advertises its per-boot session salt here.  A new
        # salt means a fresh boot → (re)establish the symmetric session.
        sess_hex = doc.get("sess")
        if isinstance(sess_hex, str) and len(sess_hex) == 16:
            try:
                dev_salt = bytes.fromhex(sess_hex)
            except ValueError:
                dev_salt = None
            if dev_salt is not None:
                st = self._sessions.get(device_id_hex)
                if not st or st.get("dev_salt") != dev_salt:
                    import asyncio
                    task = asyncio.create_task(
                        self._establish_session(device_id_hex, dev_salt))
                    # remember the salt immediately so a duplicate
                    # INFO_REPLY doesn't kick a second handshake
                    self._sessions[device_id_hex] = {
                        "sess": None, "verified": False,
                        "dev_salt": dev_salt, "task": task}

        # Persist the config-sync payload the devices webapp renders from:
        # `fields` = [{"n","t","min","max"}, ...] field descriptors
        # (t: 0=read-only sensor, 1=writable actuator) → capabilities,
        # plus eui64 + a last_info_at stamp.
        fields = doc.get("fields")
        if isinstance(fields, list):
            eui64 = doc.get("eui64")
            try:
                if self._db.update_device_info_sync(
                        device_id_hex,
                        capabilities_json=json.dumps(fields),
                        eui64=eui64 if isinstance(eui64, str) else None):
                    log.debug("[%s] config sync: %d field descriptors stored",
                              device_id_hex, len(fields))
            except Exception as e:
                log.warning("[%s] config sync persist failed: %s",
                            device_id_hex, e)

        firmware = doc.get("firmware")
        if firmware and isinstance(firmware, str):
            self._note_running(device_id_hex, firmware)
            st = self._ota_state.get(device_id_hex)
            if st is not None:
                prev = st.get("running_version", "")
                if prev != firmware:
                    st["running_version"] = firmware
                    log.info("[%s] running_version: %r → %r (INFO_REPLY)",
                             device_id_hex, prev, firmware)
                # Zephyr's APP_VERSION_TWEAK_STRING produces a `MAJOR.MINOR.PATCH+TWEAK`
                # string ("0.0.2+0"); hub stores the target as plain
                # `MAJOR.MINOR.PATCH` ("0.0.2").  Compare base versions
                # only so 'applying' flips to 'done' regardless of tweak.
                def _base_ver(v: str) -> str:
                    return v.split("+", 1)[0] if v else ""
                if (st.get("in_flight")
                        and _base_ver(firmware) == _base_ver(st.get("target_version", ""))
                        and _base_ver(firmware)):
                    st["in_flight"] = False
                    log.info("[%s] OTA complete: running %r matches target %r",
                             device_id_hex, firmware, st.get("target_version"))

    def _note_running(self, device_id_hex: str, version: str) -> None:
        """Persist a device's self-reported running firmware version
        (settings-KV row, same pattern as device_image) and wake fleet-WS
        subscribers when it changed.  The KV copy is what lets the Fleet
        view answer from cache across hub restarts instead of re-asking
        every sensor over the mesh."""
        if not version:
            return
        import time as _t
        key = f"device_running:{device_id_hex}"
        try:
            changed = self._db.get_setting(key) != version
            if changed:
                self._db.set_setting(key, version)
            self._db.set_setting(f"device_running_at:{device_id_hex}",
                                 str(int(_t.time())))
        except Exception as e:
            log.warning("[%s] running_version persist failed: %s",
                        device_id_hex, e)
            return
        if changed:
            for q in list(self._fleet_subs):
                try:
                    q.put_nowait({"device_id": device_id_hex,
                                  "running": version})
                except Exception:
                    pass

    def subscribe_fleet(self):
        import asyncio
        q: asyncio.Queue = asyncio.Queue(maxsize=64)
        self._fleet_subs.add(q)
        return q

    def unsubscribe_fleet(self, q) -> None:
        self._fleet_subs.discard(q)

    async def _on_ota_check(self, device_id_hex: str,
                            tid: int, body: bytes) -> None:
        import json
        device_type = ""
        running_ver = ""
        try:
            doc = json.loads(body.decode("utf-8", errors="replace") or "{}")
            device_type = str(doc.get("type", ""))
            running_ver = str(doc.get("version", ""))
        except Exception as e:
            log.warning("[%s] OTA_CHECK bad json: %s", device_id_hex, e)

        target = None
        try:
            if device_type:
                target = self._db.get_firmware_target(device_type)
        except Exception as e:
            log.warning("[%s] OTA_CHECK lookup err: %s", device_id_hex, e)

        # Track in-flight state for the REST `GET /devices/{dev}/ota`
        # endpoint.  Always record the running version reported by the
        # device, plus the offer if any.
        st = self._ota_state.get(device_id_hex, {})
        st["running_version"] = running_ver
        self._note_running(device_id_hex, running_ver)

        # Base-version comparison: the device reports Zephyr's extended
        # version string ("0.0.4+0"); the hub stores the target as plain
        # semver ("0.0.4").  Raw string comparison would re-offer the
        # same image forever to an up-to-date device.
        def _base_ver(v: str) -> str:
            return v.split("+", 1)[0] if v else ""

        # catalog key: the device's self-reported image name wins over
        # the device_type it sent in the OTA_CHECK body
        img = self._db.get_setting(f"device_image:{device_id_hex}")
        if img:
            t2 = self._db.get_firmware_target(img)
            if t2 is not None:
                target = t2
                device_type = img

        if target is None or _base_ver(target.target_version) == _base_ver(running_ver):
            reply = b'{"update":false}'
            log.info("[%s] OTA_CHECK type=%s cur=%s → up-to-date",
                     device_id_hex, device_type, running_ver)
            st["in_flight"] = False
            st["target_version"] = (target.target_version
                                    if target else "")
            st["target_size"]    = (int(target.size_bytes)
                                    if target else 0)
            st["total_blocks"]   = (
                (int(target.size_bytes) + self.OTA_BLOCK_DEFAULT_SIZE - 1)
                // self.OTA_BLOCK_DEFAULT_SIZE) if target else 0
        else:
            doc_out = {
                "update":  True,
                "version": target.target_version,
                "size":    int(target.size_bytes),
                "sha256":  target.sha256,
            }
            # Offer a binary delta when we can build one from the device's
            # running image (cached) to the target.  The device decides
            # whether to use it (patch mode) or ignore it and full-download.
            patch = self._maybe_build_patch(device_type, running_ver, target)
            if patch is not None:
                doc_out["patch"] = {
                    "size":   patch["size_bytes"],
                    "sha256": patch["sha256"],
                }
                st["patch_path"]   = str(patch["path"])
                st["patch_size"]   = patch["size_bytes"]
            else:
                st.pop("patch_path", None)
                st.pop("patch_size", None)
            # Compact separators (no spaces): the offer reply carries two
            # 64-char sha256 hex strings and fragments across several
            # 6LoWPAN frames; losing any frame loses the whole reply on a
            # weak path.  Trimming whitespace shaves a fragment.  The
            # device parser keys off `"key"` tokens and tolerates the
            # absence of spaces, so this stays wire-compatible.
            reply = json.dumps(doc_out, separators=(",", ":")).encode("utf-8")
            log.info("[%s] OTA_CHECK type=%s cur=%s → offer %s (%d B%s)",
                     device_id_hex, device_type, running_ver,
                     target.target_version, target.size_bytes,
                     f", patch {patch['size_bytes']}B" if patch else "")
            st["target_version"] = target.target_version
            st["target_size"]    = int(target.size_bytes)
            st["total_blocks"]   = (
                (int(target.size_bytes) + self.OTA_BLOCK_DEFAULT_SIZE - 1)
                // self.OTA_BLOCK_DEFAULT_SIZE)
            st["in_flight"]      = True
            st.setdefault("started_at", int(time.time()))
            # Fresh download — reset the block counter (but only when
            # we're transitioning from idle/done into an in-flight run).
            if st.get("highest_block", -1) >= st["total_blocks"] - 1:
                st["highest_block"] = -1
                st["last_block_at"] = 0
                st["started_at"]    = int(time.time())
        self._ota_state[device_id_hex] = st
        await self._send_h2d_reply(device_id_hex, proto.Cmd.OTA_MANIFEST,
                                   tid, reply)

    def _maybe_build_patch(self, device_type: str, running_ver: str,
                           target) -> Optional[dict]:
        """Generate (or reuse) a detools delta from the device's running
        image to the target.  Returns {path, size_bytes, sha256} or None
        when the from-image isn't cached / detools is unavailable."""
        try:
            from . import ota_patch
            from pathlib import Path
            cache_dir = Path(target.firmware_path).parent.parent.parent
            return ota_patch.ensure_patch(
                self._db, cache_dir, device_type,
                running_ver, Path(target.firmware_path))
        except Exception as e:
            log.warning("[%s] patch build failed: %s", device_type, e)
            return None

    async def _on_ota_patch_req(self, device_id_hex: str,
                                tid: int, body: bytes) -> None:
        from pathlib import Path
        if len(body) < 6:
            log.warning("[%s] OTA_PATCH_REQ short body=%dB",
                        device_id_hex, len(body))
            return
        offset, size = struct.unpack_from("<IH", body, 0)
        if size == 0 or size > self.OTA_BLOCK_MAX_SIZE:
            size = self.OTA_BLOCK_DEFAULT_SIZE

        st = self._ota_state.get(device_id_hex, {})
        patch_path = st.get("patch_path")
        patch_size = int(st.get("patch_size", 0))
        if not patch_path:
            log.warning("[%s] OTA_PATCH_REQ but no patch offered",
                        device_id_hex)
            return
        p = Path(patch_path)
        if not p.is_file():
            log.error("[%s] patch file missing: %s", device_id_hex, p)
            return
        if offset >= patch_size:
            log.warning("[%s] OTA_PATCH_REQ offset %u past EOF (%u)",
                        device_id_hex, offset, patch_size)
            return
        with open(p, "rb") as f:
            f.seek(offset)
            chunk = f.read(size)

        # Reuse the OTA progress tracker so GET /ota shows patch progress.
        st["in_flight"] = True
        st["last_block_at"] = int(time.time())
        st["patch_recv"] = max(int(st.get("patch_recv", 0)), offset + len(chunk))
        self._ota_state[device_id_hex] = st

        reply = struct.pack("<I", offset) + chunk
        await self._send_h2d_reply(device_id_hex, proto.Cmd.OTA_PATCH,
                                   tid, reply)

    async def _on_ota_block_req(self, device_id_hex: str,
                                tid: int, body: bytes) -> None:
        from pathlib import Path
        if len(body) < 6:
            log.warning("[%s] OTA_BLOCK_REQ short body=%dB",
                        device_id_hex, len(body))
            return
        block_num, size = struct.unpack_from("<IH", body, 0)

        # Update OTA progress tracker (exposed via REST).  Hub may have
        # restarted mid-OTA — populate target_version + sizing fields
        # from the firmware row so progress_percent isn't 0 in that case.
        st = self._ota_state.setdefault(device_id_hex, {})
        st["in_flight"]     = True
        st["highest_block"] = max(int(st.get("highest_block", -1)),
                                  int(block_num))
        st["last_block_at"] = int(time.time())
        st.setdefault("started_at", int(time.time()))
        if size == 0 or size > self.OTA_BLOCK_MAX_SIZE:
            size = self.OTA_BLOCK_DEFAULT_SIZE

        # Look up the device's registered type → resolves to a single
        # active firmware target.  We trust the hub-side registry rather
        # than echo back whatever the device sent.
        device_type = None
        try:
            dev = self._db.get_device(device_id_hex)
            if dev is not None:
                device_type = dev.type
            if not device_type:
                pi = self._db.get_provision_info(device_id_hex)
                if pi is not None:
                    device_type = pi.device_type
        except Exception as e:
            log.warning("[%s] OTA_BLOCK_REQ dev lookup: %s", device_id_hex, e)

        if not device_type:
            log.warning("[%s] OTA_BLOCK_REQ no device_type registered",
                        device_id_hex)
            return
        # Same image-identity preference as OTA_CHECK — the CHECK offers
        # the image-keyed target's manifest, so the BLOCKS must stream
        # from the SAME target or the device downloads a mismatched
        # image (observed: swap "succeeds" back onto the old version).
        img = self._db.get_setting(f"device_image:{device_id_hex}")
        if img and self._db.get_firmware_target(img) is not None:
            device_type = img
        target = self._db.get_firmware_target(device_type)
        if target is None:
            log.warning("[%s] OTA_BLOCK_REQ no firmware target for %s",
                        device_id_hex, device_type)
            return

        # Backfill OTA-tracker target info if missing (hub-restart-during-
        # OTA case, where the tracker missed the OTA_CHECK that would
        # normally populate these).
        if not st.get("target_version"):
            st["target_version"] = target.target_version
            st["target_size"]    = int(target.size_bytes)
            st["total_blocks"]   = (
                (int(target.size_bytes) + self.OTA_BLOCK_DEFAULT_SIZE - 1)
                // self.OTA_BLOCK_DEFAULT_SIZE)

        fw_path = Path(target.firmware_path)
        if not fw_path.exists():
            log.error("[%s] firmware file missing: %s",
                      device_id_hex, fw_path)
            return

        # Offset MUST use the canonical OTA_BLOCK_DEFAULT_SIZE, NOT the
        # per-request `size`.  The last block of an OTA download has
        # `size = remaining_bytes < OTA_BLOCK_DEFAULT_SIZE`; computing
        # offset from that partial size puts the seek deep inside the
        # file (e.g. block_num=2006 with size=380 → offset 762280 instead
        # of the intended 1027072), which corrupts the last block of
        # slot1 with bytes from the middle of the image, breaking
        # MCUboot's hash check on the next boot.  Diagnosed 2026-05-19.
        offset = block_num * self.OTA_BLOCK_DEFAULT_SIZE
        if offset >= target.size_bytes:
            log.warning("[%s] OTA_BLOCK_REQ block=%u past EOF (size=%d)",
                        device_id_hex, block_num, target.size_bytes)
            return
        with open(fw_path, "rb") as f:
            f.seek(offset)
            chunk = f.read(size)

        reply = struct.pack("<I", block_num) + chunk
        await self._send_h2d_reply(device_id_hex, proto.Cmd.OTA_BLOCK,
                                   tid, reply)

    # ── OTA chunk checksums (chunk-diff download) ───────────────────────
    #
    # The device asks for the target image's per-block checksum table so
    # it can skip downloading blocks it already holds (slot 1 leftovers
    # from an interrupted run) or can copy locally from the running
    # image (slot 0) when only a few blocks changed between versions.
    # Page format keeps each reply under one OTA block:
    #   request : [u32 LE first_block | u16 LE count]
    #   reply   : [u32 LE first_block | count × 8B truncated sha256]

    OTA_CHUNKSUM_LEN       = 8     # truncated sha256 per block
    OTA_CHUNKSUMS_PER_PAGE = 63    # 4 + 63*8 = 508 B ≤ one OTA block

    def _fw_target_for_device(self, device_id_hex: str):
        """Resolve the device's registered type → active firmware target.
        Returns (device_type, FirmwareTarget) or (None, None)."""
        device_type = None
        try:
            dev = self._db.get_device(device_id_hex)
            if dev is not None:
                device_type = dev.type
            if not device_type:
                pi = self._db.get_provision_info(device_id_hex)
                if pi is not None:
                    device_type = pi.device_type
        except Exception as e:
            log.warning("[%s] fw-target dev lookup: %s", device_id_hex, e)
        if not device_type:
            return None, None
        img = self._db.get_setting(f"device_image:{device_id_hex}")
        if img and self._db.get_firmware_target(img) is not None:
            device_type = img
        return device_type, self._db.get_firmware_target(device_type)

    def _chunksum_table(self, fw_path, size_bytes: int) -> bytes:
        """Return the per-block truncated-sha256 table for *fw_path*,
        using a `<path>.chunksums` sidecar as cache.  Regenerates when
        the sidecar is missing or its length disagrees with the image."""
        import hashlib
        from pathlib import Path
        fw_path = Path(fw_path)
        n_blocks = (size_bytes + self.OTA_BLOCK_DEFAULT_SIZE - 1) \
                   // self.OTA_BLOCK_DEFAULT_SIZE
        expect_len = n_blocks * self.OTA_CHUNKSUM_LEN
        sidecar = fw_path.with_suffix(fw_path.suffix + ".chunksums")
        if sidecar.is_file() and sidecar.stat().st_size == expect_len:
            return sidecar.read_bytes()
        data = fw_path.read_bytes()
        out = bytearray()
        for i in range(n_blocks):
            blk = data[i * self.OTA_BLOCK_DEFAULT_SIZE:
                       (i + 1) * self.OTA_BLOCK_DEFAULT_SIZE]
            out += hashlib.sha256(blk).digest()[:self.OTA_CHUNKSUM_LEN]
        tmp = sidecar.with_suffix(sidecar.suffix + ".tmp")
        tmp.write_bytes(bytes(out))
        tmp.replace(sidecar)
        log.info("chunksum table built for %s (%d blocks)", fw_path, n_blocks)
        return bytes(out)

    async def _on_ota_chunksums_req(self, device_id_hex: str,
                                    tid: int, body: bytes) -> None:
        from pathlib import Path
        if len(body) < 6:
            log.warning("[%s] OTA_CHUNKSUMS_REQ short body=%dB",
                        device_id_hex, len(body))
            return
        first_block, count = struct.unpack_from("<IH", body, 0)
        count = min(count or self.OTA_CHUNKSUMS_PER_PAGE,
                    self.OTA_CHUNKSUMS_PER_PAGE)
        # Optional 7th byte selects the artifact the block indices address:
        # 0 = target image (chunk-diff), 1 = staged detools patch (delta
        # resume).  Absent (legacy 6-byte) => image.
        target_kind = body[6] if len(body) >= 7 else 0

        if target_kind == 1:
            # Patch chunksums: the patch path + size were recorded by
            # _on_ota_check when it offered the delta.
            st = self._ota_state.get(device_id_hex, {})
            patch_path = st.get("patch_path")
            patch_size = int(st.get("patch_size") or 0)
            if not patch_path or patch_size <= 0:
                log.warning("[%s] OTA_CHUNKSUMS_REQ(patch) no patch offered",
                            device_id_hex)
                return
            if not Path(patch_path).exists():
                log.error("[%s] patch file missing: %s",
                          device_id_hex, patch_path)
                return
            table = self._chunksum_table(patch_path, patch_size)
        else:
            device_type, target = self._fw_target_for_device(device_id_hex)
            if target is None:
                log.warning("[%s] OTA_CHUNKSUMS_REQ no firmware target (type=%s)",
                            device_id_hex, device_type)
                return
            if not Path(target.firmware_path).exists():
                log.error("[%s] firmware file missing: %s",
                          device_id_hex, target.firmware_path)
                return
            table = self._chunksum_table(target.firmware_path,
                                         int(target.size_bytes))
        n_blocks = len(table) // self.OTA_CHUNKSUM_LEN
        if first_block >= n_blocks:
            log.warning("[%s] OTA_CHUNKSUMS_REQ first_block=%u past end (%u)",
                        device_id_hex, first_block, n_blocks)
            return
        end = min(first_block + count, n_blocks)
        page = table[first_block * self.OTA_CHUNKSUM_LEN:
                     end * self.OTA_CHUNKSUM_LEN]
        reply = struct.pack("<I", first_block) + page
        await self._send_h2d_reply(device_id_hex, proto.Cmd.OTA_CHUNKSUMS,
                                   tid, reply)

    # ── OTA_READY: device downloaded + verified, armed, awaiting apply ──

    def note_apply_sent(self, device_id_hex: str) -> None:
        """Record that an OTA_APPLY was dispatched so the applying-
        watchdog confirms the outcome even if the device's ACK is lost."""
        st = self._ota_state.setdefault(device_id_hex, {})
        st["apply_sent_at"] = int(time.time())

    async def _on_ota_ready(self, device_id_hex: str,
                            tid: int, body: bytes) -> None:
        import json
        version = ""
        sha256 = ""
        try:
            doc = json.loads(body.decode("utf-8", errors="replace") or "{}")
            version = str(doc.get("version", ""))
            sha256 = str(doc.get("sha256", ""))
        except Exception as e:
            log.warning("[%s] OTA_READY bad json: %s", device_id_hex, e)
        st = self._ota_state.setdefault(device_id_hex, {})
        # Validate the reported sha against the ACTIVE target before
        # trusting the arm: a device armed with different bytes than the
        # target (hub-side serving bug, catalog swap mid-download) must
        # never be applied.  Second half of the pre-arm verify pair —
        # the device checks its slot against the manifest; we check the
        # report against the target.
        _, target = self._fw_target_for_device(device_id_hex)
        if (target is not None and sha256 and
                sha256.lower() != (target.sha256 or "").lower()):
            st["armed"] = False
            st["armed_version"] = ""
            st["in_flight"] = False
            st["state_note"] = "armed-sha-mismatch"
            log.error("[%s] OTA_READY sha MISMATCH: device armed %s "
                      "(%s…) but target %s is %s… — REJECTING arm",
                      device_id_hex, version, sha256[:12],
                      target.target_version, (target.sha256 or "")[:12])
            return
        st["armed"] = True
        st["armed_version"] = version
        st["armed_sha256"] = sha256
        st["armed_at"] = int(time.time())
        st["in_flight"] = False
        log.info("[%s] OTA_READY: armed for %s (awaiting operator apply)",
                 device_id_hex, version)

    # ── inbound dispatch ────────────────────────────────────────────────

    async def handle_frame(self, gateway_id: str, frame: proto.Frame):
        """Called by ProtoServer for each frame received on a gateway TCP."""
        type_name = {
            proto.FrameType.D2H: "D2H",
            proto.FrameType.H2D: "H2D",
            proto.FrameType.D2G: "D2G",
            proto.FrameType.G2D: "G2D",
        }.get(frame.type, f"0x{frame.type:04x}")
        did = frame.device_id.hex() if frame.device_id else "(none)"

        if frame.type == proto.FrameType.D2H:
            # Provisioning may leave no gateway_id, and the device's first
            # frames can arrive BEFORE the provisioning job writes its row
            # (with gateway_id ""), so check until a row with a gateway exists.
            if did not in self._gw_recorded:
                try:
                    pi = self._db.get_provision_info(did)
                    if pi is not None:
                        if not pi.gateway_id and \
                                self._db.set_device_gateway_if_empty(did, gateway_id):
                            log.info("[%s] recorded gateway %s (provisioning left none)",
                                     did, gateway_id[:8])
                        self._gw_recorded.add(did)
                except Exception as e:                       # noqa: BLE001
                    log.debug("gateway record for %s: %s", did, e)
            self._device_route[did] = gateway_id
            log.debug("[gw=%s] D2H from device=%s payload=%dB (route cached)",
                      gateway_id, did, len(frame.payload))
            # One line per device message: when it arrived, what it was,
            # how long handling took and what it did.  The journal stamps
            # the line when handling FINISHES, so the arrival time is
            # written into the line itself.
            t_rx = time.time()
            if self._is_burst_copy(did, frame.payload, t_rx):
                return
            _rx_note.set("")
            try:
                await self._on_d2h(gateway_id, did, frame.payload)
            finally:
                self._log_rx(gateway_id, did, frame.payload, t_rx,
                             time.time() - t_rx, _rx_note.get())
        elif frame.type == proto.FrameType.D2G:
            try:
                cmd, args = proto.decode_inner(frame.payload)
                # every gateway sends 7 of these every 5 s: DEBUG, or the
                # device messages drown in them (170 lines/min)
                log.debug("[gw=%s] D2G did=%s cmd=0x%04x args=%dB",
                         gateway_id, did, cmd, len(args))
            except proto.ProtoError as e:
                log.warning("[gw=%s] D2G inner parse: %s", gateway_id, e)
            else:
                if cmd == proto.Cmd.GATEWAY_THREAD_STATE:
                    self._on_gateway_thread_state(gateway_id, args)
                elif cmd == proto.Cmd.DEVICE_THREAD_STATE:
                    self._on_device_thread_state(gateway_id, args)
                elif cmd == proto.Cmd.GW_H2D_UNDELIVERABLE:
                    await self._on_h2d_undeliverable(gateway_id, args)
                elif cmd in (proto.Cmd.GW_CHANNEL_SCAN_RESULT,
                             proto.Cmd.GW_CHANNEL_SET_RESULT,
                             proto.Cmd.GW_DATASET,
                             proto.Cmd.GW_ROUTE_SET_RESULT) and len(args) >= 5:
                    tid = struct.unpack_from("<I", args, 0)[0]
                    fut = self._pending_gw.get(tid)
                    if fut is not None and not fut.done():
                        fut.set_result((int(cmd), args[4:]))
                    log.info("[gw=%s] %s tid=0x%08x status=%d %dB",
                             gateway_id[:8], proto.Cmd(cmd).name, tid,
                             struct.unpack_from("<b", args, 4)[0], len(args) - 5)
        else:
            log.info("[gw=%s] %s did=%s payload=%dB",
                     gateway_id, type_name, did, len(frame.payload))

    def _on_gateway_thread_state(self, gateway_id: str, args: bytes) -> None:
        """Persist a GATEWAY_THREAD_STATE inner payload.

        Layout:
          1B role (spinel net role 0..4)
          2B rloc16 (little-endian)
         16B mesh-local EID (full IPv6)
        """
        if len(args) < 19:
            log.warning("[gw=%s] GATEWAY_THREAD_STATE short args=%dB",
                        gateway_id, len(args))
            return
        role = args[0]
        rloc16 = int.from_bytes(args[1:3], "little")
        mleid_hex = args[3:19].hex()
        try:
            self._db.update_gateway_thread_state(gateway_id, role, rloc16,
                                                 mleid_hex)
        except Exception as e:  # pragma: no cover — DB writes are local
            log.warning("[gw=%s] update_gateway_thread_state: %s",
                        gateway_id, e)
            return
        from .db import role_name
        # every 5 s per gateway: say it only when it changes
        prev = self._gw_state_seen.get(gateway_id)
        self._gw_state_seen[gateway_id] = (role, rloc16)
        (log.info if prev != (role, rloc16) else log.debug)(
            "[gw=%s] thread state: role=%s(%d) rloc16=0x%04x",
            gateway_id, role_name(role), role, rloc16)

    def _on_device_thread_state(self, gateway_id: str, args: bytes) -> None:
        """Persist a DEVICE_THREAD_STATE inner payload.

        Layout:
          2B did_size (little-endian)
          N  device_id bytes
         16B device mesh-local EID (full IPv6, current ML-EID)

        The gateway caches every device's ML-EID in proto_router on every
        inbound D2H/D2G and periodically replays the cache to us so we
        can self-heal the per-device ml_eid stored at provisioning time
        but invalidated by every sensor reboot.
        """
        if len(args) < 2:
            log.warning("[gw=%s] DEVICE_THREAD_STATE short args=%dB",
                        gateway_id, len(args))
            return
        did_size = int.from_bytes(args[0:2], "little")
        if len(args) < 2 + did_size + 16:
            log.warning("[gw=%s] DEVICE_THREAD_STATE truncated did_size=%d args=%dB",
                        gateway_id, did_size, len(args))
            return
        device_id_hex = args[2:2 + did_size].hex()
        ml_eid_bytes = args[2 + did_size:2 + did_size + 16]
        import ipaddress
        try:
            ml_eid_str = str(ipaddress.IPv6Address(bytes(ml_eid_bytes)))
        except ValueError as e:
            log.warning("[gw=%s] DEVICE_THREAD_STATE bad addr did=%s: %s",
                        gateway_id, device_id_hex, e)
            return
        try:
            pi = self._db.get_provision_info(device_id_hex)
            if pi is None:
                return  # device unknown — gateway saw it but hub never provisioned
            if pi.ml_eid == ml_eid_str:
                return  # already up to date
            self._db.update_ml_eid(device_id_hex, ml_eid_str)
        except Exception as e:  # pragma: no cover
            log.warning("[gw=%s] update_ml_eid did=%s: %s",
                        gateway_id, device_id_hex, e)
            return
        log.info("[gw=%s] ml_eid refresh did=%s → %s",
                 gateway_id, device_id_hex, ml_eid_str)

    # ── pending (unknown) devices ───────────────────────────────────────

    PENDING_MAX = 64
    PENDING_TTL_SEC = 24 * 3600

    def _record_pending_device(self, device_id_hex: str,
                               gateway_id: str) -> None:
        now = int(time.time())
        entry = self._pending_devices.get(device_id_hex)
        if entry is None:
            self._pending_devices[device_id_hex] = {
                "first_seen":  now,
                "last_seen":   now,
                "via_gateway": gateway_id,
                "frame_count": 1,
            }
            log.info("pending device sighted: id=%s via gw=%s",
                     device_id_hex, gateway_id)
        else:
            entry["last_seen"]   = now
            entry["via_gateway"] = gateway_id
            entry["frame_count"] += 1

        # Garbage-collect: drop entries older than TTL, then if still
        # over MAX, evict the oldest by last_seen.
        cutoff = now - self.PENDING_TTL_SEC
        stale = [k for k, v in self._pending_devices.items()
                 if v["last_seen"] < cutoff]
        for k in stale:
            self._pending_devices.pop(k, None)
        if len(self._pending_devices) > self.PENDING_MAX:
            ordered = sorted(self._pending_devices.items(),
                             key=lambda kv: kv[1]["last_seen"])
            for k, _ in ordered[: len(self._pending_devices) - self.PENDING_MAX]:
                self._pending_devices.pop(k, None)

    def get_pending_devices(self) -> list[dict]:
        """Snapshot of unknown-device sightings, newest first."""
        out = []
        for did, e in self._pending_devices.items():
            out.append({
                "device_id":   did,
                "first_seen":  e["first_seen"],
                "last_seen":   e["last_seen"],
                "via_gateway": e["via_gateway"],
                "frame_count": e["frame_count"],
            })
        out.sort(key=lambda d: d["last_seen"], reverse=True)
        return out

    def is_pending_device(self, device_id_hex: str) -> bool:
        return device_id_hex in self._pending_devices

    # ── per-device OTA progress ─────────────────────────────────────

    # If no new BLOCK_REQ arrives within this many seconds AND the
    # download hasn't completed, surface the run as "stalled".
    OTA_STALLED_SEC = 180

    def get_ota_status(self, device_id_hex: str) -> dict:
        """Snapshot of the most recent / in-flight OTA for a device.

        Returns a synthetic dict — derived from the cached OTA_CHECK
        + BLOCK_REQ events.  When no OTA has been tracked for this
        device yet returns an `idle` placeholder.
        """
        st = self._ota_state.get(device_id_hex)
        now = int(time.time())
        if not st:
            return {"state": "idle"}

        highest = int(st.get("highest_block", -1))
        total   = int(st.get("total_blocks", 0))
        last_at = int(st.get("last_block_at", 0))
        in_flight = bool(st.get("in_flight", False))
        running   = str(st.get("running_version", ""))
        target    = str(st.get("target_version", ""))
        if not target:
            # Hub restarted mid-cycle: the in-memory tracker lost the
            # target the device armed for.  Backfill from the firmware
            # registry so the armed/done derivation still works.
            _, fwt = self._fw_target_for_device(device_id_hex)
            if fwt is not None:
                target = fwt.target_version
                st["target_version"] = target

        # Derive a coarse state name.  Compare base versions throughout
        # ("0.0.4+0" reported by the device matches target "0.0.4" —
        # Zephyr appends the tweak suffix).
        run_base = running.split("+", 1)[0]
        tgt_base = target.split("+", 1)[0]
        armed = bool(st.get("armed", False))
        armed_base = str(st.get("armed_version", "")).split("+", 1)[0]

        if armed and armed_base and armed_base == tgt_base \
                and run_base != tgt_base:
            # Downloaded + verified + waiting for operator apply.
            state = "armed"
        elif not in_flight:
            state = "done" if run_base and run_base == tgt_base else "idle"
        elif total > 0 and highest >= total - 1:
            state = "applying"
        elif last_at and (now - last_at) > self.OTA_STALLED_SEC:
            state = "stalled"
        elif highest >= 0:
            state = "downloading"
        else:
            state = "hint_sent"

        progress_percent = (
            round(100.0 * (highest + 1) / total, 1)
            if total > 0 and highest >= 0 else 0.0)

        out = {
            "state":            state,
            "running_version":  running,
            "target_version":   target,
            "target_size":      int(st.get("target_size", 0)),
            "total_blocks":     total,
            "highest_block":    highest if highest >= 0 else None,
            "progress_percent": progress_percent,
            "last_block_at":    last_at or None,
            "started_at":       int(st.get("started_at", 0)) or None,
            "elapsed_sec":      (now - int(st["started_at"]))
                                if st.get("started_at") else None,
            "armed":            armed,
            "armed_version":    st.get("armed_version") or None,
            "armed_at":         st.get("armed_at") or None,
        }
        return out

    # Sensors fire each reliable message as a burst of copies ~30 ms
    # apart (nn_proto_client FIRE_BURST, from before the radio driver
    # retried lost frames).  A copy inside this window is the SAME send,
    # not a retry: handle and ack the first only.  Must stay below the
    # sensor's shortest retry timeout (150 ms) so a real retry, which
    # means our ack was lost, is still answered.
    BURST_COPY_WINDOW_S = 0.12

    def _is_burst_copy(self, did: str, payload: bytes, t_rx: float) -> bool:
        if len(payload) < 6:
            return False
        cmd, tid = struct.unpack_from("<HI", payload, 0)
        if not tid:
            return False
        last = self._burst_first.get(did)
        if last and last[0] == cmd and last[1] == tid and t_rx - last[2] < self.BURST_COPY_WINDOW_S:
            c = self._rx_counts.setdefault(did, {"hub_rx": 0, "hub_repeats": 0})
            c["hub_burst_copies"] = c.get("hub_burst_copies", 0) + 1
            log.debug("[%s] RX burst copy cmd=0x%04x tid=0x%08x +%.0f ms (dropped, first one was acked)",
                      did, cmd, tid, (t_rx - last[2]) * 1000)
            return True
        self._burst_first[did] = (cmd, tid, t_rx)
        return False

    def _log_rx(self, gateway_id: str, did: str, payload: bytes,
                t_rx: float, took: float, note: str) -> None:
        cmd = tid = None
        if len(payload) >= 6:
            cmd, tid = struct.unpack_from("<HI", payload, 0)
        try:
            cname = proto.Cmd(cmd).name if cmd is not None else "?"
        except ValueError:
            cname = f"0x{cmd:04x}"
        d = self._db.get_device(did)
        who = d.name if d else did
        # The same (device, tid) again = the device retransmitted: our
        # ack/reply did not reach it.  Count it in the line.
        rep = ""
        if tid:
            seen = self._rx_seen.get(did)
            if seen and seen[0] == tid and t_rx - seen[2] < 120:
                seen[1] += 1
                seen[2] = t_rx
                rep = f" REPEAT#{seen[1]} (device did not get our ack)"
            else:
                self._rx_seen[did] = [tid, 0, t_rx]
        c = self._rx_counts.setdefault(did, {"hub_rx": 0, "hub_repeats": 0})
        c["hub_rx"] += 1
        if rep:
            c["hub_repeats"] += 1
        rx = time.strftime("%H:%M:%S", time.localtime(t_rx)) + ".%03d" % (int(t_rx * 1000) % 1000)
        log.info("[%s] RX %s tid=%s %dB via gw=%s rx=%s handled=%.1fms%s%s",
                 who, cname, f"0x{tid:08x}" if tid is not None else "-",
                 len(payload), gateway_id[:8], rx, took * 1000,
                 f" -> {note}" if note else "", rep)

    async def _on_d2h(self, gateway_id: str, device_id_hex: str, payload: bytes):
        """Handle a D2H telemetry frame.

        Phase 6: if the inner cmd is FIELD_REPLY, resolve the matching
        request future.  Otherwise treat as plain telemetry.

        Phase 7 (post-CoAP migration): if the inner cmd is one of the
        0x0020..0x002F sensor↔hub control-plane cmds, dispatch to the
        registered handler.  Sensor-originated requests reply via H2D
        with the same tid; fire-and-forget telemetry (LOG_LINE,
        AUTO_EVENT) just runs the handler and returns.
        """
        if self._db.get_device(device_id_hex) is None:
            # Sensor sending traffic that the hub can't tie to a row.
            # Record the sighting so the API can surface a recovery hint
            # to the operator instead of silently dropping the frame.
            self._record_pending_device(device_id_hex, gateway_id)
        else:
            self._db.touch_device(device_id_hex)

        # All [cmd:2 LE | tid:4 LE | body...] inner shape needs ≥6 B.
        if len(payload) < 6:
            return
        cmd, tid = struct.unpack_from("<HI", payload, 0)
        body = payload[6:]

        # FIELD_REPLY: the sensor's reliable-send layer expects a
        # FIELD_REPLY_ACK back so it can retire its retry slot.  Send
        # the ACK for BOTH first-arrival and duplicate FIELD_REPLY
        # frames — the duplicates only exist because an earlier ACK
        # was lost, so re-acking short-circuits the next sensor
        # round.  ACK before resolving the future so the sensor can
        # stop retrying even if the API handler downstream is slow.
        if cmd in (proto.Cmd.FIELD_REPLY, proto.Cmd.FIELD_REPLY_S):
            asyncio.create_task(
                self._send_field_reply_ack(device_id_hex, tid))

        # Heartbeat salt piggyback: a device whose session predates a hub
        # restart still heartbeats its per-boot salt — an unknown salt
        # here means WE lost the session (restart), so re-run the
        # handshake without waiting for the device to reboot.
        if cmd == proto.Cmd.DEVICE_HEARTBEAT and len(body) >= 4:
            # First 4 bytes are the device's uptime_ms (LE) — sent every
            # ~30 s whenever the sensor is otherwise idle.  Kept in RAM:
            # it is telemetry about NOW, and a hub restart just means one
            # heartbeat period of "unknown".
            import time as _t
            self.hb_uptime[device_id_hex] = {
                "uptime_ms": int.from_bytes(bytes(body[0:4]), "little"),
                "at": _t.time(),
            }
            _note("heartbeat, device up %ds" % (self.hb_uptime[device_id_hex]["uptime_ms"] // 1000))
        if cmd == proto.Cmd.DEVICE_HEARTBEAT and len(body) >= 12:
            hb_salt = bytes(body[4:12])
            st = self._sessions.get(device_id_hex)
            if st is None or st.get("dev_salt") != hb_salt:
                self._sessions[device_id_hex] = {
                    "sess": None, "verified": False, "dev_salt": hb_salt}
                asyncio.create_task(
                    self._establish_session(device_id_hex, hb_salt))
            elif (not st.get("verified") and st.get("sess") is not None
                  and st.get("hub_salt")):
                # Same salt, session derived, but the device never proved
                # the mirror: our SESS_INIT was lost.  Every heartbeat
                # (~30 s) is another chance until it lands.
                log.info("[%s] heartbeat while session unverified — "
                         "re-sending SESS_INIT", device_id_hex)
                asyncio.create_task(
                    self._send_sess_init(device_id_hex, st["hub_salt"]))

        # Reliable AUTO_EVENT: the device retransmits (5 rounds, backoff)
        # until this ack arrives — that retry loop is what makes cache
        # freshness survive weak radios.  Ack duplicates too; the cache
        # overwrite below is idempotent.
        if cmd == proto.Cmd.AUTO_EVENT and tid != 0:
            inner = (int(proto.Cmd.AUTO_EVENT_ACK).to_bytes(2, "little")
                     + int(tid).to_bytes(4, "little"))
            asyncio.create_task(self.send_h2d(device_id_hex, inner))

        # INFO_REPLY can arrive paired (response to INFO_QUERY, tid
        # matches an outstanding request) OR unsolicited (device's
        # own boot-time / rule-change config sync, tid=0).  Either way
        # the body is the device's authoritative view of its config —
        # parse `firmware` to keep the hub's `running_version` fresh
        # so the OTA state-machine knows when an in-flight apply is
        # actually complete.  See feedback_hub_ota_state_running_version_stale.
        if cmd == proto.Cmd.INFO_REPLY:
            self._consume_info_reply(device_id_hex, body)
            _note("config sync stored")

        # A D2H reply that matches one of the outstanding hub-initiated
        # requests (either FIELD_OP via request_field, or generic
        # request_h2d) — resolve its future.  We accept any reply cmd
        # paired with the tid, but if the caller of request_h2d
        # specified an expected_reply_cmd we gate on that.
        if tid in self._pending_field:
            expect = self._pending_expect_cmd.get(tid)
            if expect is None or expect == cmd:
                fut = self._pending_field.pop(tid, None)
                self._pending_expect_cmd.pop(tid, None)
                if fut is not None and not fut.done():
                    fut.set_result(body)
                    _note("reply to a waiting hub request")
                else:
                    _note("reply, requester already gave up")
                return
            # Wrong cmd for this tid — duplicates from a different
            # request reuse?  Just drop.
            log.debug("D2H tid=0x%08x cmd=0x%04x but expected=0x%04x — drop",
                      tid, cmd, expect)
            _note("dropped: unexpected cmd for this tid")
            return

        # Control-plane cmds — dispatcher table keyed on cmd code.
        handler = self._d2h_dispatch.get(cmd)
        if handler is not None:
            try:
                await handler(device_id_hex, tid, body)
            except Exception as e:
                log.warning("[%s] D2H cmd 0x%04x handler error: %s",
                            device_id_hex, cmd, e)
                _note(f"handler error: {e}")
            return
        if cmd in (proto.Cmd.FIELD_REPLY, proto.Cmd.FIELD_REPLY_S):
            _note("reply for no pending request (duplicate/late), acked")
            return
        log.debug("[%s] unhandled D2H cmd 0x%04x (tid=0x%08x, %d B)",
                  device_id_hex, cmd, tid, len(body))
        if not _rx_note.get():
            _note("no handler")

    # ── H2D send ────────────────────────────────────────────────────────

    def _resolve_gateway_for_device(self, device_id: str) -> Optional[str]:
        """Find the gateway-of-record for *device_id*.  Prefers the live
        D2H-derived route cache; falls back to ``provision_info.gateway_id``
        so freshly-provisioned devices can be reached before they've
        emitted any telemetry."""
        gw_id = self._device_route.get(device_id)
        if gw_id:
            return gw_id
        try:
            pi = self._db.get_provision_info(device_id)
        except Exception:
            pi = None
        if pi and pi.gateway_id:
            self._device_route[device_id] = pi.gateway_id
            return pi.gateway_id
        return None

    async def gateway_request(self, gateway_id: str, cmd: int, body: bytes = b"",
                              timeout: float = 10.0, attempts: int = 2) -> tuple[int, bytes]:
        """Hub -> gateway command (H2G), answered by a D2G reply with the same
        tid.  Returns (status, body).  The frame is signed and carries the
        send time; the gateway rejects stale or already-handled tids, so a
        re-send after a timeout reuses the tid only for idempotent reads.
        Raises TimeoutError / RuntimeError."""
        if not self._server.is_gateway_online(gateway_id):
            raise RuntimeError(f"gateway {gateway_id[:8]} offline")
        loop = asyncio.get_event_loop()
        while True:
            tid = secrets.randbits(32) or 1
            if tid not in self._pending_gw:
                break
        fut = loop.create_future()
        self._pending_gw[tid] = fut
        # a set-channel must never run twice: one attempt only
        if cmd == proto.Cmd.GW_CHANNEL_SET:
            attempts = 1
        try:
            for attempt in range(attempts):
                inner = (struct.pack("<HIQ", int(cmd), tid, int(time.time() * 1000))
                         + (body or b""))
                frame = proto.encode(proto.FrameType.H2G, bytes.fromhex(gateway_id),
                                     inner, self._hub_priv)
                ok = await self._server.send_to_gateway(gateway_id, frame)
                log.info("[gw=%s] TX %s tid=0x%08x %s", gateway_id[:8],
                         proto.Cmd(cmd).name, tid,
                         "handed to gateway" if ok else "NOT SENT")
                if not ok:
                    raise RuntimeError(f"gateway {gateway_id[:8]} socket write failed")
                try:
                    rcmd, rbody = await asyncio.wait_for(asyncio.shield(fut), timeout)
                    break
                except asyncio.TimeoutError:
                    if attempt + 1 >= attempts:
                        raise
                    # the gateway drops a repeated tid: a retry needs a new one
                    self._pending_gw.pop(tid, None)
                    tid = secrets.randbits(32) or 1
                    self._pending_gw[tid] = fut
            if rcmd != int(cmd) + 1 or not rbody:
                raise RuntimeError(f"unexpected reply 0x{rcmd:04x}")
            return struct.unpack_from("<b", rbody, 0)[0], rbody[1:]
        finally:
            self._pending_gw.pop(tid, None)

    def _fallback_gateway(self, exclude=()) -> Optional[str]:
        """Another online gateway: every gateway reaches every device over
        the mesh (the device's ML-EID is routable from any border router)."""
        cands = [g for g in self._db.list_gateways()
                 if g.id not in exclude and self._server.is_gateway_online(g.id)]
        cands.sort(key=lambda g: {3: 0, 2: 1}.get(g.role, 2))    # leader, router
        return cands[0].id if cands else None

    async def _on_h2d_undeliverable(self, gateway_id: str, args: bytes) -> None:
        """The gateway could not forward an H2D frame.  Runs from the
        gateway's reader loop: the recovery (which waits for that gateway's
        reply) goes to its own task."""
        if len(args) < 3:
            return
        reason, dsz = struct.unpack_from("<bH", args, 0)
        if len(args) < 3 + dsz + 6:
            return
        did = args[3:3 + dsz].hex()
        cmd, tid = struct.unpack_from("<HI", args, 3 + dsz)
        rec = self._recent_h2d.get((did, cmd, tid))
        if rec is None or rec[2] or time.time() - rec[1] > 30:
            log.debug("[%s] gw=%s undeliverable cmd=0x%04x tid=0x%08x (reason %d), "
                      "already handled", did, gateway_id[:8], cmd, tid, reason)
            return
        self._recent_h2d[(did, cmd, tid)] = (rec[0], rec[1], True)
        asyncio.ensure_future(self._redeliver(gateway_id, did, rec[0], reason))

    async def _redeliver(self, gateway_id: str, did: str, payload: bytes, reason: int) -> None:
        """Gateways learn a device's address only from frames the device sent
        through them.  -EHOSTUNREACH: teach this gateway the device's mesh
        address (from the gateways' DEVICE_THREAD_STATE reports) and re-send
        through it; otherwise re-send through another online gateway."""
        d = self._db.get_device(did)
        who = d.name if d else did
        target, how = None, ""
        pi = None
        try:
            pi = self._db.get_provision_info(did)
        except Exception:
            pass
        if reason == -113 and pi and pi.ml_eid:
            try:
                import ipaddress
                addr = ipaddress.IPv6Address(pi.ml_eid).packed
                st, _ = await self.gateway_request(
                    gateway_id, proto.Cmd.GW_ROUTE_SET,
                    struct.pack("<H", len(bytes.fromhex(did))) + bytes.fromhex(did) + addr,
                    timeout=5, attempts=1)
                if st == 0:
                    target, how = gateway_id, f"route to {pi.ml_eid} pushed to gw={gateway_id[:8]}"
            except Exception as e:                                # noqa: BLE001
                log.info("[%s] route push to gw=%s failed: %s", who, gateway_id[:8], e)
        if target is None:
            target = self._fallback_gateway(exclude=(gateway_id,))
            how = f"gw={gateway_id[:8]} could not deliver (reason {reason})"
        if target is None:
            log.warning("[%s] gw=%s could not deliver (reason %d); no other way to reach it",
                        who, gateway_id[:8], reason)
            return
        self._device_route[did] = target
        frame = proto.encode(proto.FrameType.H2D, bytes.fromhex(did), payload, self._hub_priv)
        ok = await self._server.send_to_gateway(target, frame)
        self._log_tx(did, payload, target, f"re-sent: {how}" + ("" if ok else " -- NOT SENT"))

    async def send_h2d(self, device_id: str, payload: bytes) -> bool:
        """Send an H2D frame to the named device.  Returns True if the
        frame was enqueued on a gateway TCP socket.  When the device's
        usual gateway is offline, another online gateway carries it."""
        gw_id = self._resolve_gateway_for_device(device_id)
        via = ""
        if not gw_id or not self._server.is_gateway_online(gw_id):
            alt = self._fallback_gateway(exclude=(gw_id,) if gw_id else ())
            if alt is None:
                why = "no route" if not gw_id else "gateway offline, no other online"
                log.warning("send_h2d(%s): %s", device_id, why)
                self._log_tx(device_id, payload, gw_id, f"NOT SENT: {why}")
                return False
            via = (f" (fallback: usual gw={gw_id[:8]} offline)" if gw_id
                   else " (fallback: no route yet)")
            gw_id = alt
            self._device_route[device_id] = alt
        try:
            did_bytes = bytes.fromhex(device_id)
        except ValueError:
            log.warning("send_h2d(%s): bad hex device_id", device_id)
            return False
        frame = proto.encode(proto.FrameType.H2D, did_bytes,
                             payload, self._hub_priv)
        ok = await self._server.send_to_gateway(gw_id, frame)
        if ok and len(payload) >= 6:
            now = time.time()
            if len(self._recent_h2d) > 256:
                self._recent_h2d = {k: v for k, v in self._recent_h2d.items()
                                    if now - v[1] < 30}
            cmd, tid = struct.unpack_from("<HI", payload, 0)
            self._recent_h2d[(device_id, cmd, tid)] = (payload, now, False)
        self._log_tx(device_id, payload, gw_id,
                     ("handed to gateway" if ok else "NOT SENT: gateway socket write failed") + via)
        return ok

    def _log_tx(self, did: str, payload: bytes, gw_id, outcome: str) -> None:
        """Counterpart of the RX line: every hub -> device message, with
        the gateway it was handed to.  With both, a device retransmitting
        (REPEAT) can be told apart: no TX at all = the hub never answered;
        TX 'handed to gateway' = lost between the gateway and the device."""
        cmd = tid = None
        if len(payload) >= 6:
            cmd, tid = struct.unpack_from("<HI", payload, 0)
        try:
            cname = proto.Cmd(cmd).name if cmd is not None else "?"
        except ValueError:
            cname = f"0x{cmd:04x}"
        d = self._db.get_device(did)
        log.info("[%s] TX %s tid=%s %dB via gw=%s -> %s",
                 d.name if d else did, cname,
                 f"0x{tid:08x}" if tid is not None else "-", len(payload),
                 (gw_id or "-")[:8], outcome)

    async def _send_field_reply_ack(self, device_id: str, tid: int) -> None:
        """Send H2D FIELD_REPLY_ACK with the given tid + empty body so
        the sensor's reliable-send slot for the matching FIELD_REPLY
        retires.  Failures are logged but never raised — the sensor
        will time out and give up after its own retry budget, which
        is the right outcome if the gateway/route is wedged."""
        try:
            inner = struct.pack("<HI", proto.Cmd.FIELD_REPLY_ACK, tid)
            ok = await self.send_h2d(device_id, inner)
            if not ok:
                log.debug("FIELD_REPLY_ACK to %s tid=0x%08x: no route",
                          device_id, tid)
        except Exception as e:
            log.warning("FIELD_REPLY_ACK to %s tid=0x%08x: %s",
                        device_id, tid, e)

    # ── Phase 6: FIELD_OP request/reply via H2D/D2H ─────────────────────

    async def request_field(self, device_id: str, envelope: bytes,
                            timeout: float = 10.0) -> bytes:
        """Send an H2D FIELD_OP carrying *envelope* (already-encrypted
        ECIES JSON bytes) and await the matching D2H FIELD_REPLY.

        Now backed by the unified `request_h2d` helper — inherits the
        same inner-burst + exponential-backoff retry policy used by
        AUTO_PUSH, INFO_QUERY, etc.  No more per-cmd retry constants;
        everything goes through one X2Y transport layer.

        Returns the device's response envelope (still ECIES — caller
        decrypts).  Raises asyncio.TimeoutError if the full retry
        budget elapses without a FIELD_REPLY.
        """
        return await self.request_h2d(
            device_id,
            req_cmd=proto.Cmd.FIELD_OP,
            body=envelope,
            expected_reply_cmd=proto.Cmd.FIELD_REPLY,
            timeout=timeout,
        )

    async def request_field_sealed(self, device_id: str, plain: bytes,
                                   timeout: float = 10.0) -> bytes:
        """Phase 3 hot path: send the field-op JSON under the symmetric
        session (FIELD_OP_S) and open the sealed FIELD_REPLY_S.  ZERO
        asymmetric crypto on the device.  AAD binds cmd+tid per
        direction.  Raises KeyError if no verified session (caller
        falls back to the ECIES path)."""
        st = self._sessions.get(device_id)
        if not st or not st.get("verified") or st.get("sess") is None:
            raise KeyError("no verified session")
        sess = st["sess"]

        def _seal(tid: int) -> bytes:
            aad = struct.pack("<HI", proto.Cmd.FIELD_OP_S, tid)
            return sess.seal(aad, plain)

        # capture tid via closure for the reply AAD
        tid_box: list = []
        def _seal_and_note(tid: int) -> bytes:
            tid_box.append(tid)
            return _seal(tid)

        sealed_reply = await self.request_h2d(
            device_id,
            req_cmd=proto.Cmd.FIELD_OP_S,
            body=b"",
            expected_reply_cmd=proto.Cmd.FIELD_REPLY_S,
            timeout=timeout,
            body_fn=_seal_and_note,
        )
        aad = struct.pack("<HI", proto.Cmd.FIELD_REPLY_S, tid_box[0])
        return sess.open(aad, sealed_reply)

    async def request_clear_user_data(self, device_id: str,
                                      timeout: float = 12.0) -> int:
        """Unregister step 1: sealed CLEAR_USER_DATA → sealed ack status.

        Idempotent BY DESIGN (the device only persists a 1-byte flag), so
        unlike REBOOT this uses the full retry budget.  The sealed body is
        built once per tid via body_fn, so every resend is byte-identical
        and the device reliable layer dedups it.  Raises KeyError when no
        verified session exists (caller surfaces 503 — the device
        re-HELLOs within seconds of being up)."""
        st = self._sessions.get(device_id)
        if not st or not st.get("verified") or st.get("sess") is None:
            raise KeyError("no verified session")
        sess = st["sess"]

        tid_box: list = []
        def _seal(tid: int) -> bytes:
            tid_box.append(tid)
            aad = struct.pack("<HI", proto.Cmd.CLEAR_USER_DATA, tid)
            return sess.seal(aad, b"\x01\x01")

        sealed = await self.request_h2d(
            device_id,
            req_cmd=proto.Cmd.CLEAR_USER_DATA,
            body=b"",
            expected_reply_cmd=proto.Cmd.CLEAR_USER_DATA_ACK,
            timeout=timeout,
            body_fn=_seal,
        )
        aad = struct.pack("<HI", proto.Cmd.CLEAR_USER_DATA_ACK, tid_box[0])
        plain = sess.open(aad, sealed)
        return plain[0] if plain else 255

    # ── Phase 7: AUTO_PUSH (H2D) → AUTO_ACK (D2H) ──────────────────────
    #
    # Replaces the legacy WebSocket ConfigResponseMsg push path.  Hub
    # ECIES-encrypts the raw auto_bin blob for the target device, sends
    # it as H2D AUTO_PUSH, and awaits the D2H AUTO_ACK whose body is a
    # 1-byte status (0 = applied, non-zero = sensor-side errno).

    async def push_auto_config(self, device_id: str,
                               raw_blob: bytes,
                               device_x25519_pub: bytes,
                               hub_enc_priv,
                               timeout: float = 10.0) -> int:
        """Push *raw_blob* (compiled auto_bin) to *device_id* via H2D
        AUTO_PUSH.  Returns the 1-byte status reported by the sensor
        (0 = applied, negative-like values are sensor errno bytes).

        The exponential-backoff retry policy now lives in the unified
        `request_h2d` transport layer — see its docstring for the
        attempt × timeout × backoff budget shared by all H2D-request /
        D2H-reply patterns (FIELD_OP, AUTO_PUSH, INFO_QUERY, OTA_*).

        Raises asyncio.TimeoutError or RuntimeError if the full retry
        budget elapses without an AUTO_ACK.
        """
        from .crypto import encrypt
        import json as _json
        envelope = encrypt(raw_blob, device_x25519_pub, hub_enc_priv,
                           direction="h2d")
        env_bytes = _json.dumps(envelope).encode()

        reply = await self.request_h2d(
            device_id,
            req_cmd=proto.Cmd.AUTO_PUSH,
            body=env_bytes,
            expected_reply_cmd=proto.Cmd.AUTO_ACK,
            timeout=timeout,
        )
        if not reply:
            return -1   # malformed (no body)
        return reply[0]

    # ── OTA_HINT (H2D) → OTA_HINT_ACK (D2H) ────────────────────────────
    #
    # Hub-initiated OTA trigger.  Body is empty — the sensor runs its own
    # check → download → apply pipeline as if the user typed
    # `mesh ota check ; mesh ota download ; mesh ota apply` at the shell.
    # ACK is a 1-byte status (0 = accepted, non-zero = sensor errno).

    async def trigger_ota(self, device_id: str,
                          timeout: float = 10.0) -> int:
        """Send an H2D OTA_HINT to *device_id* and await OTA_HINT_ACK.

        Returns the 1-byte status reported by the sensor (0 = accepted,
        non-zero = sensor-side errno).  The actual OTA download happens
        asynchronously on the sensor after the ACK.

        Raises asyncio.TimeoutError if the full retry budget elapses
        without an OTA_HINT_ACK.
        """
        reply = await self.request_h2d(
            device_id,
            req_cmd=proto.Cmd.OTA_HINT,
            body=b"",
            expected_reply_cmd=proto.Cmd.OTA_HINT_ACK,
            timeout=timeout,
        )
        if not reply:
            return -1
        return reply[0]
