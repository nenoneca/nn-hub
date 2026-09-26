"""Hub-side failsafe for device-to-device automation cascades.

The sensors run the automations themselves: a trigger edge on one device is
sent as a sealed notify to every action device (burst of 3 frames, 4 retry
rounds, a few seconds in all).  When all of that is lost on the mesh the
target simply never learns about the edge, and nothing in the fleet ever
corrects it — the LED stays wrong until the next edge in the same direction.
Seen on the bench 2026-09-18: c6-s1's button moved c6-s3 but c6-s2 kept its
old state.

This module makes the hub the last line: it already receives every actuator
change the sensors push (`{"t":"fld","n":<field>,"v":<value>}` AUTO_EVENTs,
reliable, acked), and it can write any actuator over a sealed field op.

    event from device D: field F = V
      ├─ resolves any queued watch for (D, F) whose desired value is V
      └─ if some automation has an ACTION (D, F, V): that automation fired
         (the trigger field itself is a sensor field and is not pushed, but
         every rule in this fleet also acts on the trigger device's own LED,
         which is pushed) → queue a watch for each OTHER action of the rule:
         (target device, field, desired value, due = now + delay)

    watch loop, once a second:
      due item whose target has not reported the desired value
        → sealed set_field(target, field, desired)
           success → done; failure → retry with backoff 3,6,12,…60 s
      an item is dropped when the target reports the desired value, or is
      replaced when a newer edge wants a different value for the same
      (target, field).  Retries never give up on their own — the desired
      state changing is what ends them, as the operator asked.

Writes the hub makes itself are remembered for a few seconds so the event
they cause is not mistaken for a fresh trigger (an operator writing s1.led
by hand is likewise not propagated).

Settings (hub `settings` table, changeable live through the API):
    auto_failsafe_enabled   "1" / "0"      default 1
    auto_failsafe_delay_s   seconds        default 3
"""
from __future__ import annotations

import asyncio
import base64
import logging
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

import yaml

log = logging.getLogger("hub.auto_failsafe")

BACKOFF_S = (3.0, 6.0, 12.0, 24.0, 48.0, 60.0)
# Our own writes are not fresh triggers.  The echo (the target's own event
# for the value we wrote) arrived 10 s after the write on the bench mesh
# (2026-09-18 14:41), so this must comfortably exceed a bad round trip.
HUB_WRITE_MEMORY_S = 45.0


@dataclass
class Action:
    device: str            # device id (hex)
    device_name: str
    field: str
    value: float


@dataclass
class Rule:
    auto_id: str
    trigger_devices: set
    actions: list          # list[Action]


@dataclass
class Watch:
    auto_id: str
    device: str
    device_name: str
    field: str
    desired: float
    queued_at: float
    due_at: float
    attempts: int = 0
    last_error: str = ""
    resolved_by: str = ""  # set when done: "event" | "write"


class AutoFailsafe:
    def __init__(self, db, router, enc_priv,
                 set_field: Callable[..., Awaitable[dict]],
                 clock: Callable[[], float] = time.time):
        self._db = db
        self._router = router
        self._enc_priv = enc_priv
        self._set_field = set_field
        self._clock = clock
        self._rules: list[Rule] = []
        self._rules_src = None
        self._queue: dict[tuple[str, str], Watch] = {}
        self._hub_writes: dict[tuple[str, str], tuple[float, float]] = {}  # (dev,field) -> (value, ts)
        self.stats = {"events": 0, "queued": 0, "resolved_by_event": 0,
                      "resolved_by_write": 0, "write_failures": 0,
                      "superseded": 0}
        self._history: list[dict] = []
        if router is not None and hasattr(router, "add_field_event_listener"):
            router.add_field_event_listener(self.on_field_event)

    # ── settings ─────────────────────────────────────────────────────────
    @property
    def enabled(self) -> bool:
        return str(self._db.get_setting("auto_failsafe_enabled", "1") or "1") not in ("0", "false", "")

    @property
    def delay_s(self) -> float:
        try:
            return max(0.5, float(self._db.get_setting("auto_failsafe_delay_s", "3") or 3))
        except (TypeError, ValueError):
            return 3.0

    # ── rule table (from the operator's automations.yaml) ────────────────
    def reload_rules(self) -> None:
        src = self._db.get_setting("automations_yaml") or ""
        if src == self._rules_src:
            return
        self._rules_src = src
        rules: list[Rule] = []
        try:
            raw = yaml.safe_load(src) or {}
        except Exception as e:                                   # noqa: BLE001
            log.warning("automations_yaml does not parse: %s", e)
            self._rules = []
            return
        for auto in raw.get("automations", []) or []:
            try:
                acts = []
                for a in auto.get("action", []) or []:
                    d = self._device_by_name(a["device"])
                    if d is None:
                        log.warning("[%s] action device %r unknown — skipped",
                                    auto.get("id"), a["device"])
                        continue
                    acts.append(Action(d.id, d.name, str(a["field"]), float(a["value"])))
                trig = set()
                for t in auto.get("trigger", []) or []:
                    d = self._device_by_name(t["device"])
                    if d is not None:
                        trig.add(d.id)
                if acts:
                    rules.append(Rule(str(auto["id"]), trig, acts))
            except Exception as e:                               # noqa: BLE001
                log.warning("automation %r skipped: %s", auto.get("id"), e)
        self._rules = rules
        log.info("failsafe rule table: %d automations, %d actions",
                 len(rules), sum(len(r.actions) for r in rules))

    def _device_by_name(self, name: str):
        d = self._db.get_device_by_name(name)
        if d is None:
            d = self._db.get_device(name)
        return d

    # ── inbound: a device reported an actuator change ────────────────────
    def on_field_event(self, device_id: str, fld: str, value: float) -> None:
        if not self.enabled:
            return
        self.stats["events"] += 1
        now = self._clock()
        key = (device_id, fld)

        # 1. does this satisfy a watch?
        w = self._queue.get(key)
        if w is not None and _same(w.desired, value):
            w.resolved_by = "event"
            self._done(w, now)
            self.stats["resolved_by_event"] += 1

        # 2. our own write echoing back is not a trigger
        hw = self._hub_writes.get(key)
        if hw and _same(hw[0], value) and now - hw[1] < HUB_WRITE_MEMORY_S:
            return

        # 3. an automation fired: the TRIGGER device's own action carries
        #    this (device, field, value).  Only the trigger device is
        #    believed — its action runs locally the instant the edge
        #    happens, so its event is the freshest word on what the
        #    operator did.  A non-trigger action device's event can be a
        #    late arrival from the PREVIOUS edge (seen 14:41:06: s3's "on"
        #    landed after the operator had already pressed "off" and
        #    briefly queued everyone back to "on").  Rules whose actions
        #    do not touch the trigger device fall back to any action
        #    device, since nothing better is pushed to the hub.
        self.reload_rules()
        for r in self._rules:
            hit = [a for a in r.actions
                   if a.device == device_id and a.field == fld and _same(a.value, value)]
            if not hit:
                continue
            trigger_acts = any(a.device in r.trigger_devices for a in r.actions)
            if trigger_acts and device_id not in r.trigger_devices:
                continue
            for a in r.actions:
                if a.device == device_id and a.field == fld:
                    continue                    # the one we just saw
                self._enqueue(r.auto_id, a, now)

    def _enqueue(self, auto_id: str, a: Action, now: float) -> None:
        key = (a.device, a.field)
        cur = self._queue.get(key)
        if cur is not None:
            if _same(cur.desired, a.value):
                return                          # already watching for this value
            cur.resolved_by = "superseded"
            self._done(cur, now)
            self.stats["superseded"] += 1
        # already there?  (the target's event may have arrived first)
        cache = {}
        if self._router is not None and hasattr(self._router, "get_field_cache"):
            cache = self._router.get_field_cache(a.device) or {}
        c = cache.get(a.field)
        if c and _same(c.get("v"), a.value) and c.get("ts", 0) >= now - self.delay_s:
            return
        self._queue[key] = Watch(auto_id, a.device, a.device_name, a.field,
                                 a.value, now, now + self.delay_s)
        # the automation's intent is the target's desired state: the cards
        # show "turning on…" for s2/s3 while the cascade is in flight
        if self._router is not None and hasattr(self._router, "set_desired"):
            self._router.set_desired(a.device, a.field, a.value, auto_id)
        self.stats["queued"] += 1
        log.info("[failsafe] %s: watching %s.%s -> %s (due in %.1fs)",
                 auto_id, a.device_name, a.field, a.value, self.delay_s)

    def _done(self, w: Watch, now: float) -> None:
        self._queue.pop((w.device, w.field), None)
        self._history.append({
            "auto_id": w.auto_id, "device": w.device_name, "field": w.field,
            "desired": w.desired, "queued_at": int(w.queued_at),
            "resolved_at": int(now), "attempts": w.attempts,
            "resolved_by": w.resolved_by, "last_error": w.last_error,
        })
        del self._history[:-50]

    # ── the loop ─────────────────────────────────────────────────────────
    async def tick(self) -> None:
        """One pass over the queue; separated from run() for tests."""
        if not self.enabled:
            return
        now = self._clock()
        for key, w in list(self._queue.items()):
            if w.due_at > now:
                continue
            # the target may have reported the value without us seeing an
            # event (e.g. hub restart between): consult the cache first
            cache = self._router.get_field_cache(w.device) if hasattr(self._router, "get_field_cache") else {}
            c = (cache or {}).get(w.field)
            if c and _same(c.get("v"), w.desired) and c.get("ts", 0) >= w.queued_at:
                w.resolved_by = "event"
                self._done(w, now)
                self.stats["resolved_by_event"] += 1
                continue
            await self._write(w, now)

    async def _write(self, w: Watch, now: float) -> None:
        w.attempts += 1
        pi = self._db.get_provision_info(w.device)
        if not pi or not pi.enc_pubkey_b64:
            w.last_error = "no device pubkey"
            w.due_at = now + BACKOFF_S[-1]
            return
        dev_pub = base64.b64decode(pi.enc_pubkey_b64)
        log.info("[failsafe] %s: %s.%s did not reach %s after %.0fs — writing it "
                 "(attempt %d)", w.auto_id, w.device_name, w.field, w.desired,
                 now - w.queued_at, w.attempts)
        self._hub_writes[(w.device, w.field)] = (w.desired, now)
        try:
            resp = await self._set_field(self._router, w.device, w.field,
                                         w.desired, dev_pub, self._enc_priv,
                                         timeout=10.0)
        except Exception as e:                                   # noqa: BLE001
            resp = {"err": f"{type(e).__name__}: {e}"}
        if isinstance(resp, dict) and "err" not in resp:
            # the device's reply is its word on the value: cache it (this
            # also confirms the desired state the cards are showing)
            if hasattr(self._router, "remember_field") and isinstance(resp.get("value"), (int, float)):
                self._router.remember_field(w.device, w.field, float(resp["value"]))
            w.resolved_by = "write"
            w.last_error = ""
            self._done(w, self._clock())
            self.stats["resolved_by_write"] += 1
            log.info("[failsafe] %s: %s.%s = %s written OK", w.auto_id,
                     w.device_name, w.field, w.desired)
            return
        w.last_error = str((resp or {}).get("err", "no reply"))[:120]
        self.stats["write_failures"] += 1
        delay = BACKOFF_S[min(w.attempts - 1, len(BACKOFF_S) - 1)]
        w.due_at = self._clock() + delay
        log.warning("[failsafe] %s: write to %s.%s failed (%s) — retry in %.0fs",
                    w.auto_id, w.device_name, w.field, w.last_error, delay)

    async def run(self) -> None:
        log.info("automation failsafe: enabled=%s delay=%.1fs", self.enabled, self.delay_s)
        while True:
            try:
                await self.tick()
            except Exception as e:                               # noqa: BLE001
                log.warning("failsafe tick error: %s", e)
            await asyncio.sleep(1.0)

    # ── for the API ──────────────────────────────────────────────────────
    def status(self) -> dict:
        now = self._clock()
        self.reload_rules()
        return {
            "enabled": self.enabled,
            "delay_s": self.delay_s,
            "rules": [{"id": r.auto_id,
                       "actions": [f"{a.device_name}.{a.field}={a.value:g}" for a in r.actions]}
                      for r in self._rules],
            "queue": [{"auto_id": w.auto_id, "device": w.device_name, "field": w.field,
                       "desired": w.desired, "waiting_s": round(now - w.queued_at, 1),
                       "due_in_s": round(w.due_at - now, 1), "attempts": w.attempts,
                       "last_error": w.last_error}
                      for w in self._queue.values()],
            "stats": dict(self.stats),
            "recent": list(reversed(self._history[-10:])),
        }


def _same(a, b) -> bool:
    try:
        return abs(float(a) - float(b)) < 1e-6
    except (TypeError, ValueError):
        return False
