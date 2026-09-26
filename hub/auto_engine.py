"""
Automation engine — Python reference implementation of the device-side rule engine.

Used for simulation/testing. Each DeviceWorker runs in its own thread,
processing messages from a queue. The same logic will be implemented
in C on the device (auto_engine.c).

Message types on the queue:
  ("set_field", field, value)          — app sets a sensor/actuator value
  ("trigger", auto_id, tid, field, value)   — trigger notification from another device
  ("condition", auto_id, tid, field, value) — condition value push from another device
  ("get_field", field, result_event, result_dict)  — query field value (sync)
"""

from __future__ import annotations
import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

log = logging.getLogger("hub.auto_engine")


@dataclass
class PendingAction:
    """Tracks an in-flight automation waiting for conditions."""
    auto_id: str
    tid: int
    deadline: float                 # time.monotonic() deadline
    received: dict[str, float]      # device → value (conditions received so far)
    expected: list[dict]            # from config: expect_conditions list
    do: dict                        # {"field": ..., "value": ...}


class DeviceWorker:
    """Simulates one device's automation rule engine in a thread."""

    def __init__(self, name: str, config: dict,
                 send_fn: Callable[[str, dict], None]):
        """
        name:    device name (e.g. "sensor-01")
        config:  compiled per-device config payload from the compiler
        send_fn: callable(target_device_name, message_dict) for sending
                 CoAP notifications to other devices
        """
        self.name = name
        self.config = config
        self._send = send_fn
        self._q: queue.Queue = queue.Queue()
        self._fields: dict[str, float] = {}
        self._tid_counter = 0
        self._pending: list[PendingAction] = []
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._action_log: list[dict] = []  # for test verification

    @property
    def action_log(self) -> list[dict]:
        return list(self._action_log)

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name=f"dev-{self.name}")
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._q.put(None)  # wake up
        if self._thread:
            self._thread.join(timeout=2)

    def set_field(self, field: str, value: float):
        """App sets a sensor/actuator value (simulates sensor reading)."""
        self._q.put(("set_field", field, value))

    def get_field(self, field: str, timeout: float = 1.0) -> float | None:
        """Synchronously query a field value."""
        result: dict = {}
        evt = threading.Event()
        self._q.put(("get_field", field, evt, result))
        evt.wait(timeout)
        return result.get("value")

    def receive(self, msg: dict):
        """Receive a CoAP /auto message from another device."""
        t = msg.get("t")
        if t == "trg":
            self._q.put(("trigger", msg["a"], msg["tid"],
                         msg["f"], msg["v"]))
        elif t == "cond":
            self._q.put(("condition", msg["a"], msg["tid"],
                         msg["f"], msg["v"]))

    # ── Worker thread ─────────────────────────────────────────────────

    def _run(self):
        while not self._stop.is_set():
            # Check pending action timeouts
            self._check_pending_timeouts()

            try:
                item = self._q.get(timeout=0.1)
            except queue.Empty:
                continue

            if item is None:
                break

            kind = item[0]
            if kind == "set_field":
                _, field, value = item
                self._handle_set_field(field, value)
            elif kind == "trigger":
                _, auto_id, tid, field, value = item
                self._handle_trigger(auto_id, tid, field, value)
            elif kind == "condition":
                _, auto_id, tid, field, value = item
                self._handle_condition(auto_id, tid, field, value)
            elif kind == "get_field":
                _, field, evt, result = item
                result["value"] = self._fields.get(field)
                evt.set()

    def _handle_set_field(self, field: str, value: float):
        old = self._fields.get(field)
        self._fields[field] = value

        # Check triggers
        for trg in self.config.get("triggers", []):
            if trg["field"] != field:
                continue
            if not self._evaluate(value, trg["op"], trg["threshold"]):
                continue
            # Avoid re-triggering on same value (edge detection)
            if old is not None and self._evaluate(old, trg["op"],
                                                   trg["threshold"]):
                continue

            self._tid_counter = (self._tid_counter + 1) & 0xFFFF
            tid = self._tid_counter
            auto_id = trg["auto_id"]

            log.info("[%s] TRIGGER %s tid=%d %s=%s",
                     self.name, auto_id, tid, field, value)

            msg = {"t": "trg", "a": auto_id, "tid": tid,
                   "f": field, "v": value}

            notify = trg.get("notify", [])
            if trg.get("broadcast"):
                # Broadcast to all — handled by simulation harness
                self._send("__broadcast__", msg)
            else:
                for target in notify:
                    self._send(target, msg)

    def _handle_trigger(self, auto_id: str, tid: int,
                        field: str, value: float):
        # Condition role: if we have a condition rule for this auto_id,
        # push our field value to action devices
        for cond in self.config.get("conditions", []):
            if cond["auto_id"] != auto_id:
                continue
            cond_value = self._fields.get(cond["field"])
            if cond_value is None:
                cond_value = 0  # unknown → send 0
            msg = {"t": "cond", "a": auto_id, "tid": tid,
                   "f": cond["field"], "v": cond_value}
            log.info("[%s] CONDITION %s tid=%d %s=%s",
                     self.name, auto_id, tid, cond["field"], cond_value)
            for target in cond.get("notify", []):
                self._send(target, msg)

        # Action role: if we have an action rule for this auto_id,
        # start collecting conditions
        for act in self.config.get("actions", []):
            if act["auto_id"] != auto_id:
                continue
            timeout_ms = act.get("timeout_ms", 5000)
            pending = PendingAction(
                auto_id=auto_id,
                tid=tid,
                deadline=time.monotonic() + timeout_ms / 1000.0,
                received={},
                expected=act["expect_conditions"],
                do=act["do"],
            )
            if not pending.expected:
                # No conditions needed — execute immediately
                log.info("[%s] ACTION %s tid=%d (unconditional) → %s",
                         self.name, auto_id, tid, pending.do)
                self._execute_action(pending)
            else:
                self._pending.append(pending)
                log.info("[%s] ACTION %s tid=%d waiting for %d conditions",
                         self.name, auto_id, tid, len(pending.expected))

    def _handle_condition(self, auto_id: str, tid: int,
                          field: str, value: float):
        for pending in self._pending:
            if pending.auto_id != auto_id or pending.tid != tid:
                continue
            # Find which expected condition this satisfies
            for ec in pending.expected:
                if ec["field"] == field:
                    pending.received[ec["device"]] = value
                    break
            # Check if all conditions received
            if self._all_conditions_received(pending):
                self._evaluate_and_execute(pending)
                self._pending.remove(pending)
                return

    def _check_pending_timeouts(self):
        now = time.monotonic()
        expired = [p for p in self._pending if now >= p.deadline]
        for pending in expired:
            self._pending.remove(pending)
            log.info("[%s] ACTION %s tid=%d TIMEOUT — evaluating with defaults",
                     self.name, pending.auto_id, pending.tid)
            self._evaluate_and_execute(pending)

    def _all_conditions_received(self, pending: PendingAction) -> bool:
        for ec in pending.expected:
            if ec["device"] not in pending.received:
                return False
        return True

    def _evaluate_and_execute(self, pending: PendingAction):
        """Evaluate all condition thresholds and execute if all pass."""
        for ec in pending.expected:
            value = pending.received.get(ec["device"])
            if value is None:
                # Not received — use on_timeout default
                value_ok = ec.get("on_timeout", False)
            else:
                value_ok = self._evaluate(value, ec["op"], ec["threshold"])

            if not value_ok:
                log.info("[%s] ACTION %s tid=%d SKIPPED — condition %s.%s "
                         "not met (value=%s, need %s %s)",
                         self.name, pending.auto_id, pending.tid,
                         ec["device"], ec["field"],
                         value, ec["op"], ec["threshold"])
                return

        self._execute_action(pending)

    def _execute_action(self, pending: PendingAction):
        field = pending.do["field"]
        value = pending.do["value"]
        self._fields[field] = value
        log.info("[%s] ACTION %s tid=%d EXECUTED → %s=%s",
                 self.name, pending.auto_id, pending.tid, field, value)
        self._action_log.append({
            "auto_id": pending.auto_id,
            "tid": pending.tid,
            "field": field,
            "value": value,
            "time": time.monotonic(),
        })

    @staticmethod
    def _evaluate(value: float, op: str, threshold: float) -> bool:
        if op == "above":
            return value > threshold
        elif op == "below":
            return value < threshold
        elif op == "equals":
            return value == threshold
        elif op == "not_equals":
            return value != threshold
        return False


# ── Simulation harness ────────────────────────────────────────────────────────

class AutoSimulation:
    """
    Runs multiple DeviceWorkers connected via in-memory message passing.
    Replaces CoAP/UDP transport with direct queue injection.
    """

    def __init__(self):
        self._devices: dict[str, DeviceWorker] = {}

    def add_device(self, name: str, config: dict) -> DeviceWorker:
        worker = DeviceWorker(name, config, self._send)
        self._devices[name] = worker
        return worker

    def start_all(self):
        for w in self._devices.values():
            w.start()

    def stop_all(self):
        for w in self._devices.values():
            w.stop()

    def get_device(self, name: str) -> DeviceWorker:
        return self._devices[name]

    def _send(self, target: str, msg: dict):
        if target == "__broadcast__":
            for w in self._devices.values():
                w.receive(msg)
        elif target in self._devices:
            self._devices[target].receive(msg)
        else:
            log.warning("Send to unknown device: %s", target)
