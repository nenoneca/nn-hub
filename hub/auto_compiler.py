"""
Automation compiler — transforms YAML automations into per-device config JSON.

Input:  automations.yaml (HA-style trigger/condition/action rules)
Output: per-device JSON payloads stored in the configs table via DB.set_config()

Compilation flow:
  1. Parse YAML
  2. Collect each device's roles across all automations
  3. Resolve device names (validate against DB)
  4. Estimate payload size vs device flash budget
  5. Apply compression: names → integer IDs → broadcast fallback
  6. Store per-device configs
"""

from __future__ import annotations
import base64
import json
import logging
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from .db import DB

log = logging.getLogger("hub.auto_compiler")

# ── Data model ────────────────────────────────────────────────────────────────

SUPPORTED_OPS = {"above", "below", "equals", "not_equals"}


@dataclass
class TriggerDef:
    auto_id: str
    field: str
    op: str
    threshold: float
    notify: list[str]        # device names to notify (condition + action devices)


@dataclass
class ConditionDef:
    auto_id: str
    field: str
    notify: list[str]        # action devices to push value to


@dataclass
class ExpectedCondition:
    device: str
    field: str
    op: str
    threshold: float
    on_timeout: bool         # value if condition not received within timeout


@dataclass
class ActionDef:
    auto_id: str
    timeout_ms: int
    expect_conditions: list[ExpectedCondition]
    do_field: str
    do_value: str | float | int | bool


@dataclass
class DeviceRoles:
    """Aggregated roles for one device across all automations."""
    triggers: list[TriggerDef] = field(default_factory=list)
    conditions: list[ConditionDef] = field(default_factory=list)
    actions: list[ActionDef] = field(default_factory=list)


# ── Compiler ──────────────────────────────────────────────────────────────────

class AutoCompiler:
    def __init__(self, db: DB):
        self._db = db

    def compile(self, yaml_path: Path,
                default_timeout_ms: int = 5000) -> dict[str, dict]:
        """
        Compile automations.yaml into per-device config payloads.

        Returns dict of {device_name: payload_dict}.
        Also stores each payload in the DB via set_config().
        """
        raw = yaml.safe_load(yaml_path.read_text())
        automations = raw.get("automations", [])
        if not automations:
            log.warning("No automations found in %s", yaml_path)
            return {}

        # Validate and collect roles
        device_roles: dict[str, DeviceRoles] = {}

        for auto in automations:
            auto_id = auto["id"]
            triggers = auto.get("trigger", [])
            conditions = auto.get("condition", [])
            actions = auto.get("action", [])

            if not triggers:
                raise ValueError(f"Automation '{auto_id}': no triggers")
            if not actions:
                raise ValueError(f"Automation '{auto_id}': no actions")

            # Collect all devices that need notification from triggers
            condition_devices = [c["device"] for c in conditions]
            action_devices = [a["device"] for a in actions]
            notify_list = list(set(condition_devices + action_devices))

            # Triggers
            for trg in triggers:
                dev = trg["device"]
                op = self._resolve_op(trg)
                threshold = trg.get(op)
                self._validate_field(dev, trg["field"], "trigger")

                # Exclude the trigger device itself from the notify list —
                # auto_engine.handle_trigger() already dispatches same-device
                # actions locally without needing a D2D AUTO_NOTIFY.  Leaving
                # the trigger device in notify[] used to make the runtime
                # try to D2D-send to its own ML-EID, wastes a reliable-layer
                # slot per cycle, and eventually starves cross-device targets
                # with NO_SLOT.
                roles = device_roles.setdefault(dev, DeviceRoles())
                notify_no_self = [n for n in notify_list if n != dev]
                roles.triggers.append(TriggerDef(
                    auto_id=auto_id,
                    field=trg["field"],
                    op=op,
                    threshold=threshold,
                    notify=notify_no_self,
                ))

            # Conditions
            for cond in conditions:
                dev = cond["device"]
                self._validate_field(dev, cond["field"], "condition")
                roles = device_roles.setdefault(dev, DeviceRoles())
                roles.conditions.append(ConditionDef(
                    auto_id=auto_id,
                    field=cond["field"],
                    notify=action_devices,
                ))

            # Actions
            for act in actions:
                dev = act["device"]
                self._validate_field(dev, act["field"], "action")
                roles = device_roles.setdefault(dev, DeviceRoles())

                expect = []
                for cond in conditions:
                    cond_op = self._resolve_op(cond)
                    expect.append(ExpectedCondition(
                        device=cond["device"],
                        field=cond["field"],
                        op=cond_op,
                        threshold=cond.get(cond_op, 0),
                        on_timeout=cond.get("on_timeout", False),
                    ))

                roles.actions.append(ActionDef(
                    auto_id=auto_id,
                    timeout_ms=auto.get("timeout_ms", default_timeout_ms),
                    expect_conditions=expect,
                    do_field=act["field"],
                    do_value=act["value"],
                ))

        # Generate per-device payloads
        payloads: dict[str, dict] = {}
        for dev_name, roles in device_roles.items():
            payload = self._build_payload(dev_name, roles)
            payloads[dev_name] = payload

            # Resolve device name to ID and store config
            device = self._db.get_device_by_name(dev_name)
            if device:
                self._db.set_config(device.id, payload)
                log.info("Compiled config for %s (%s): %d triggers, "
                         "%d conditions, %d actions",
                         dev_name, device.id,
                         len(roles.triggers), len(roles.conditions),
                         len(roles.actions))
            else:
                log.warning("Device '%s' not found in DB — config not stored",
                            dev_name)

        return payloads

    def _validate_field(self, device_name: str, field: str, role: str) -> None:
        """Warn if device doesn't declare the field in capabilities."""
        caps = self._db.get_capabilities(device_name)
        if not caps:
            return  # no capabilities reported yet — skip validation
        field_names = [c.get("n", c.get("name", "")) for c in caps]
        if field not in field_names:
            log.warning("Device '%s' does not declare field '%s' "
                        "(role=%s, known fields: %s)",
                        device_name, field, role, field_names)

    def _resolve_name_to_addr(self, name: str) -> str:
        """Map device name → ML-EID IPv6 address for Thread mesh routing."""
        dev = self._db.get_device_by_name(name)
        if dev:
            pi = self._db.get_provision_info(dev.id)
            if pi and pi.ml_eid:
                return pi.ml_eid
        log.warning("No IPv6 address for '%s', using name", name)
        return name

    def _resolve_op(self, item: dict) -> str:
        """Find which operator key is present."""
        for op in SUPPORTED_OPS:
            if op in item:
                return op
        raise ValueError(f"No supported operator in {item}")

    def _build_payload(self, dev_name: str, roles: DeviceRoles) -> dict:
        """Build the JSON config payload for one device."""
        payload: dict = {"v": 1}

        if roles.triggers:
            payload["triggers"] = [
                {
                    "auto_id": t.auto_id,
                    "field": t.field,
                    "op": t.op,
                    "threshold": t.threshold,
                    "notify": t.notify,
                }
                for t in roles.triggers
            ]

        if roles.conditions:
            payload["conditions"] = [
                {
                    "auto_id": c.auto_id,
                    "field": c.field,
                    "notify": c.notify,
                }
                for c in roles.conditions
            ]

        if roles.actions:
            payload["actions"] = [
                {
                    "auto_id": a.auto_id,
                    "timeout_ms": a.timeout_ms,
                    "expect_conditions": [
                        {
                            "device": ec.device,
                            "field": ec.field,
                            "op": ec.op,
                            "threshold": ec.threshold,
                            "on_timeout": ec.on_timeout,
                        }
                        for ec in a.expect_conditions
                    ],
                    "do": {"field": a.do_field, "value": a.do_value},
                }
                for a in roles.actions
            ]

        return payload

    # ── Binary format ─────────────────────────────────────────────────

    OP_MAP = {"above": 0, "below": 1, "equals": 2, "not_equals": 3}
    HDR_FMT = "<BBH"

    def compile_binary(self, yaml_path: Path,
                       default_timeout_ms: int = 5000) -> dict[str, bytes]:
        """
        Compile automations into per-device variable-length binary blobs.

        Binary format (little-endian):
          Header: version(1) rule_count(1) reserved(2)
          Per rule:
            uint8  auto_id_len, char auto_id[],
            uint8  role, uint8 op, uint8 notify_count, uint8 on_timeout,
            float  threshold, float value, uint32 timeout_ms,
            uint8  field_len, char field[],
            per notify: uint8 name_len, char name[]

        Returns dict of {device_name: bytes}.
        """
        raw = yaml.safe_load(yaml_path.read_text())
        automations = raw.get("automations", [])
        if not automations:
            return {}

        device_roles: dict[str, DeviceRoles] = {}
        self._collect_roles(automations, device_roles, default_timeout_ms)

        # Resolve device names → EUI-64 in all notify lists
        for roles in device_roles.values():
            for t in roles.triggers:
                t.notify = [self._resolve_name_to_addr(n) for n in t.notify]
            for c in roles.conditions:
                c.notify = [self._resolve_name_to_addr(n) for n in c.notify]
            for a in roles.actions:
                for ec in a.expect_conditions:
                    ec.device = self._resolve_name_to_addr(ec.device)

        binaries: dict[str, bytes] = {}
        for dev_name, roles in device_roles.items():
            parts: list[bytes] = []

            for t in roles.triggers:
                parts.append(self._pack_trigger(t))
            for c in roles.conditions:
                parts.append(self._pack_condition(c))
            for a in roles.actions:
                parts.append(self._pack_action(a))

            hdr = struct.pack(self.HDR_FMT, 1, len(parts), 0)
            blob = hdr + b"".join(parts)
            binaries[dev_name] = blob

            device = self._db.get_device_by_name(dev_name)
            if device:
                payload = {"v": 1, "auto_bin": base64.b64encode(blob).decode()}
                self._db.set_config(device.id, payload)
                log.info("Binary config for %s: %d rules, %d bytes",
                         dev_name, len(parts), len(blob))

        return binaries

    def _collect_roles(self, automations: list, device_roles: dict,
                       default_timeout_ms: int):
        """Collect roles from automations (shared logic)."""
        for auto in automations:
            auto_id = auto["id"]
            triggers = auto.get("trigger", [])
            conditions = auto.get("condition", [])
            actions = auto.get("action", [])

            if not triggers:
                raise ValueError(f"Automation '{auto_id}': no triggers")
            if not actions:
                raise ValueError(f"Automation '{auto_id}': no actions")

            condition_devices = [c["device"] for c in conditions]
            action_devices = [a["device"] for a in actions]
            notify_list = list(set(condition_devices + action_devices))

            for trg in triggers:
                dev = trg["device"]
                op = self._resolve_op(trg)
                # See comment in compile() — drop the trigger device from
                # its own notify[] list; local actions go through
                # handle_trigger() instead of a D2D self-send.
                notify_no_self = [n for n in notify_list if n != dev]
                roles = device_roles.setdefault(dev, DeviceRoles())
                roles.triggers.append(TriggerDef(
                    auto_id=auto_id, field=trg["field"], op=op,
                    threshold=trg.get(op), notify=notify_no_self,
                ))

            for cond in conditions:
                dev = cond["device"]
                roles = device_roles.setdefault(dev, DeviceRoles())
                roles.conditions.append(ConditionDef(
                    auto_id=auto_id, field=cond["field"],
                    notify=action_devices,
                ))

            for act in actions:
                dev = act["device"]
                roles = device_roles.setdefault(dev, DeviceRoles())
                expect = []
                for cond in conditions:
                    cond_op = self._resolve_op(cond)
                    expect.append(ExpectedCondition(
                        device=cond["device"], field=cond["field"],
                        op=cond_op, threshold=cond.get(cond_op, 0),
                        on_timeout=cond.get("on_timeout", False),
                    ))
                roles.actions.append(ActionDef(
                    auto_id=auto_id,
                    timeout_ms=auto.get("timeout_ms", default_timeout_ms),
                    expect_conditions=expect,
                    do_field=act["field"],
                    do_value=act["value"],
                ))

    @staticmethod
    def _lps(s: str) -> bytes:
        """Length-prefixed string: uint8 len + bytes."""
        b = s.encode("utf-8")[:255]
        return struct.pack("B", len(b)) + b

    def _pack_rule_common(self, auto_id: str, role: int, op: int,
                          notify_count: int, on_timeout: int,
                          threshold: float, value: float,
                          timeout_ms: int, field: str,
                          notify_names: list[str]) -> bytes:
        buf = self._lps(auto_id)
        buf += struct.pack("<BBBBffI", role, op, notify_count, on_timeout,
                           threshold, value, timeout_ms)
        buf += self._lps(field)
        for name in notify_names[:notify_count]:
            buf += self._lps(name)
        return buf

    def _pack_trigger(self, t: TriggerDef) -> bytes:
        return self._pack_rule_common(
            t.auto_id, ord('T'),
            self.OP_MAP.get(t.op, 0xFF),
            len(t.notify[:4]), 0,
            float(t.threshold), 0.0, 0,
            t.field, t.notify[:4],
        )

    def _pack_condition(self, c: ConditionDef) -> bytes:
        return self._pack_rule_common(
            c.auto_id, ord('C'),
            0xFF, len(c.notify[:4]), 0,
            0.0, 0.0, 0,
            c.field, c.notify[:4],
        )

    def _pack_action(self, a: ActionDef) -> bytes:
        cond_devices = [ec.device for ec in a.expect_conditions[:4]]
        ec = a.expect_conditions[0] if a.expect_conditions else None

        do_val = a.do_value
        if isinstance(do_val, bool):
            do_val = 1.0 if do_val else 0.0
        elif isinstance(do_val, str):
            s = do_val.strip().lower()
            if s in ("off", "false", "no", "0"):
                do_val = 0.0
            elif s in ("on", "true", "yes", "1"):
                do_val = 1.0
            else:
                raise ValueError(
                    f"Automation '{a.auto_id}': action value '{do_val}' "
                    "must be a number or on/off/true/false/yes/no"
                )
        elif not isinstance(do_val, (int, float)):
            raise ValueError(
                f"Automation '{a.auto_id}': action value "
                f"{do_val!r} has unsupported type {type(do_val).__name__}"
            )

        return self._pack_rule_common(
            a.auto_id, ord('A'),
            self.OP_MAP.get(ec.op, 0xFF) if ec else 0xFF,
            len(cond_devices),
            int(ec.on_timeout) if ec else 0,
            float(ec.threshold) if ec else 0.0,
            float(do_val), a.timeout_ms,
            a.do_field, cond_devices,
        )
