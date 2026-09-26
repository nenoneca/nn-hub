"""
Tests for automation compiler + distributed engine simulation.

Each test compiles a YAML automation, creates mock device workers,
and verifies the trigger → condition → action flow via queues.
"""

import base64
import json
import struct
import tempfile
import time
from pathlib import Path

import pytest
import yaml

from hub.auto_compiler import AutoCompiler
from hub.auto_engine import AutoSimulation, DeviceWorker
from hub.db import DB


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def db(tmp_path):
    """In-memory DB with 3 test devices registered."""
    db = DB(tmp_path / "test.db")
    db.register_device("dev-01", "sensor-01", "end_device", "")
    db.register_device("dev-02", "sensor-02", "end_device", "")
    db.register_device("dev-03", "sensor-03", "end_device", "")
    return db


def compile_yaml(db: DB, yaml_text: str) -> dict[str, dict]:
    """Helper: compile YAML text and return per-device payloads."""
    path = Path(tempfile.mktemp(suffix=".yaml"))
    path.write_text(yaml_text)
    compiler = AutoCompiler(db)
    return compiler.compile(path)


def wait_until(pred, timeout: float = 5.0, step: float = 0.05) -> bool:
    """Poll *pred* until true or *timeout*.  The engine delivers actions
    asynchronously; a fixed sleep flaked under CI load (nn-hub-verify #7)."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return True
        time.sleep(step)
    return pred()


def run_sim(payloads: dict[str, dict]) -> AutoSimulation:
    """Helper: create and start a simulation with the given payloads."""
    sim = AutoSimulation()
    for name, config in payloads.items():
        sim.add_device(name, config)
    sim.start_all()
    return sim


# ── Compiler Tests ────────────────────────────────────────────────────────────

class TestCompiler:

    def test_basic_compile(self, db):
        payloads = compile_yaml(db, """
automations:
  - id: test1
    trigger:
      - device: sensor-01
        field: temp
        above: 30
    condition:
      - device: sensor-02
        field: humidity
        above: 60
        on_timeout: false
    action:
      - device: sensor-03
        field: fan
        value: on
""")
        assert "sensor-01" in payloads
        assert "sensor-02" in payloads
        assert "sensor-03" in payloads

        # Trigger device
        trg = payloads["sensor-01"]
        assert trg["v"] == 1
        assert len(trg["triggers"]) == 1
        assert trg["triggers"][0]["field"] == "temp"
        assert trg["triggers"][0]["op"] == "above"
        assert trg["triggers"][0]["threshold"] == 30
        assert set(trg["triggers"][0]["notify"]) == {"sensor-02", "sensor-03"}

        # Condition device
        cond = payloads["sensor-02"]
        assert len(cond["conditions"]) == 1
        assert cond["conditions"][0]["field"] == "humidity"
        assert cond["conditions"][0]["notify"] == ["sensor-03"]

        # Action device
        act = payloads["sensor-03"]
        assert len(act["actions"]) == 1
        assert act["actions"][0]["auto_id"] == "test1"
        assert act["actions"][0]["do"]["field"] == "fan"
        assert len(act["actions"][0]["expect_conditions"]) == 1

    def test_unconditional_action(self, db):
        payloads = compile_yaml(db, """
automations:
  - id: simple
    trigger:
      - device: sensor-01
        field: button
        equals: 1
    condition: []
    action:
      - device: sensor-03
        field: led
        value: on
""")
        act = payloads["sensor-03"]["actions"][0]
        assert act["expect_conditions"] == []
        assert act["do"] == {"field": "led", "value": True}  # YAML 'on' → True

    def test_multiple_automations(self, db):
        payloads = compile_yaml(db, """
automations:
  - id: auto1
    trigger:
      - device: sensor-01
        field: temp
        above: 30
    condition: []
    action:
      - device: sensor-03
        field: fan
        value: on
  - id: auto2
    trigger:
      - device: sensor-01
        field: light
        below: 10
    condition: []
    action:
      - device: sensor-03
        field: led
        value: off
""")
        # sensor-01 has 2 triggers
        assert len(payloads["sensor-01"]["triggers"]) == 2
        # sensor-03 has 2 actions
        assert len(payloads["sensor-03"]["actions"]) == 2

    def test_db_config_stored(self, db):
        compile_yaml(db, """
automations:
  - id: t1
    trigger:
      - device: sensor-01
        field: x
        above: 0
    condition: []
    action:
      - device: sensor-03
        field: y
        value: 1
""")
        # Config should be stored in DB
        cfg = db.get_config("dev-01")
        assert cfg is not None
        assert cfg.version == 1
        assert "triggers" in cfg.payload

    def test_invalid_no_trigger(self, db):
        with pytest.raises(ValueError, match="no triggers"):
            compile_yaml(db, """
automations:
  - id: bad
    trigger: []
    condition: []
    action:
      - device: sensor-03
        field: x
        value: 1
""")

    def test_invalid_no_action(self, db):
        with pytest.raises(ValueError, match="no actions"):
            compile_yaml(db, """
automations:
  - id: bad
    trigger:
      - device: sensor-01
        field: x
        above: 0
    condition: []
    action: []
""")

    def test_compile_binary_basic(self, db):
        """Binary compiler produces correct header and rule count."""
        compiler = AutoCompiler(db)
        path = Path(tempfile.mktemp(suffix=".yaml"))
        path.write_text("""
automations:
  - id: t1
    trigger:
      - device: sensor-01
        field: temp
        above: 30
    condition:
      - device: sensor-02
        field: humidity
        above: 60
        on_timeout: false
    action:
      - device: sensor-03
        field: fan
        value: on
""")
        binaries = compiler.compile_binary(path)
        assert "sensor-01" in binaries
        assert "sensor-02" in binaries
        assert "sensor-03" in binaries

        # Header check
        blob = binaries["sensor-01"]
        ver, count, _ = struct.unpack("<BBH", blob[:4])
        assert ver == 1
        assert count == 1

    def test_compile_binary_rule_content(self, db):
        """Variable-length binary rules parse back correctly."""
        compiler = AutoCompiler(db)
        path = Path(tempfile.mktemp(suffix=".yaml"))
        path.write_text("""
automations:
  - id: cool
    trigger:
      - device: sensor-01
        field: temp
        above: 30.5
    condition: []
    action:
      - device: sensor-03
        field: fan
        value: 1
""")
        binaries = compiler.compile_binary(path)
        blob = binaries["sensor-01"]

        # Parse variable-length format
        pos = 4  # skip header
        id_len = blob[pos]; pos += 1
        auto_id = blob[pos:pos+id_len].decode(); pos += id_len
        role = blob[pos]; pos += 1
        op = blob[pos]; pos += 1
        _nc = blob[pos]; pos += 1
        _ot = blob[pos]; pos += 1
        threshold = struct.unpack_from("<f", blob, pos)[0]; pos += 4
        _val = struct.unpack_from("<f", blob, pos)[0]; pos += 4
        _tms = struct.unpack_from("<I", blob, pos)[0]; pos += 4
        fld_len = blob[pos]; pos += 1
        field = blob[pos:pos+fld_len].decode()

        assert auto_id == "cool"
        assert role == ord('T')
        assert op == 0  # above
        assert abs(threshold - 30.5) < 0.01
        assert field == "temp"

    def test_compile_binary_stored_in_db(self, db):
        """Binary blob stored as base64 in config payload."""
        compiler = AutoCompiler(db)
        path = Path(tempfile.mktemp(suffix=".yaml"))
        path.write_text("""
automations:
  - id: t1
    trigger:
      - device: sensor-01
        field: x
        above: 0
    condition: []
    action:
      - device: sensor-03
        field: y
        value: 1
""")
        binaries = compiler.compile_binary(path)

        cfg = db.get_config("dev-01")
        assert cfg is not None
        assert "auto_bin" in cfg.payload
        decoded = base64.b64decode(cfg.payload["auto_bin"])
        assert decoded == binaries["sensor-01"]

    def test_compile_binary_multiple_roles(self, db):
        """Device with multiple roles gets all rules in one blob."""
        compiler = AutoCompiler(db)
        path = Path(tempfile.mktemp(suffix=".yaml"))
        path.write_text("""
automations:
  - id: a1
    trigger:
      - device: sensor-01
        field: temp
        above: 30
    condition: []
    action:
      - device: sensor-01
        field: fan
        value: 1
""")
        binaries = compiler.compile_binary(path)

        blob = binaries["sensor-01"]
        _, count, _ = struct.unpack("<BBH", blob[:4])
        assert count == 2  # 1 trigger + 1 action

    def test_compile_binary_compact(self, db):
        """Variable-length format is more compact than fixed-size."""
        compiler = AutoCompiler(db)
        path = Path(tempfile.mktemp(suffix=".yaml"))
        path.write_text("""
automations:
  - id: x
    trigger:
      - device: sensor-01
        field: t
        above: 0
    condition: []
    action:
      - device: sensor-03
        field: y
        value: 1
""")
        binaries = compiler.compile_binary(path)
        # Short names → small blob
        blob = binaries["sensor-01"]
        assert len(blob) < 60  # header + short rule should be compact

    def test_value_string_aliases_encode_correctly(self, db):
        """Quoted strings 'on'/'off'/'true'/'false'/'no'/'yes'/'0'/'1' all
        encode to the right float in the binary blob.  Previously the
        compiler's truthy-string fallback turned 'off' into 1.0 because
        bool('off') is True."""
        compiler = AutoCompiler(db)
        for raw, expected in [
            ("off", 0.0), ("OFF", 0.0), ("false", 0.0),
            ("no",  0.0), ("0",   0.0),
            ("on",  1.0), ("true", 1.0),
            ("yes", 1.0), ("1",   1.0),
        ]:
            path = Path(tempfile.mktemp(suffix=".yaml"))
            path.write_text(f"""
automations:
  - id: r
    trigger:
      - device: sensor-01
        field: t
        above: 1
    condition: []
    action:
      - device: sensor-03
        field: led
        value: "{raw}"
""")
            binaries = compiler.compile_binary(path)
            blob = binaries["sensor-03"]
            # Layout per _pack_rule_common: skip 4B header + 1B id_len + id +
            # 1B role + 1B op + 1B nc + 1B ot + 4B threshold, then 4B value.
            pos = 4
            id_len = blob[pos]; pos += 1 + id_len + 4  # past id, role, op, nc, ot
            pos += 4  # past threshold
            val = struct.unpack_from("<f", blob, pos)[0]
            assert abs(val - expected) < 0.001, (
                f"value: \"{raw}\" → {val}, expected {expected}"
            )

    def test_value_unknown_string_rejected(self, db):
        """Garbage strings raise rather than silently encoding to 1.0."""
        compiler = AutoCompiler(db)
        path = Path(tempfile.mktemp(suffix=".yaml"))
        path.write_text("""
automations:
  - id: bad
    trigger:
      - device: sensor-01
        field: t
        above: 1
    condition: []
    action:
      - device: sensor-03
        field: led
        value: "wat"
""")
        with pytest.raises(ValueError, match="must be a number or on/off"):
            compiler.compile_binary(path)

    def test_all_operators(self, db):
        payloads = compile_yaml(db, """
automations:
  - id: ops
    trigger:
      - device: sensor-01
        field: a
        above: 10
      - device: sensor-01
        field: b
        below: 5
      - device: sensor-01
        field: c
        equals: 42
      - device: sensor-01
        field: d
        not_equals: 0
    condition: []
    action:
      - device: sensor-03
        field: out
        value: 1
""")
        triggers = payloads["sensor-01"]["triggers"]
        ops = {t["field"]: t["op"] for t in triggers}
        assert ops == {"a": "above", "b": "below", "c": "equals", "d": "not_equals"}


# ── Engine Simulation Tests ───────────────────────────────────────────────────

class TestEngine:

    def test_trigger_condition_action(self, db):
        """Full flow: trigger → condition push → action execution."""
        payloads = compile_yaml(db, """
automations:
  - id: cool
    trigger:
      - device: sensor-01
        field: temp
        above: 30
    condition:
      - device: sensor-02
        field: humidity
        above: 60
        on_timeout: false
    action:
      - device: sensor-03
        field: fan
        value: 1
""")
        sim = run_sim(payloads)
        try:
            # Pre-set condition device's humidity
            sim.get_device("sensor-02").set_field("humidity", 65)
            time.sleep(0.2)

            # Trigger
            sim.get_device("sensor-01").set_field("temp", 31)
            time.sleep(1.0)  # allow message propagation

            # Verify action executed
            wait_until(lambda: sim.get_device("sensor-03").get_field("fan") == 1)
            fan = sim.get_device("sensor-03").get_field("fan")
            assert fan == 1, f"fan should be 1, got {fan}"

            log = sim.get_device("sensor-03").action_log
            assert len(log) == 1
            assert log[0]["auto_id"] == "cool"
            assert log[0]["field"] == "fan"
            assert log[0]["value"] == 1
        finally:
            sim.stop_all()

    def test_unconditional_action(self, db):
        """Trigger with no conditions → immediate action."""
        payloads = compile_yaml(db, """
automations:
  - id: simple
    trigger:
      - device: sensor-01
        field: button
        equals: 1
    condition: []
    action:
      - device: sensor-03
        field: led
        value: 1
""")
        sim = run_sim(payloads)
        try:
            sim.get_device("sensor-01").set_field("button", 1)
            time.sleep(0.5)

            wait_until(lambda: sim.get_device("sensor-03").get_field("led") == 1)
            assert sim.get_device("sensor-03").get_field("led") == 1
            assert len(sim.get_device("sensor-03").action_log) == 1
        finally:
            sim.stop_all()

    def test_condition_not_met(self, db):
        """Condition value doesn't meet threshold → action skipped."""
        payloads = compile_yaml(db, """
automations:
  - id: cool
    trigger:
      - device: sensor-01
        field: temp
        above: 30
    condition:
      - device: sensor-02
        field: humidity
        above: 60
        on_timeout: false
    action:
      - device: sensor-03
        field: fan
        value: 1
""")
        sim = run_sim(payloads)
        try:
            # Humidity below threshold
            sim.get_device("sensor-02").set_field("humidity", 40)
            time.sleep(0.2)

            sim.get_device("sensor-01").set_field("temp", 31)
            time.sleep(1.0)

            # Action should NOT execute
            assert sim.get_device("sensor-03").get_field("fan") is None
            assert len(sim.get_device("sensor-03").action_log) == 0
        finally:
            sim.stop_all()

    def test_condition_timeout_false(self, db):
        """Condition device doesn't respond → on_timeout=false → action skipped."""
        payloads = compile_yaml(db, """
automations:
  - id: cool
    timeout_ms: 500
    trigger:
      - device: sensor-01
        field: temp
        above: 30
    condition:
      - device: sensor-02
        field: humidity
        above: 60
        on_timeout: false
    action:
      - device: sensor-03
        field: fan
        value: 1
""")
        # Don't add sensor-02 to simulation — it won't respond
        sim = AutoSimulation()
        sim.add_device("sensor-01", payloads["sensor-01"])
        sim.add_device("sensor-03", payloads["sensor-03"])
        sim.start_all()
        try:
            sim.get_device("sensor-01").set_field("temp", 31)
            time.sleep(1.0)  # > 500ms timeout

            # on_timeout=false → action skipped
            assert sim.get_device("sensor-03").get_field("fan") is None
            assert len(sim.get_device("sensor-03").action_log) == 0
        finally:
            sim.stop_all()

    def test_condition_timeout_true(self, db):
        """Condition device doesn't respond → on_timeout=true → action executes."""
        payloads = compile_yaml(db, """
automations:
  - id: lock
    timeout_ms: 500
    trigger:
      - device: sensor-01
        field: motion
        equals: 1
    condition:
      - device: sensor-02
        field: door
        equals: 1
        on_timeout: true
    action:
      - device: sensor-03
        field: alarm
        value: 1
""")
        # sensor-02 missing → condition times out → on_timeout=true
        sim = AutoSimulation()
        sim.add_device("sensor-01", payloads["sensor-01"])
        sim.add_device("sensor-03", payloads["sensor-03"])
        sim.start_all()
        try:
            sim.get_device("sensor-01").set_field("motion", 1)
            time.sleep(1.0)

            # on_timeout=true → action should execute
            wait_until(lambda: sim.get_device("sensor-03").get_field("alarm") == 1)
            assert sim.get_device("sensor-03").get_field("alarm") == 1
        finally:
            sim.stop_all()

    def test_edge_detection_no_retrigger(self, db):
        """Setting same value twice doesn't re-trigger."""
        payloads = compile_yaml(db, """
automations:
  - id: t1
    trigger:
      - device: sensor-01
        field: x
        above: 10
    condition: []
    action:
      - device: sensor-03
        field: y
        value: 1
""")
        sim = run_sim(payloads)
        try:
            sim.get_device("sensor-01").set_field("x", 15)
            time.sleep(0.5)
            sim.get_device("sensor-01").set_field("x", 15)  # same value
            time.sleep(0.5)

            # Should trigger only once
            assert len(sim.get_device("sensor-03").action_log) == 1
        finally:
            sim.stop_all()

    def test_multiple_automations_same_trigger(self, db):
        """Two automations triggered by the same device field."""
        payloads = compile_yaml(db, """
automations:
  - id: a1
    trigger:
      - device: sensor-01
        field: temp
        above: 30
    condition: []
    action:
      - device: sensor-03
        field: fan
        value: 1
  - id: a2
    trigger:
      - device: sensor-01
        field: temp
        above: 25
    condition: []
    action:
      - device: sensor-03
        field: alert
        value: 1
""")
        sim = run_sim(payloads)
        try:
            sim.get_device("sensor-01").set_field("temp", 31)
            time.sleep(0.5)

            # Both automations should fire
            wait_until(lambda: sim.get_device("sensor-03").get_field("fan") == 1
                       and sim.get_device("sensor-03").get_field("alert") == 1)
            assert sim.get_device("sensor-03").get_field("fan") == 1
            assert sim.get_device("sensor-03").get_field("alert") == 1
            assert len(sim.get_device("sensor-03").action_log) == 2
        finally:
            sim.stop_all()

    def test_below_operator(self, db):
        payloads = compile_yaml(db, """
automations:
  - id: cold
    trigger:
      - device: sensor-01
        field: temp
        below: 5
    condition: []
    action:
      - device: sensor-03
        field: heater
        value: 1
""")
        sim = run_sim(payloads)
        try:
            sim.get_device("sensor-01").set_field("temp", 3)
            time.sleep(0.5)

            assert sim.get_device("sensor-03").get_field("heater") == 1
        finally:
            sim.stop_all()

    def test_broadcast_mode(self, db):
        """Trigger with broadcast=true reaches all devices."""
        # Manually create configs with broadcast flag
        trigger_cfg = {
            "v": 1,
            "triggers": [{
                "auto_id": "bcast",
                "field": "x",
                "op": "above",
                "threshold": 0,
                "broadcast": True,
            }]
        }
        action_cfg = {
            "v": 1,
            "actions": [{
                "auto_id": "bcast",
                "timeout_ms": 5000,
                "expect_conditions": [],
                "do": {"field": "y", "value": 1},
            }]
        }

        sim = AutoSimulation()
        sim.add_device("trigger-dev", trigger_cfg)
        sim.add_device("action-dev", action_cfg)
        sim.add_device("bystander", {"v": 1})  # no rules
        sim.start_all()
        try:
            sim.get_device("trigger-dev").set_field("x", 1)
            time.sleep(0.5)

            assert sim.get_device("action-dev").get_field("y") == 1
            # Bystander unaffected
            assert sim.get_device("bystander").get_field("y") is None
        finally:
            sim.stop_all()
