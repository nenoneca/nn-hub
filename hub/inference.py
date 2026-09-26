"""
Inference engine — pluggable ML processing for incoming telemetry.

Currently ships a rule-based stub that can be replaced with a real model
(TFLite, ONNX, scikit-learn, torch, etc.) without changing the call sites.

Contract:
  process(device_id, payload) -> list[dict]
    payload: the raw dict from TelemetryMsg.data
    returns: list of alert dicts (may be empty)
    each alert: {"level": "info"|"warn"|"critical", "message": str, "data": dict}

To swap in a real model:
  1. Subclass InferenceEngine and override process()
  2. Pass the subclass instance to CoapServer and HubServer
"""

from __future__ import annotations
import logging
from typing import Any

log = logging.getLogger("hub.inference")


class InferenceEngine:
    """Pluggable inference backend.  Default: threshold rules on numeric fields."""

    # Override these per deployment
    WARN_THRESHOLDS: dict[str, tuple[float, float]] = {
        # field_name: (low_warn, high_warn)
        "temperature": (-10.0, 60.0),
        "humidity":    (0.0, 100.0),
        "voltage":     (2.5, 4.3),
    }
    CRITICAL_THRESHOLDS: dict[str, tuple[float, float]] = {
        "temperature": (-20.0, 80.0),
        "humidity":    (0.0, 100.0),
        "voltage":     (2.0, 4.5),
    }

    def process(self, device_id: str, payload: dict[str, Any]) -> list[dict]:
        """
        Run inference on one telemetry payload.
        Returns a list of alert dicts (empty = all normal).
        """
        alerts = []
        for field, value in payload.items():
            if not isinstance(value, (int, float)):
                continue

            crit = self.CRITICAL_THRESHOLDS.get(field)
            if crit and not (crit[0] <= value <= crit[1]):
                alerts.append({
                    "level":   "critical",
                    "message": f"{field} {value} outside critical range {crit}",
                    "data":    {"field": field, "value": value, "range": crit},
                })
                continue

            warn = self.WARN_THRESHOLDS.get(field)
            if warn and not (warn[0] <= value <= warn[1]):
                alerts.append({
                    "level":   "warn",
                    "message": f"{field} {value} outside warn range {warn}",
                    "data":    {"field": field, "value": value, "range": warn},
                })

        if alerts:
            levels = [a["level"] for a in alerts]
            top = "critical" if "critical" in levels else "warn"
            log.warning("device=%s  inference=%s  alerts=%d",
                        device_id, top, len(alerts))
        return alerts
