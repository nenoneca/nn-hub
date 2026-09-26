"""Radio/mesh health from devices' RADIO_STATS reports (cmd 0x0045).

Devices send CUMULATIVE counters since boot every ~5 min (node_mgr
radio_stats.c, v1 = 59 bytes LE).  Rates come from two reports: a lost
report only costs resolution, which matters because the reports travel
over the very channel whose health they describe.  The hub adds its own
view -- messages received from the device and how many were REPEATs (the
device retransmitted because our ack never reached it) -- sampled at the
moment each report arrives so both sides cover the same interval.

Why these ratios, not "packet loss": since the driver reports tx failures
and OpenThread retries them (2026-09-24), end-to-end loss stays low while
the channel is already mostly busy.  The early signal is the CCA-busy share
of transmit attempts; no-ack without busy points at a weak link, which a
channel change would not fix.
"""
from __future__ import annotations

import struct
from typing import Optional

V1_LEN = 59
ROLES = {0: "disabled", 1: "detached", 2: "child", 3: "router", 4: "leader"}

_FIELDS_U32 = ("uptime_s", "tx_ok", "tx_cca_busy", "tx_no_ack", "tx_other",
               "mac_tx_total", "mac_tx_retry", "mac_rx_total", "mac_rx_err",
               "ip_rx_fail", "ip_tx_fail")
COUNTERS = _FIELDS_U32[1:] + ("parent_changes", "attach_attempts", "detached",
                              "hub_rx", "hub_repeats", "hub_burst_copies")


def parse_v1(body: bytes) -> Optional[dict]:
    """Decode a v1 RADIO_STATS body; None if it is not one."""
    if len(body) < V1_LEN or body[0] != 1:
        return None
    ver, flags, ch, role, parent, rssi, lqi, lqo = struct.unpack_from("<BBBBHbBB", body, 0)
    u = struct.unpack_from("<11I", body, 9)
    pc, aa, det = struct.unpack_from("<3H", body, 53)
    d = {"version": ver, "driver_counters": bool(flags & 1), "channel": ch,
         "role": ROLES.get(role, str(role)),
         "parent": None if parent == 0xFFFF else f"0x{parent:04x}",
         "parent_rssi": None if rssi == 127 else rssi,
         "parent_lq_in": lqi, "parent_lq_out": lqo,
         "parent_changes": pc, "attach_attempts": aa, "detached": det}
    d.update(dict(zip(_FIELDS_U32, u)))
    return d


def delta(a: dict, b: dict) -> dict:
    """Counter increase from report a to report b.  A reboot in between
    (uptime went backwards, or any counter did) means b counts from zero."""
    reboot = b.get("uptime_s", 0) < a.get("uptime_s", 0) or any(
        b.get(k, 0) < a.get(k, 0) for k in COUNTERS
        if not k.startswith("hub_"))
    out = {}
    for k in COUNTERS:
        base = 0 if (reboot and not k.startswith("hub_")) else a.get(k, 0)
        out[k] = max(0, b.get(k, 0) - base)
    out["seconds"] = max(1, b.get("ts", 0) - a.get("ts", 0))
    out["rebooted"] = reboot
    return out


def _pct(n: float, d: float) -> Optional[float]:
    return round(100.0 * n / d, 1) if d > 0 else None


def ratios(d: dict) -> dict:
    attempts = d["tx_ok"] + d["tx_cca_busy"] + d["tx_no_ack"] + d["tx_other"]
    sent_on_air = d["tx_ok"] + d["tx_no_ack"]
    return {
        "tx_attempts": attempts,
        "busy_pct": _pct(d["tx_cca_busy"], attempts),        # channel occupied
        "no_ack_pct": _pct(d["tx_no_ack"], sent_on_air),     # link quality
        # OT's txTotal counts each frame once and txRetry counts every extra
        # attempt (a frame can be retried several times), so retry/total can
        # pass 100%.  Report the share of on-air attempts that were retries.
        "mac_retry_pct": _pct(d["mac_tx_retry"], d["mac_tx_total"] + d["mac_tx_retry"]),
        "hub_repeat_pct": _pct(d["hub_repeats"], d["hub_rx"]),  # our acks lost
        "reassembly_fail_per_h": round(d["ip_rx_fail"] * 3600.0 / d["seconds"], 1),
        "parent_changes": d["parent_changes"],
        "seconds": d["seconds"],
        "rebooted": d["rebooted"],
    }


def summarize(reports: list[dict]) -> Optional[dict]:
    """reports: oldest..newest dicts (parsed + 'ts' + hub counters).
    Rates over the whole list: first vs last."""
    if not reports:
        return None
    last = reports[-1]
    out = {"latest": last, "reports": len(reports)}
    if len(reports) >= 2:
        out["window"] = ratios(delta(reports[0], last))
    return out
