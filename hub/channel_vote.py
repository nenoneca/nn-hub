"""Channel vote: combine per-voter energy scans into one channel choice.

Each voter (a gateway or a sensor) scanned channels 11..26 and reported the
maximum energy it heard on each (dBm; None = no reading).  Borda count:
every voter ranks its allowed channels quietest first and gives its k best
k, k-1, ..., 1 points (times its weight).  A channel any voter heard at or
above the veto level is excluded outright: at -60 dBm the ESP32 radio's
clear-channel check (energy detect) already calls it busy, so a single
device next to a Wi-Fi access point can make a channel unusable for
everyone behind it.

Readings at or below `quiet_dbm` (default -85, near the radio's noise
floor) count as equally clear: one sample of -90 vs -99 dBm is noise, not
a reason to move.  Tied channels share the points of the ranks they span.

decide() adds the hysteresis: stay while the current channel is clear at
every voter; otherwise move only when it is vetoed, out of the top k, or
beaten by at least `margin` points.
"""
from __future__ import annotations

from typing import Iterable, Optional

CHANNELS = range(11, 27)
NO_READING = 127


def parse_allowed(spec: str | Iterable[int] | None) -> set[int]:
    """'11-26' / '15,20,25' / '11-14,25' / iterable -> set of channels."""
    if spec is None or spec == "":
        return set(CHANNELS)
    if not isinstance(spec, str):
        return {int(c) for c in spec if 11 <= int(c) <= 26}
    out: set[int] = set()
    for part in spec.replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.update(range(int(a), int(b) + 1))
        else:
            out.add(int(part))
    return {c for c in out if 11 <= c <= 26}


def scan_from_bytes(b: bytes) -> dict[int, Optional[int]]:
    """16 signed bytes (channel 11..26) -> {channel: dBm or None}."""
    out: dict[int, Optional[int]] = {}
    for i, ch in enumerate(CHANNELS):
        if i >= len(b):
            out[ch] = None
            continue
        v = b[i] - 256 if b[i] > 127 else b[i]
        out[ch] = None if v == NO_READING else v
    return out


def vote(scans: list[dict], k: int = 3, allowed: Optional[set[int]] = None,
         veto_dbm: int = -60, quiet_dbm: int = -85) -> list[dict]:
    """scans: [{"voter": str, "weight": float, "dbm": {ch: int|None}}].
    Returns one row per allowed channel, best first:
      {"channel", "points", "ranks": {voter: rank}, "worst_dbm", "vetoed_by": [voters]}"""
    allowed = set(CHANNELS) if allowed is None else set(allowed)
    rows = {ch: {"channel": ch, "points": 0.0, "ranks": {}, "worst_dbm": None,
                 "vetoed_by": []} for ch in sorted(allowed)}
    for s in scans:
        dbm = s.get("dbm") or {}
        w = float(s.get("weight", 1.0))
        readings = [(v, ch) for ch, v in dbm.items() if ch in allowed and v is not None]
        for v, ch in readings:
            r = rows[ch]
            r["worst_dbm"] = v if r["worst_dbm"] is None else max(r["worst_dbm"], v)
            if v >= veto_dbm:
                r["vetoed_by"].append(s["voter"])
        # quietest first; everything at/below the quiet level ties
        readings = sorted((max(v, quiet_dbm), ch) for v, ch in readings)
        pos = 0
        while pos < len(readings):
            end = pos
            while end + 1 < len(readings) and readings[end + 1][0] == readings[pos][0]:
                end += 1
            # ranks pos+1..end+1 share their points
            pts = sum(max(0, k + 1 - r) for r in range(pos + 1, end + 2)) / (end - pos + 1)
            for _, ch in readings[pos:end + 1]:
                if pts > 0:
                    rows[ch]["points"] += w * pts
                    rows[ch]["ranks"][s["voter"]] = pos + 1
            pos = end + 1
    return sorted(rows.values(),
                  key=lambda r: (bool(r["vetoed_by"]), -r["points"],
                                 r["worst_dbm"] if r["worst_dbm"] is not None else 999,
                                 r["channel"]))


def decide(current: Optional[int], ranking: list[dict], k: int = 3,
           margin: float = 1.0, quiet_dbm: int = -85) -> dict:
    """Pick the winner and say whether moving is worth it."""
    usable = [r for r in ranking if not r["vetoed_by"] and r["points"] > 0]
    if not usable:
        return {"winner": None, "current": current, "move": False,
                "reason": "no usable channel: every allowed channel is vetoed or unscored"}
    win = usable[0]
    cur = next((r for r in ranking if r["channel"] == current), None)
    if current is None:
        return {"winner": win["channel"], "current": None, "move": False,
                "reason": "current channel unknown"}
    if win["channel"] == current:
        return {"winner": current, "current": current, "move": False,
                "reason": "the current channel is already the best"}
    if cur is None:
        return {"winner": win["channel"], "current": current, "move": True,
                "reason": f"current channel {current} is not in the allowed list"}
    if cur["vetoed_by"]:
        return {"winner": win["channel"], "current": current, "move": True,
                "reason": f"channel {current} is at or above the busy level at "
                          f"{', '.join(cur['vetoed_by'])}"}
    if cur["worst_dbm"] is not None and cur["worst_dbm"] <= quiet_dbm:
        return {"winner": win["channel"], "current": current, "move": False,
                "reason": f"channel {current} is clear at every device "
                          f"(worst {cur['worst_dbm']} dBm)"}
    top = [r["channel"] for r in usable[:k]]
    if current not in top:
        return {"winner": win["channel"], "current": current, "move": True,
                "reason": f"channel {current} is not among the {k} best"}
    if win["points"] - cur["points"] >= margin:
        return {"winner": win["channel"], "current": current, "move": True,
                "reason": f"channel {win['channel']} scores {win['points']:g} vs "
                          f"{cur['points']:g} for {current}"}
    return {"winner": win["channel"], "current": current, "move": False,
            "reason": f"channel {win['channel']} is not better than {current} by "
                      f"{margin:g} points"}
