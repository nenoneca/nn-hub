"""Which local Bluetooth adapter the hub's BLE tooling should use.

A host often has more than one: the OrangePi has a deaf built-in radio (no
antenna fitted) plus whatever USB dongle is plugged in, and the dev PC has a CSR
dongle.  BlueZ picks the *first* adapter by default, which is not necessarily
the one that can actually reach a device — so every BLE entry point takes an
adapter and passes it through to bleak.

Resolution order (first hit wins):

  1. an explicit argument  (``--ble-adapter hci1``)
  2. ``NN_BLE_ADAPTER`` in the environment — the hub-wide setting; put it in the
     systemd unit so every provisioning run uses the right radio
  3. ``None`` — let BlueZ choose, i.e. the previous behaviour

``list_adapters()`` exists so the user can see what the choices are before
picking one.  Note sysfs does NOT expose the BD address on every kernel (on the
dev PC ``/sys/class/bluetooth/hci0`` holds only device/power/rfkill*/subsystem/
uevent), so the address comes from ``hciconfig`` when available; the adapter
NAMES and the rfkill block state always come from sysfs.
"""

from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

ENV_VAR = "NN_BLE_ADAPTER"
_SYSFS = Path("/sys/class/bluetooth")


@dataclass
class Adapter:
    name: str                 # "hci0"
    address: str = ""         # "C0:FB:F9:62:27:96" ("" if unknown)
    up: bool | None = None    # None = unknown
    blocked: bool = False     # rfkill soft/hard block


def resolve(explicit: str | None = None) -> str | None:
    """Adapter name to hand to bleak, or None to let BlueZ decide."""
    val = (explicit or os.environ.get(ENV_VAR) or "").strip()
    return val or None


class UnknownAdapter(ValueError):
    """The configured adapter does not exist on this host."""


def kwargs(explicit: str | None = None) -> dict:
    """``**ble_adapter.kwargs(a)`` for BleakScanner / BleakClient.

    Empty dict when nothing is configured, so callers keep BlueZ's default
    behaviour rather than passing ``adapter=None`` explicitly.

    Raises UnknownAdapter if a name IS configured but absent.  This matters:
    bleak's BlueZ backend SILENTLY IGNORES an unknown adapter (verified —
    ``BleakScanner.discover(adapter="hci99")`` returns normally) and falls back
    to BlueZ's first radio.  That is the precise failure this module exists to
    prevent: you ask for the USB dongle, the dongle is not enumerated, and you
    silently scan on the deaf built-in radio instead and conclude the peripheral
    is missing.  Better to stop and say so.
    """
    a = resolve(explicit)
    if not a:
        return {}
    present = list_adapters()
    # Empty enumeration means we cannot tell (no /sys/class/bluetooth — e.g. the
    # hub in an LXC talking to the host's BlueZ over a proxied D-Bus socket).
    # Don't second-guess bleak in that case.
    if present and a not in {ad.name for ad in present}:
        raise UnknownAdapter(
            "BLE adapter %r not found. Available:\n%s\n"
            "Set a different one with --ble-adapter, or %s=hciN."
            % (a, describe(a), ENV_VAR))
    return {"adapter": a}


def _rfkill_blocked(hci_dir: Path) -> bool:
    for rf in hci_dir.glob("rfkill*"):
        try:
            if (rf / "soft").read_text().strip() != "0":
                return True
            if (rf / "hard").read_text().strip() != "0":
                return True
        except OSError:
            pass
    return False


def _hciconfig() -> dict[str, tuple[str, bool]]:
    """{hciN: (address, up)} from hciconfig; {} if it is unavailable."""
    try:
        out = subprocess.run(["hciconfig"], capture_output=True, text=True,
                             timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return {}
    info: dict[str, tuple[str, bool]] = {}
    cur = None
    for line in out.splitlines():
        m = re.match(r"^(hci\d+):", line)
        if m:
            cur = m.group(1)
            info[cur] = ("", False)
            continue
        if not cur:
            continue
        m = re.search(r"BD Address:\s*([0-9A-Fa-f:]{17})", line)
        if m:
            info[cur] = (m.group(1).upper(), info[cur][1])
        if re.search(r"\bUP\b", line):
            info[cur] = (info[cur][0], True)
    return info


def list_adapters() -> list[Adapter]:
    """Local BT adapters in hci order.  Empty if none / sysfs unavailable."""
    if not _SYSFS.is_dir():
        return []
    hci_info = _hciconfig()
    out: list[Adapter] = []
    for d in sorted(_SYSFS.iterdir(),
                    key=lambda p: (len(p.name), p.name)):   # hci2 before hci10
        if not d.name.startswith("hci"):
            continue
        addr, up = hci_info.get(d.name, ("", None))
        out.append(Adapter(d.name, addr, up, _rfkill_blocked(d)))
    return out


def describe(explicit: str | None = None) -> str:
    """Multi-line summary for CLI output; marks which one will be used."""
    ads = list_adapters()
    if not ads:
        return "  (no Bluetooth adapters found)"
    chosen = resolve(explicit)
    lines = []
    for i, a in enumerate(ads):
        if chosen:
            mark = "  <- selected" if chosen == a.name else ""
        else:
            mark = "  <- BlueZ default (%s unset)" % ENV_VAR if i == 0 else ""
        state = "up" if a.up else ("down" if a.up is not None else "?")
        if a.blocked:
            state += ", rfkill-BLOCKED"
        lines.append("  %-6s %-17s [%s]%s"
                     % (a.name, a.address or "address unknown", state, mark))
    if chosen and chosen not in {a.name for a in ads}:
        lines.append("  WARNING: selected %r is not present" % chosen)
    return "\n".join(lines)
