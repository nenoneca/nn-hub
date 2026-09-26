"""
Network-transport gateway provisioner.

Mirrors hub.ble_gateway_provisioner one-to-one but pushes the same set
of writes over a TCP socket to gw_linux instead of over BLE GATT.  The
ECIES envelopes (WIFI_CRED, HUB_HOST, HUB_IDENTITY, OT_DATASET) are
unchanged — the gateway's protocol module is transport-neutral.

Frame protocol (see fw_common/include/fw_common/gw_net_prov.h):

    [u8 type][u16-BE len][payload...]

Discovery: mDNS service _nn-gw._tcp.local published by avahi on the
gateway, with the same `nn-gw-XXXXXX` instance name the BLE
advertisement uses.  Optional explicit address (`--net-addr host[:port]`)
skips the scan.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import socket
import struct
from pathlib import Path
from typing import Optional

from . import crypto
from .ble_gateway_provisioner import (
    HUB_HOST_MAX, HUB_ID_LEN, HUB_PUB_LEN, PSK_MAX, SSID_MAX,
    BleGatewayProvisionError as ProvisionError,  # re-use exception type
    ProvisioningResult,
    load_hub_identity,
    _encrypt_for_gateway as _ecies_envelope,
)

log = logging.getLogger("hub.net_gw")

DEFAULT_PORT = 8770
SERVICE_TYPE = "_nn-gw._tcp.local."

# Frame types — kept in lockstep with fw_common/gw_net_prov.h
REQ_GET_INFO     = 0x01
REQ_HUB_X25519   = 0x02
REQ_WIFI_CRED    = 0x03
REQ_HUB_HOST     = 0x04
REQ_HUB_IDENTITY = 0x05
REQ_OT_DATASET   = 0x06
REQ_COMMIT       = 0x07
REP_INFO         = 0x80
REP_OK           = 0x81
REP_ERR          = 0x82
NOTIFY_STATUS    = 0x90

# Status byte values (must match fw_common/gw_ble_prov.c)
_STATUS_IDLE              = 0x00
_STATUS_APPLYING          = 0x01
_STATUS_SUCCESS           = 0x02
_STATUS_ERROR_VALIDATION  = 0x03
_STATUS_ERROR_INCOMPLETE  = 0x04
_STATUS_ERROR_NVS         = 0x05
_STATUS_STAGED_WIFI       = 0x10
_STATUS_STAGED_HUB_HOST   = 0x11
_STATUS_STAGED_HUB_ID     = 0x12
_STATUS_STAGED_OT_DATASET = 0x13


# ── mDNS discovery ──────────────────────────────────────────────────


async def find_gateway_mdns(scan_time: float = 6.0,
                            name_filter: Optional[str] = None
                            ) -> Optional[tuple[str, int, str]]:
    """Browse `_nn-gw._tcp.local`, return (host, port, instance_name) of
    the first match or `None`.  When `name_filter` is set, match the
    instance name exactly (e.g. "nn-gw-4E5E09")."""
    try:
        from zeroconf import IPVersion
        from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf
    except ImportError as e:
        raise ProvisionError(
            "python-zeroconf not installed (pip install zeroconf)") from e

    found: list[tuple[str, int, str]] = []
    done = asyncio.Event()

    class Listener:
        def add_service(self, zc, type_, name):
            asyncio.create_task(self._resolve(zc, type_, name))

        def update_service(self, zc, type_, name):
            pass

        def remove_service(self, zc, type_, name):
            pass

        async def _resolve(self, zc, type_, name):
            info = await zc.async_get_service_info(type_, name, timeout=3000)
            if info is None:
                return
            inst = name.split(".", 1)[0]
            if name_filter and inst != name_filter:
                return
            addrs = info.parsed_addresses(IPVersion.V4Only)
            if not addrs:
                return
            found.append((addrs[0], info.port or DEFAULT_PORT, inst))
            done.set()

    azc = AsyncZeroconf()
    AsyncServiceBrowser(azc.zeroconf, SERVICE_TYPE, listener=Listener())
    try:
        await asyncio.wait_for(done.wait(), timeout=scan_time)
    except asyncio.TimeoutError:
        pass
    finally:
        await azc.async_close()

    return found[0] if found else None


# ── framed-TCP helpers ─────────────────────────────────────────────


async def _recv_full(reader: asyncio.StreamReader, n: int) -> bytes:
    """Read exactly n bytes or raise on close."""
    buf = b""
    while len(buf) < n:
        chunk = await reader.read(n - len(buf))
        if not chunk:
            raise ProvisionError(
                f"connection closed waiting for {n} bytes (got {len(buf)})")
        buf += chunk
    return buf


async def _send_frame(writer: asyncio.StreamWriter,
                      ftype: int, payload: bytes = b"") -> None:
    if len(payload) > 0xffff:
        raise ProvisionError(f"payload too large: {len(payload)} B")
    hdr = struct.pack(">BH", ftype, len(payload))
    writer.write(hdr)
    if payload:
        writer.write(payload)
    await writer.drain()


async def _recv_frame(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    hdr = await _recv_full(reader, 3)
    ftype, plen = struct.unpack(">BH", hdr)
    payload = await _recv_full(reader, plen) if plen else b""
    return ftype, payload


# ── core provisioning routine ──────────────────────────────────────


@dataclasses.dataclass
class _Sink:
    """Tracks STATUS notifications coming in asynchronously while we
    drive the request/response sequence."""
    statuses: asyncio.Queue


async def _wait_status(sink: _Sink, expected: int, label: str,
                       timeout: float = 15.0) -> None:
    try:
        st = await asyncio.wait_for(sink.statuses.get(), timeout=timeout)
    except asyncio.TimeoutError:
        raise ProvisionError(
            f"timed out waiting for STATUS=0x{expected:02x} ({label})")
    if st != expected:
        raise ProvisionError(
            f"expected STATUS=0x{expected:02x} ({label}), got 0x{st:02x}")


async def _frame_pump(reader: asyncio.StreamReader,
                      sink: _Sink,
                      reply_q: asyncio.Queue) -> None:
    """Background task: read frames and route NOTIFY_STATUS to the
    status queue, everything else to reply_q where the request driver
    is awaiting."""
    try:
        while True:
            ftype, payload = await _recv_frame(reader)
            if ftype == NOTIFY_STATUS and payload:
                log.debug("STATUS notify: 0x%02x", payload[0])
                sink.statuses.put_nowait(payload[0])
            else:
                reply_q.put_nowait((ftype, payload))
    except (ProvisionError, ConnectionError, asyncio.CancelledError):
        reply_q.put_nowait((-1, b""))  # signal EOF to awaiter


async def _request(writer: asyncio.StreamWriter, reply_q: asyncio.Queue,
                   ftype: int, payload: bytes,
                   expect: tuple[int, ...] = (REP_OK,)) -> tuple[int, bytes]:
    await _send_frame(writer, ftype, payload)
    rtype, rpayload = await asyncio.wait_for(reply_q.get(), timeout=15.0)
    if rtype == -1:
        raise ProvisionError("connection closed before reply")
    if rtype == REP_ERR:
        err = rpayload[0] if rpayload else 0
        raise ProvisionError(
            f"gateway rejected request 0x{ftype:02x}: err=0x{err:02x}")
    if rtype not in expect:
        raise ProvisionError(
            f"unexpected reply type 0x{rtype:02x} for request 0x{ftype:02x}")
    return rtype, rpayload


async def provision_gateway_net(
        ssid: str,
        psk: str,
        hub_host: str,
        hub_x25519_priv,
        ot_dataset_tlvs: bytes,
        *,
        address: Optional[str] = None,     # "host" or "host:port"
        scan_time: float = 6.0,
        privkey_path: Optional[Path] = None,
) -> ProvisioningResult:
    """Provision a fresh gateway over TCP.  Same wire-format semantics
    as ble_gateway_provisioner.provision_gateway; raises
    BleGatewayProvisionError on any failure (re-exported as
    `ProvisionError` from this module for symmetry)."""

    if not (0 < len(ssid) <= SSID_MAX):
        raise ProvisionError(f"ssid len {len(ssid)} not in 1..{SSID_MAX}")
    if not (0 <= len(psk) <= PSK_MAX):
        raise ProvisionError(f"psk len {len(psk)} not in 0..{PSK_MAX}")
    if not (0 < len(hub_host) <= HUB_HOST_MAX):
        raise ProvisionError(
            f"hub_host len {len(hub_host)} not in 1..{HUB_HOST_MAX}")
    if not ot_dataset_tlvs or len(ot_dataset_tlvs) > 254:
        raise ProvisionError(
            f"ot_dataset_tlvs len {len(ot_dataset_tlvs)} not in 1..254")

    hub_id, hub_pub = load_hub_identity(privkey_path)

    # ── resolve host:port ──────────────────────────────────────────
    instance = None
    if address:
        if ":" in address:
            host, port_s = address.rsplit(":", 1)
            port = int(port_s)
        else:
            host = address
            port = DEFAULT_PORT
        log.info("net: using explicit address %s:%d (no mDNS scan)", host, port)
    else:
        log.info("net: scanning mDNS for %s (%.0fs)...", SERVICE_TYPE, scan_time)
        hit = await find_gateway_mdns(scan_time=scan_time)
        if not hit:
            raise ProvisionError(
                f"no gateway found publishing {SERVICE_TYPE}")
        host, port, instance = hit
        log.info("net: found gateway %r at %s:%d", instance, host, port)

    # ── connect ───────────────────────────────────────────────────
    log.info("net: connecting to %s:%d...", host, port)
    reader, writer = await asyncio.open_connection(host, port)

    sink = _Sink(statuses=asyncio.Queue())
    reply_q: asyncio.Queue = asyncio.Queue()
    pump = asyncio.create_task(_frame_pump(reader, sink, reply_q))

    try:
        # ── GW_INFO ───────────────────────────────────────────────
        _, info = await _request(
            writer, reply_q, REQ_GET_INFO, b"", expect=(REP_INFO,))
        EXPECTED_LEN = 1 + HUB_ID_LEN + HUB_PUB_LEN + 32
        if len(info) != EXPECTED_LEN:
            raise ProvisionError(
                f"GW_INFO bad length {len(info)} (expected {EXPECTED_LEN})")
        if info[0] != 2:
            raise ProvisionError(f"GW_INFO schema {info[0]} (expected 2)")
        gw_id         = bytes(info[1:1 + HUB_ID_LEN])
        gw_p256_pub   = bytes(info[1 + HUB_ID_LEN:1 + HUB_ID_LEN + HUB_PUB_LEN])
        gw_x25519_pub = bytes(info[1 + HUB_ID_LEN + HUB_PUB_LEN:])
        if gw_p256_pub[0] != 0x04:
            raise ProvisionError(
                f"GW_INFO p256 prefix 0x{gw_p256_pub[0]:02x} (expected 0x04)")
        log.info("net: gateway id=%s  x25519=%s...",
                 gw_id.hex(), gw_x25519_pub.hex()[:16])

        # ── HUB_X25519 bootstrap (plaintext) ──────────────────────
        hub_x25519_pub = crypto.x25519_pubkey_bytes(hub_x25519_priv)
        await _request(writer, reply_q, REQ_HUB_X25519, hub_x25519_pub)
        log.info("net: hub X25519 pub written (%d bytes)", len(hub_x25519_pub))

        # ── WIFI_CRED ──────────────────────────────────────────────
        wifi_pt = (bytes([len(ssid)]) + ssid.encode() +
                   bytes([len(psk)])  + psk.encode())
        wifi_env = _ecies_envelope(wifi_pt, gw_x25519_pub, hub_x25519_priv)
        await _request(writer, reply_q, REQ_WIFI_CRED, wifi_env)
        await _wait_status(sink, _STATUS_STAGED_WIFI, "STAGED_WIFI")

        # ── HUB_HOST ──────────────────────────────────────────────
        host_pt  = bytes([len(hub_host)]) + hub_host.encode()
        host_env = _ecies_envelope(host_pt, gw_x25519_pub, hub_x25519_priv)
        await _request(writer, reply_q, REQ_HUB_HOST, host_env)
        await _wait_status(sink, _STATUS_STAGED_HUB_HOST, "STAGED_HUB_HOST")

        # ── HUB_IDENTITY ──────────────────────────────────────────
        ident_pt  = hub_id + hub_pub
        ident_env = _ecies_envelope(ident_pt, gw_x25519_pub, hub_x25519_priv)
        await _request(writer, reply_q, REQ_HUB_IDENTITY, ident_env)
        await _wait_status(sink, _STATUS_STAGED_HUB_ID, "STAGED_HUB_ID")

        # ── OT_DATASET ────────────────────────────────────────────
        ds_env = _ecies_envelope(ot_dataset_tlvs, gw_x25519_pub, hub_x25519_priv)
        log.info("net: writing OT_DATASET (%d B plain → %d B envelope)",
                 len(ot_dataset_tlvs), len(ds_env))
        await _request(writer, reply_q, REQ_OT_DATASET, ds_env)
        await _wait_status(sink, _STATUS_STAGED_OT_DATASET, "STAGED_OT_DATASET")

        # ── COMMIT ────────────────────────────────────────────────
        try:
            await _request(writer, reply_q, REQ_COMMIT, b"\x01")
        except ProvisionError as e:
            log.debug("COMMIT reply raised %s — checking status queue", e)
        await _wait_status(sink, _STATUS_APPLYING, "APPLYING", timeout=8.0)
        try:
            st = await asyncio.wait_for(sink.statuses.get(), timeout=8.0)
        except asyncio.TimeoutError:
            st = None
        if st != _STATUS_SUCCESS:
            await asyncio.sleep(2.0)
            if writer.is_closing():
                log.info("net: gateway closed connection after APPLYING — "
                         "treating as soft-success (gateway rebooted)")
            else:
                raise ProvisionError(
                    f"COMMIT did not reach SUCCESS (last status: "
                    f"0x{st:02x})" if st is not None else
                    "COMMIT did not reach SUCCESS (no further status)")
        else:
            log.info("net: STATUS=SUCCESS (gateway will reboot/restart)")
    finally:
        pump.cancel()
        try:
            await pump
        except (asyncio.CancelledError, Exception):
            pass
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    return ProvisioningResult(
        gateway_id=gw_id.hex(),
        gateway_pubkey=gw_p256_pub,
        address=f"{host}:{port}",
    )
