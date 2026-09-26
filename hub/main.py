"""
nn-hub CLI

Commands:
  nn-hub serve                                 Start the hub server
  nn-hub identity                              Show hub ID and public key

  nn-hub device list                           List all devices
  nn-hub device register <id> <name> <type> <pubkey_b64>
  nn-hub device info <id>

  nn-hub config get <device_id>                Show stored config
  nn-hub config set <device_id> <json_file>    Update config (bumps version)

  nn-hub firmware upload <device_type> <version> <file>
  nn-hub firmware list

  nn-hub events [--limit N]                    Show recent event log
"""

from __future__ import annotations
import asyncio
import hashlib
import json
import logging
import shutil
import sys
import time
from pathlib import Path
from typing import Optional

import click

import base64

from .auth import load_or_generate, hub_id as get_hub_id, pubkey_b64 as get_pubkey_b64
from .crypto import (
    load_or_generate_enc_key, encode_hub_config,
    x25519_pubkey_bytes,
)
from .db import DB
from .inference import InferenceEngine
from .server import HubServer
from .syslog_server import SyslogServer
from .auto_compiler import AutoCompiler

DEFAULT_DATA_DIR = Path.home() / ".nn-hub"
DEFAULT_HOST     = "0.0.0.0"
DEFAULT_PORT     = 8765


# ── CLI root ──────────────────────────────────────────────────────────────────

@click.group()
@click.option("--data-dir", default=str(DEFAULT_DATA_DIR),
              envvar="NN_HUB_DATA_DIR", show_default=True,
              help="Hub data directory (key, database, firmware images)")
@click.option("--api-base", default="http://127.0.0.1:8769/api/v1",
              envvar="NN_HUB_API_BASE", show_default=True,
              help="REST API base URL used by `device read|write` and "
                   "other client-side commands.")
@click.option("--api-token", default=None, envvar="NN_HUB_API_TOKEN",
              help="Bearer token if the API requires auth.")
@click.option("-v", "--verbose", is_flag=True)
@click.pass_context
def cli(ctx, data_dir, api_base, api_token, verbose):
    """nn-hub — device registry, config sync, OTA server."""
    logging.basicConfig(
        level   = logging.DEBUG if verbose else logging.INFO,
        format  = "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt = "%H:%M:%S",
    )
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "firmware").mkdir(exist_ok=True)

    ctx.ensure_object(dict)
    ctx.obj["data_dir"]  = data_dir
    ctx.obj["db"]        = DB(data_dir / "hub.db")
    ctx.obj["privkey"]   = load_or_generate(data_dir)
    ctx.obj["api_base"]  = api_base
    ctx.obj["api_token"] = api_token


def _db(ctx) -> DB:
    return ctx.obj["db"]

def _data_dir(ctx) -> Path:
    return ctx.obj["data_dir"]


# ── identity ──────────────────────────────────────────────────────────────────

@cli.command()
@click.pass_context
def identity(ctx):
    """Show hub ID and Ed25519 public key."""
    privkey = ctx.obj["privkey"]
    click.echo(f"Hub ID:     {get_hub_id(privkey)}")
    click.echo(f"Public key: {get_pubkey_b64(privkey)}")
    click.echo(f"Data dir:   {_data_dir(ctx)}")


# ── serve ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--host", default=DEFAULT_HOST, show_default=True,
              help="Bind address for WebSocket server")
@click.option("--port", default=DEFAULT_PORT, show_default=True,
              help="WebSocket port gateways connect to")
@click.option("--proto-host", default="0.0.0.0", show_default=True,
              help="Bind address for nn_proto TCP server")
@click.option("--proto-port", default=8767, show_default=True,
              help="TCP port for nn_proto gateway connections")
@click.option("--fw-http-port", default=8770, show_default=True,
              help="HTTP port serving gateway-side OTA pulls")
@click.option("--log-rotate-min", default=60, show_default=True,
              help="Rotate the device-log SQLite store every N minutes")
@click.option("--api-host", default="127.0.0.1", show_default=True,
              help="Bind address for REST API (default: localhost only)")
@click.option("--api-port", default=8769, show_default=True,
              help="Port for the REST API")
@click.option("--api-token", default=None,
              help="Optional bearer token required for REST API. "
                   "If unset, no auth (safe with localhost bind).")
@click.pass_context
def serve(ctx, host, port, proto_host, proto_port,
          fw_http_port, log_rotate_min, api_host, api_port, api_token):
    """Start the hub server.  Thread devices use CoAP; gateways use WebSocket."""
    privkey  = ctx.obj["privkey"]
    db       = _db(ctx)
    data_dir = _data_dir(ctx)

    click.echo(f"Hub ID  : {get_hub_id(privkey)}")
    click.echo(f"Pubkey  : {get_pubkey_b64(privkey)}")
    click.echo(f"Data    : {data_dir}")
    click.echo(f"WS      : ws://{host}:{port}")
    click.echo(f"nn_proto: tcp://{proto_host}:{proto_port}")
    click.echo(f"FW-HTTP : http://0.0.0.0:{fw_http_port}/gw_firmware/<type>/<version>")
    click.echo(f"Logs    : {data_dir/'logs'}  (rotate every {log_rotate_min} min)")
    click.echo(f"REST API: http://{api_host}:{api_port}/api/v1  "
               f"(auth={'bearer' if api_token else 'off'})")
    click.echo()

    from .log_store import LogStore
    try:
        _cap_mb = int(db.get_setting("device_log_cap_mb", "256") or "256")
    except ValueError:
        _cap_mb = 256
    log_store = LogStore(data_dir / "logs",
                         rotate_interval_sec=int(log_rotate_min) * 60,
                         max_mb=_cap_mb)
    log_store.prune()                    # enforce the cap from boot, not
                                         # just from the next rotation

    inference = InferenceEngine()
    ws_server     = HubServer(db, privkey, host, port, data_dir, inference)
    syslog_server = SyslogServer(db=db, log_store=log_store)

    from .proto_server import ProtoServer
    proto_server = ProtoServer(db, host=proto_host, port=proto_port)

    # Wire the proto router (Phase 3.D): D2H → telemetry handler,
    # H2D send API hooked up via proto_router.
    from .proto_router import HubProtoRouter
    proto_router = HubProtoRouter(db, proto_server)
    proto_router.attach_log_store(log_store)
    proto_server.set_frame_handler(proto_router.handle_frame)

    # File-based H2D queue poller — picks up requests written by
    # `nn-hub h2d <device_id> <hex>`.
    h2d_queue = data_dir / "h2d_queue"

    async def _h2d_poller():
        import json as _json, os as _os
        h2d_queue.mkdir(parents=True, exist_ok=True)
        while True:
            await asyncio.sleep(0.5)
            for req in sorted(h2d_queue.iterdir()):
                if not req.name.endswith(".json"):
                    continue
                try:
                    body = _json.loads(req.read_text())
                    did = body["device_id"]
                    payload = bytes.fromhex(body["payload_hex"])
                    ok = await proto_router.send_h2d(did, payload)
                    click.echo(f"[h2d] {did}: {'sent' if ok else 'FAILED'}")
                except Exception as _e:
                    click.echo(f"[h2d] {req.name}: error {_e}")
                finally:
                    try: req.unlink()
                    except Exception: pass

    from .firmware_http import serve_firmware_http
    from .api import serve_api
    from .crypto import load_or_generate_enc_key
    from .firmware_catalog import FirmwareCatalog
    enc_priv = load_or_generate_enc_key(data_dir)
    # Phase 2 symmetric sessions: the router needs the hub's X25519 key
    # (the session/ECIES root) to derive per-device session keys.
    proto_router.set_enc_key(enc_priv)

    sources_yaml = data_dir / "sources.yaml"
    fw_cache_dir = data_dir / "firmware-cache"
    firmware_catalog = FirmwareCatalog(db, fw_cache_dir, sources_yaml)

    async def _firmware_catalog_boot():
        # Discover-once at boot, then start per-source poll loops.
        try:
            await firmware_catalog.sync_all()
        except Exception as _e:
            click.echo(f"[firmware-catalog] boot sync failed: {_e}")
        await firmware_catalog.start_polling()
        # Park forever so this task lives in the gather.
        await asyncio.Event().wait()

    # Hub-side failsafe for D2D cascades: watches the actuator events the
    # sensors push, and writes an action the target never reported.
    from .auto_failsafe import AutoFailsafe
    from . import coap_client as _cc
    auto_failsafe = AutoFailsafe(db, proto_router, enc_priv, _cc.set_field)
    proto_router.auto_failsafe = auto_failsafe

    # Thread channel scan / vote / migrate (manual via the API, automatic
    # when radio.channel.auto.enabled is set).
    from .channel_manager import ChannelManager
    channel_manager = ChannelManager(db, proto_router)
    proto_router.channel_manager = channel_manager

    async def _group_key_rotation_loop():
        # Scheduled cascade group-key rotation (Phase 4 hygiene): rotate
        # every `group_key_rotate_s` seconds (setting; default daily; 0
        # disables).  Event-driven rotation on /auto/compile still
        # applies — this loop is the upper bound on key age.  The timer
        # re-reads the setting each cycle so it can be changed live.
        while True:
            try:
                period = int(db.get_setting("group_key_rotate_s",
                                            "86400") or "86400")
            except (TypeError, ValueError):
                period = 86400
            if period <= 0:
                await asyncio.sleep(600)     # disabled; re-check later
                continue
            await asyncio.sleep(period)
            try:
                epoch = await proto_router.rotate_group_key()
                click.echo(f"[group-key] scheduled rotation -> epoch {epoch}")
            except Exception as _e:
                click.echo(f"[group-key] scheduled rotation failed: {_e}")

    async def _ota_schedule_runner():
        # Scheduled per-device OTA applies (webapp firmware panel).
        # Every 30 s: for each due entry, apply if the device is armed
        # with the scheduled version; hint it if it hasn't downloaded
        # yet (entry stays until the apply actually goes out).  Entries
        # self-clean once the device runs the scheduled version.
        import json as _json
        from .api import _ota_apply_device
        from . import proto as _proto
        while True:
            await asyncio.sleep(30)
            raw = db.get_setting("ota_schedules")
            try:
                sched = _json.loads(raw) if raw else {}
            except Exception:
                sched = {}
            if not sched:
                continue
            import time as _time
            now = int(_time.time())
            changed = False
            for dev_id, ent in list(sched.items()):
                if int(ent.get("at", 0)) > now:
                    continue
                d = db.get_device(dev_id)
                if d is None:
                    sched.pop(dev_id); changed = True
                    continue
                snap = proto_router.get_ota_status(dev_id)
                running = (snap.get("running_version") or "").split("+")[0]
                want = ent.get("version")
                if running == want:
                    sched.pop(dev_id); changed = True
                    continue
                if snap.get("state") == "armed" and \
                   (snap.get("armed_version") or "") == want:
                    res = await _ota_apply_device(proto_router, d)
                    click.echo(f"[ota-sched] {d.name} apply {want}: {res}")
                    if res.get("applied") or res.get("ack_lost"):
                        sched.pop(dev_id); changed = True
                else:
                    # Not downloaded yet — make the SCHEDULED version the
                    # active target (the device's OTA_CHECK compares
                    # against the target, so without this promote the
                    # hint answers "up-to-date" and nothing downloads),
                    # then nudge a download; apply on a later tick once
                    # armed.
                    img_key = (db.get_setting(f"device_image:{dev_id}")
                               or d.type)
                    try:
                        await firmware_catalog.promote(img_key, want, None)
                    except Exception as _e:
                        click.echo(f"[ota-sched] {d.name} promote {want} "
                                   f"failed: {_e}")
                        continue
                    try:
                        await proto_router.request_h2d(
                            dev_id, req_cmd=_proto.Cmd.OTA_HINT, body=b"",
                            expected_reply_cmd=_proto.Cmd.OTA_HINT_ACK,
                            timeout=8.0, max_attempts=2)
                    except Exception as _e:
                        click.echo(f"[ota-sched] {d.name} hint failed: {_e}")
            if changed:
                db.set_setting("ota_schedules", _json.dumps(sched))

    async def _video_flow_poller():
        # samples each camera's HLS sequence so the UI can tell
        # "server up" from "video actually arriving"
        from .api import video_flow_poller
        await video_flow_poller(db)

    from .metrics import MetricsCollector
    metrics = MetricsCollector(db)        # hourly per-camera reports from the media host (phase 5)

    async def _main():
        await asyncio.gather(
            metrics.run(),
            ws_server.start(),
            syslog_server.start(),
            proto_server.serve_forever(),
            _h2d_poller(),
            proto_router.ota_applying_watchdog(),
            _group_key_rotation_loop(),
            proto_router.group_key_repush_loop(),
            auto_failsafe.run(),
            channel_manager.run(),
            _ota_schedule_runner(),
            _video_flow_poller(),
            serve_firmware_http(db, host="0.0.0.0", port=fw_http_port),
            _firmware_catalog_boot(),
            serve_api(db, log_store, enc_priv,
                      host=api_host, port=api_port,
                      auth_token=api_token,
                      proto_router=proto_router,
                      firmware_catalog=firmware_catalog,
                      metrics=metrics),
        )

    asyncio.run(_main())


# ── device ────────────────────────────────────────────────────────────────────

# ── network ──────────────────────────────────────────────────────────────────
#
# The hub manages a single Thread (OT) network.  Its dataset is
# generated automatically the first time a gateway is provisioned
# (`gateway new`).  Subsequent gateways and devices share the same
# network identity.

@cli.group("ble")
def ble_group():
    """Local Bluetooth adapter selection."""


@ble_group.command("adapters")
@click.option("--ble-adapter", default=None,
              help="Show what this choice would resolve to")
def ble_adapters_cmd(ble_adapter):
    """List local BLE adapters and show which one the hub will use.

    A host often has several radios and BlueZ just takes the first, which may
    not be the one that can reach anything (the OrangePi's built-in radio has
    no antenna fitted and sees every device at the noise floor).
    """
    from . import ble_adapter as _ba
    click.echo("Local Bluetooth adapters:")
    click.echo(_ba.describe(ble_adapter))
    click.echo("")
    click.echo("Select per-command with  --ble-adapter hciN")
    click.echo("or hub-wide by setting   %s=hciN" % _ba.ENV_VAR)
    click.echo("  (e.g. Environment=%s=hci1 in the nn-hub systemd unit)" % _ba.ENV_VAR)


@cli.group()
def network():
    """Manage the hub's Thread (OT) network."""


@network.command("show")
@click.pass_context
def network_show(ctx):
    """Show the hub's active OT network identity."""
    from . import network as net_mod
    db = _db(ctx)
    net = net_mod.get_network(db)
    if net is None:
        click.echo("No network configured yet — will be auto-generated on the "
                   "first `nn-hub gateway new` invocation.")
        return
    click.echo(f"Name              : {net.name}")
    click.echo(f"Channel           : {net.channel}")
    click.echo(f"PAN ID            : 0x{net.panid:04x}")
    click.echo(f"Ext PAN ID        : {net.extpanid_hex}")
    click.echo(f"Network key       : (16 B, kept secret)")
    click.echo(f"Mesh-local prefix : {net.mesh_local_prefix_hex}::/64")
    tlvs = net_mod.get_dataset_tlvs(db)
    click.echo(f"Dataset TLVs      : {len(tlvs)} bytes")
    click.echo(f"  hex             : {tlvs.hex()}")
    # Topology: gateways / devices on this network
    from .db import role_name
    gws = db.list_gateways()
    devs = db.list_devices()
    click.echo(f"\nGateways          : {len(gws)}")
    for g in gws:
        last = "never" if not g.last_seen else f"{int(time.time()) - g.last_seen}s ago"
        if g.last_thread_state_at:
            role_str = f"{role_name(g.role)}(rloc16=0x{g.rloc16:04x})"
        else:
            role_str = "thread-state=unknown"
        click.echo(f"  {g.id}  {g.name:20s}  {role_str:30s}  last_seen={last}")
    click.echo(f"\nDevices           : {len(devs)}")
    for d in devs:
        last = "never" if not d.last_seen else f"{int(time.time()) - d.last_seen}s ago"
        click.echo(f"  {d.id}  {d.name:20s}  type={d.type:12s}  last_seen={last}")


@network.command("init")
@click.option("--channel", type=int, default=None,
              help="2.4 GHz 802.15.4 channel (11..26)")
@click.option("--name", type=str, default=None,
              help="Network name (1..16 ASCII)")
@click.option("--force", is_flag=True,
              help="Replace any existing network (DESTRUCTIVE — devices "
                   "and gateways already on the old network will lose contact "
                   "until re-provisioned)")
@click.pass_context
def network_init(ctx, channel, name, force):
    """Generate the hub's OT network identity now (rather than waiting
    for the first gateway provisioning)."""
    from . import network as net_mod
    db = _db(ctx)
    existing = net_mod.get_network(db)
    if existing is not None and not force:
        click.echo("A network already exists — use --force to replace it.",
                   err=True)
        ctx.exit(1)
    kwargs = {}
    if channel is not None:
        kwargs["channel"] = channel
    if name is not None:
        kwargs["name"] = name
    if existing and force:
        net = net_mod.reset_network(db, **kwargs)
    else:
        net = net_mod.get_or_create_network(db, **kwargs)
    click.echo(f"Network initialised: name={net.name!r} "
               f"channel={net.channel} panid=0x{net.panid:04x}")


import time   # used by network_show


@cli.group()
def device():
    """Manage registered devices."""


@device.command("list")
@click.option("--type", "filter_type",
              type=click.Choice(["end_device", "gateway"]), default=None)
@click.pass_context
def device_list(ctx, filter_type):
    """List all registered devices."""
    devices = _db(ctx).list_devices(type=filter_type)
    if not devices:
        click.echo("No devices registered.")
        return
    click.echo(f"{'ID':<20}  {'NAME':<20}  {'TYPE':<12}  LAST SEEN")
    click.echo("─" * 72)
    for d in devices:
        if d.last_seen:
            ago = int(time.time()) - d.last_seen
            last = f"{ago}s ago"
        else:
            last = "never"
        click.echo(f"{d.id:<20}  {d.name:<20}  {d.type:<12}  {last}")


@device.command("register")
@click.argument("id")
@click.argument("name")
@click.argument("type", metavar="TYPE", type=click.Choice(["end_device", "gateway"]))
@click.argument("pubkey_b64")
@click.pass_context
def device_register(ctx, id, name, type, pubkey_b64):
    """Register a device with its Ed25519 public key (base64)."""
    d = _db(ctx).register_device(id, name, type, pubkey_b64)
    click.echo(f"Registered {d.type} '{d.name}'  (id={d.id})")


@device.command("info")
@click.argument("id")
@click.pass_context
def device_info(ctx, id):
    """Show device details and current config version."""
    d = _db(ctx).get_device(id)
    if not d:
        click.echo(f"Device '{id}' not found.", err=True)
        sys.exit(1)
    cfg = _db(ctx).get_config(id)
    click.echo(f"ID         : {d.id}")
    click.echo(f"Name       : {d.name}")
    click.echo(f"Type       : {d.type}")
    click.echo(f"Public key : {d.pubkey_b64}")
    click.echo(f"Registered : {time.ctime(d.registered_at)}")
    click.echo(f"Last seen  : {time.ctime(d.last_seen) if d.last_seen else 'never'}")
    click.echo(f"Config ver : {cfg.version if cfg else 'none'}")


# ── device read / write (Phase 3: hub-initiated CoAP /field) ─────────────────


def _resolve_device(db: 'DB', name_or_id: str):
    """Look up a device by name first, then by id."""
    d = db.get_device_by_name(name_or_id)
    if not d:
        d = db.get_device(name_or_id)
    return d


def _api_base(ctx) -> str:
    """REST API base URL.  Honours --api-base on the parent group;
    defaults to localhost on the standard port."""
    return ctx.obj.get("api_base", "http://127.0.0.1:8769/api/v1")


def _api_request(ctx, method: str, path: str,
                 json_body: Optional[dict] = None,
                 timeout: float = 30.0) -> tuple[int, dict | str]:
    """Tiny synchronous HTTP wrapper around urllib so the CLI works
    without aiohttp's event loop.  Returns (status_code, parsed_body)."""
    import urllib.request
    import urllib.error
    base = _api_base(ctx)
    url = f"{base}{path}"
    data = None
    headers = {"Accept": "application/json"}
    if json_body is not None:
        data = json.dumps(json_body).encode()
        headers["Content-Type"] = "application/json"
    token = ctx.obj.get("api_token")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, method=method,
                                 headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode("utf-8", errors="replace")
            try:
                return r.status, json.loads(body)
            except json.JSONDecodeError:
                return r.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(body)
        except json.JSONDecodeError:
            return e.code, body
    except urllib.error.URLError as e:
        raise click.UsageError(
            f"can't reach hub API at {base} — is `nn-hub serve` running? "
            f"({e.reason})"
        )


@device.command("read")
@click.argument("name_or_id")
@click.argument("field")
@click.pass_context
def device_read(ctx, name_or_id, field):
    """Read a field's current value via the gateway-relay API."""
    code, body = _api_request(ctx, "GET",
                              f"/devices/{name_or_id}/field/{field}")
    if code != 200:
        msg = body.get("err") if isinstance(body, dict) else str(body)
        click.echo(f"read failed (HTTP {code}): {msg}", err=True)
        sys.exit(1)
    val = body.get("value") if isinstance(body, dict) else None
    name = body.get("name", field) if isinstance(body, dict) else field
    if val is None:
        click.echo(f"{name} = (unset)")
    else:
        click.echo(f"{name} = {val}")


@device.command("write")
@click.argument("name_or_id")
@click.argument("field")
@click.argument("value", type=float)
@click.pass_context
def device_write(ctx, name_or_id, field, value):
    """Write VALUE to an actuator-typed FIELD via the gateway-relay API."""
    code, body = _api_request(ctx, "PUT",
                              f"/devices/{name_or_id}/field/{field}",
                              json_body={"value": value})
    if code != 200:
        msg = body.get("err") if isinstance(body, dict) else str(body)
        click.echo(f"write failed (HTTP {code}): {msg}", err=True)
        sys.exit(1)
    name = body.get("name", field) if isinstance(body, dict) else field
    val  = body.get("value", value) if isinstance(body, dict) else value
    click.echo(f"{name} = {val}")


# ── device new ────────────────────────────────────────────────────────────────

@device.command("new")
@click.option("--name",        required=True, help="Device name (provisioned via BLE)")
@click.option("--type", "device_type", required=True,
              help="Device type: sample_c6 | esp_tbr | ...")
@click.option("--dataset-hex", default=None,
              help="OT operational dataset hex  (default: use the hub's "
                   "active network — generated automatically on first run)")
@click.option("--addr",        default=None,
              help="BLE MAC address (skip scan, connect directly)")
@click.option("--scan-time",   default=12.0, show_default=True,
              help="BLE scan time in seconds")
@click.option("--gateway",     default="", show_default=False,
              help="Gateway device_id to associate this device with")
@click.option("--ml-eid",      default=None,
              help="Device Thread ML-EID (skip mDNS lookup)")
@click.option("--skip-coap",   is_flag=True,
              help="Don't probe device after provisioning")
@click.option("--ble-adapter", default=None,
              help="Local BLE adapter to use, e.g. hci1 (default: the "
                   "NN_BLE_ADAPTER hub setting, else BlueZ's first). "
                   "See `nn-hub ble adapters`.")
@click.pass_context
def device_new(ctx, name, device_type, dataset_hex, addr, scan_time,
               gateway, ml_eid, skip_coap, ble_adapter):
    """Provision a new device via BLE and register it in the hub.

    \b
    Flow:
      1. Pull the hub's active OT dataset (auto-generated on first run)
      2. BLE: scan → connect → exchange keys → provision OT dataset + hub config
      3. DB:  register device with keys + provisioning info
      4. CoAP: probe device with encrypted GET_INFO (unless --skip-coap)

    \b
    Example:
      nn-hub device new --name sensor-01 --type sample_c6
    """
    asyncio.run(_device_new(ctx, name, device_type, dataset_hex, addr,
                            scan_time, gateway, ml_eid, skip_coap,
                            ble_adapter))


async def _device_new(ctx, name, device_type, dataset_hex, ble_addr,
                      scan_time, gateway, ml_eid, skip_coap,
                      ble_adapter=None):
    from . import ble_provisioner as ble
    from . import coap_client as cc

    db       = _db(ctx)
    data_dir = _data_dir(ctx)
    enc_priv = load_or_generate_enc_key(data_dir)

    # ── pick OT dataset ────────────────────────────────────────────────
    # Default: pull from the hub's active network (auto-generated on
    # first gateway provisioning).  Explicit --dataset-hex still wins
    # for advanced/debug flows.
    from . import network as net_mod
    if dataset_hex:
        try:
            dataset_bytes = bytes.fromhex(
                dataset_hex.replace(" ", "").replace("\n", ""))
        except ValueError as e:
            raise click.BadParameter(
                f"Invalid dataset hex: {e}", param_hint="--dataset-hex")
        click.echo(f"  Dataset   : {len(dataset_bytes)} bytes (--dataset-hex)")
    else:
        net_row = net_mod.get_or_create_network(db)
        dataset_bytes = net_mod.get_dataset_tlvs(db)
        click.echo(f"  Network   : {net_row.name!r} "
                   f"(channel {net_row.channel}, "
                   f"panid 0x{net_row.panid:04x}, "
                   f"extpanid {net_row.extpanid_hex})")
        click.echo(f"  Dataset   : {len(dataset_bytes)} bytes "
                   f"(hub's active network)")
    click.echo(f"  Hub X25519: {x25519_pubkey_bytes(enc_priv).hex()[:16]}...")
    click.echo()

    hub_config = encode_hub_config(name, enc_priv)

    # ── BLE scan ───────────────────────────────────────────────────────
    if ble_addr:
        click.echo(f"[BLE] Connecting directly to {ble_addr} ...")
        devices = []
    else:
        click.echo(f"[BLE] Scanning for provisioning peripherals ({scan_time:.0f}s)...")
        devices = await ble.scan(scan_time=scan_time, adapter=ble_adapter)
        if not devices:
            click.echo("[BLE] No devices found — check BLE adapter and device firmware.")
            raise SystemExit(1)
        click.echo(f"[BLE] {len(devices)} device(s) found.")
        if len(devices) == 1:
            ble_addr = devices[0].address
        else:
            for i, d in enumerate(devices):
                click.echo(f"  [{i}] {d.address}  name={d.name!r}")
            idx = click.prompt("Select device index", type=int, default=0)
            ble_addr = devices[idx].address

    # ── BLE provision ──────────────────────────────────────────────────
    click.echo(f"\n[BLE] Provisioning {ble_addr} ...")
    try:
        result = await ble.provision(
            addr=ble_addr,
            dataset_bytes=dataset_bytes,
            hub_config_bytes=hub_config,
            hub_x25519_priv=enc_priv,
            adapter=ble_adapter,
        )
    except RuntimeError as e:
        click.echo(f"[BLE] FAILED: {e}", err=True)
        raise SystemExit(1)

    click.echo(f"[BLE] Provisioning complete ✓")
    click.echo(f"  has_hub_chars   : {result.has_hub_chars}")
    click.echo(f"  device X25519   : {result.device_x25519_pub.hex()[:16]}...")

    # ── Derive device_id from X25519 pubkey ───────────────────────────
    device_id = result.device_x25519_pub.hex()[:16]

    # ── Register in DB ─────────────────────────────────────────────────
    enc_pub_b64  = base64.b64encode(result.device_x25519_pub).decode()
    mdns_addr    = f"{name}.local"

    db.register_device(device_id, name, "end_device", enc_pub_b64)
    db.set_provision_info(
        device_id=device_id,
        device_type=device_type,
        enc_pubkey_b64=enc_pub_b64,
        mdns_addr=mdns_addr,
        ml_eid=ml_eid or "",
        gateway_id=gateway,
        ble_addr=result.ble_addr,
    )
    db.log_event(device_id, "device_provisioned",
                 f"type={device_type}  ble={result.ble_addr}")

    click.echo(f"\n[DB] Device registered:")
    click.echo(f"  id        : {device_id}")
    click.echo(f"  name      : {name}")
    click.echo(f"  type      : {device_type}")
    click.echo(f"  mDNS      : {mdns_addr}")
    click.echo(f"  gateway   : {gateway or '(none)'}")

    # Post-provision INFO_QUERY is now an API-side concern.
    # `nn-hub serve` exposes POST /api/v1/devices/{id}/info-query which
    # round-trips through proto_router.request_h2d (replaces the legacy
    # CoAP-via-NAT64 `coap://[device]/info` probe).  We don't gate
    # registration on it; the BLE provisioning already established the
    # device's identity.
    if skip_coap:
        click.echo("\n[Probe] Skipping post-provision INFO_QUERY (--skip-coap).")
    else:
        click.echo("\n[Probe] Run `nn-hub device info-query "
                   f"{device_id}` once `nn-hub serve` is running and the "
                   "device has joined Thread.")

    click.echo(f"\nDone. Device '{name}' (id={device_id}) provisioned and registered.")


# ── gateway ───────────────────────────────────────────────────────────────────
#
# Gateways speak the nn_proto wire format with the hub over TCP.  Each
# gateway has a P-256 (secp256r1) keypair generated on-device and
# registered here (see `gw id` shell command on the gateway firmware).

import base64 as _b64


def _normalize_p256_pubkey(s: str) -> str:
    """Accept hex (130 chars) or base64; return canonical base64.

    On-device `gw id` prints the P-256 public key as hex (uncompressed,
    65 bytes → 130 hex chars starting with '04').  We store base64 for
    consistency with the rest of the schema.
    """
    s = s.strip()
    raw: bytes
    if len(s) == 130 and all(c in "0123456789abcdefABCDEF" for c in s):
        raw = bytes.fromhex(s)
    else:
        # base64 (with or without padding)
        try:
            raw = _b64.b64decode(s + "=" * (-len(s) % 4))
        except Exception as e:
            raise click.BadParameter(f"pubkey not hex(130) or base64: {e}")
    if len(raw) != 65 or raw[0] != 0x04:
        raise click.BadParameter(
            f"pubkey must be 65 bytes uncompressed (0x04 || X || Y), "
            f"got len={len(raw)} first={raw[:1].hex()}")
    return _b64.b64encode(raw).decode()


@cli.group()
def gateway():
    """Manage registered gateways (broker between hub and devices)."""


@gateway.command("new")
@click.option("--ssid",       required=True, help="WiFi SSID")
@click.option("--psk",        required=True, help="WiFi password")
@click.option("--hub-host",   required=True,
              help="Hub address the gateway connects to "
                   "(e.g. 'nn-hub.local' or '192.0.2.10')")
@click.option("--transport",  type=click.Choice(["ble", "net"]),
              default="ble",
              help="Transport for the provisioning handshake "
                   "(BLE GATT or TCP over LAN — both speak the same "
                   "ECIES wire protocol)")
@click.option("--addr",       default=None,
              help="BLE MAC (--transport ble) or host[:port] "
                   "(--transport net) — skip the scan when known")
@click.option("--scan-time",  default=10.0, type=float,
              help="Scan duration in seconds")
@click.option("--name",       default="",
              help="Human-readable label (local-only, not sent to gateway)")
@click.option("--mdns",       default="",
              help="mDNS hostname for the gateway (rare; usually empty)")
@click.option("--ble-adapter", default=None,
              help="Local BLE adapter to use, e.g. hci1 (default: the "
                   "NN_BLE_ADAPTER hub setting, else BlueZ's first). "
                   "See `nn-hub ble adapters`.")
@click.pass_context
def gateway_new(ctx, ssid, psk, hub_host, transport, addr, scan_time, name,
                mdns, ble_adapter):
    """Provision a fresh gateway and register it in the hub DB.

    Default transport is BLE GATT (gateway advertises service e7f01001-…
    until provisioned).  Pass --transport=net to provision over TCP
    instead — the gateway must be running `gw_linux provision-net`,
    publishing `_nn-gw._tcp.local` via avahi.  Same ECIES envelopes
    in either case.
    """
    import asyncio
    from . import network as net_mod

    # M6 unified: hub's X25519 priv is the ECIES sender key for the
    # encrypted writes below.  Same key the sensor provisioning uses.
    enc_priv = load_or_generate_enc_key(_data_dir(ctx))

    db = _db(ctx)
    net = net_mod.get_or_create_network(db)
    ot_dataset_tlvs = net_mod.get_dataset_tlvs(db)
    click.echo(f"  Transport : {transport}")
    click.echo(f"  Network   : {net.name!r} channel={net.channel} "
               f"panid=0x{net.panid:04x} extpanid={net.extpanid_hex}")
    click.echo(f"  Dataset   : {len(ot_dataset_tlvs)} bytes "
               f"(hub-active OT network)")
    click.echo()

    try:
        if transport == "ble":
            from .ble_gateway_provisioner import (
                provision_gateway, BleGatewayProvisionError as PErr)
            result = asyncio.run(provision_gateway(
                adapter=ble_adapter,
                ssid=ssid, psk=psk, hub_host=hub_host,
                hub_x25519_priv=enc_priv,
                ot_dataset_tlvs=ot_dataset_tlvs,
                address=addr, scan_time=scan_time))
        else:
            from .net_gateway_provisioner import (
                provision_gateway_net, ProvisionError as PErr)
            result = asyncio.run(provision_gateway_net(
                ssid=ssid, psk=psk, hub_host=hub_host,
                hub_x25519_priv=enc_priv,
                ot_dataset_tlvs=ot_dataset_tlvs,
                address=addr, scan_time=scan_time))
    except Exception as e:
        click.echo(f"Provisioning failed: {e}", err=True)
        ctx.exit(1)

    gw = _db(ctx).register_gateway(
        result.gateway_id,
        name or result.gateway_id,
        result.pubkey_b64,
        mdns)
    click.echo(f"Provisioned gateway '{gw.name}' (id={gw.id})")
    addr_label = "ble_addr" if transport == "ble" else "net_addr"
    click.echo(f"  {addr_label:10}: {result.address}")
    click.echo(f"  pubkey_b64 : {gw.pubkey_b64}")
    click.echo("  Gateway will reboot, connect to WiFi, and register over TCP.")


@gateway.command("register")
@click.option("--id",      "gw_id",     required=True,
              help="Gateway ID (8B hex, from device `gw id`)")
@click.option("--pubkey",  "pubkey",    required=True,
              help="P-256 public key (130 hex chars or base64)")
@click.option("--name",    default="",  help="Human-readable name")
@click.option("--mdns",    default="",  help="mDNS hostname (e.g. 'nn-br.local')")
@click.pass_context
def gateway_register(ctx, gw_id, pubkey, name, mdns):
    """Register a gateway in the hub DB.

    Run this once after first flashing a gateway; the gateway prints its
    `gateway_id` and `p256_pub` on boot.  Hub will only accept TCP
    connections from gateways listed here.
    """
    pubkey_b64 = _normalize_p256_pubkey(pubkey)
    gw = _db(ctx).register_gateway(gw_id, name or gw_id, pubkey_b64, mdns)
    click.echo(f"Registered gateway '{gw.name}' (id={gw.id})")
    click.echo(f"  pubkey_b64 : {gw.pubkey_b64}")
    if gw.mdns_addr:
        click.echo(f"  mdns       : {gw.mdns_addr}")


@gateway.command("list")
@click.pass_context
def gateway_list(ctx):
    """List all registered gateways."""
    gws = _db(ctx).list_gateways()
    if not gws:
        click.echo("No gateways registered.")
        return
    click.echo(f"{'ID':<20}  {'NAME':<20}  {'MDNS':<24}  LAST SEEN")
    click.echo("─" * 80)
    for g in gws:
        if g.last_seen:
            ago = int(time.time()) - g.last_seen
            last = f"{ago}s ago"
        else:
            last = "never"
        click.echo(f"{g.id:<20}  {g.name:<20}  {g.mdns_addr:<24}  {last}")


@gateway.command("info")
@click.argument("gw_id")
@click.pass_context
def gateway_info(ctx, gw_id):
    """Show gateway details."""
    from .db import role_name
    db = _db(ctx)
    g = db.get_gateway(gw_id)
    if not g:
        click.echo(f"Gateway '{gw_id}' not found.", err=True)
        sys.exit(1)
    click.echo(f"ID         : {g.id}")
    click.echo(f"Name       : {g.name}")
    click.echo(f"Pubkey b64 : {g.pubkey_b64}")
    click.echo(f"mDNS       : {g.mdns_addr or '—'}")
    click.echo(f"Registered : {time.ctime(g.registered_at)}")
    click.echo(f"Last seen  : {time.ctime(g.last_seen) if g.last_seen else 'never'}")

    # Thread state (set by D2G GATEWAY_THREAD_STATE).  May be empty if
    # the gateway hasn't reported in yet — e.g. fresh registration.
    if g.last_thread_state_at:
        mleid = g.mleid_hex
        if len(mleid) == 32:
            # Pretty-print 16 hex bytes as compressed-ish 8 groups of 4.
            mleid = ":".join(mleid[i:i+4] for i in range(0, 32, 4))
        click.echo("")
        click.echo("Thread state:")
        click.echo(f"  Role     : {role_name(g.role)} ({g.role})")
        click.echo(f"  RLOC16   : 0x{g.rloc16:04x}")
        click.echo(f"  MLEID    : {mleid}")
        click.echo(f"  Reported : {time.ctime(g.last_thread_state_at)}")
    else:
        click.echo("")
        click.echo("Thread state: (not yet reported)")

    # Devices serviced by this gateway: provision_info.gateway_id == g.id.
    serviced = db.list_devices_for_gateway(g.id)
    click.echo("")
    click.echo(f"Devices serviced ({len(serviced)}):")
    if not serviced:
        click.echo("  (none)")
    for d in serviced:
        last = ("never" if not d.last_seen
                else f"{int(time.time()) - d.last_seen}s ago")
        click.echo(f"  {d.id}  {d.name:20s}  type={d.type:12s}  "
                   f"last_seen={last}")


# ── nn_proto h2d send (Phase 3 demo) ─────────────────────────────────────────
#
# Issued out-of-band against a running `nn-hub serve` via a small UNIX
# socket control channel — see ProtoServer.  For Phase 3.E we drive
# this directly from the serve process by writing a control file the
# server polls.

@cli.command("h2d")
@click.argument("device_id")
@click.argument("payload_hex")
@click.pass_context
def h2d_send(ctx, device_id, payload_hex):
    """Send an H2D frame to a device via its registered gateway.

    The hub server must be running.  This command writes a request file
    to the data dir; the server picks it up and forwards via TCP.
    """
    import json
    data_dir = _data_dir(ctx)
    req_dir = data_dir / "h2d_queue"
    req_dir.mkdir(parents=True, exist_ok=True)
    req_path = req_dir / f"{int(time.time() * 1000)}.json"
    req_path.write_text(json.dumps({
        "device_id": device_id,
        "payload_hex": payload_hex,
    }))
    click.echo(f"queued H2D request at {req_path}")


# ── config ────────────────────────────────────────────────────────────────────

@cli.group()
def config():
    """Manage per-device configs."""


@config.command("get")
@click.argument("device_id")
@click.pass_context
def config_get(ctx, device_id):
    """Print current config for a device."""
    cfg = _db(ctx).get_config(device_id)
    if not cfg:
        click.echo(f"No config stored for '{device_id}'.")
        return
    click.echo(f"# version {cfg.version}  updated {time.ctime(cfg.updated_at)}")
    click.echo(json.dumps(cfg.payload, indent=2))


@config.command("set")
@click.argument("device_id")
@click.argument("json_file", type=click.Path(exists=True))
@click.pass_context
def config_set(ctx, device_id, json_file):
    """Set device config from a JSON file.  Bumps version automatically."""
    payload = json.loads(Path(json_file).read_text())
    cfg = _db(ctx).set_config(device_id, payload)
    click.echo(f"Config updated: device={device_id}  version={cfg.version}")


# ── firmware ──────────────────────────────────────────────────────────────────

@cli.group()
def firmware():
    """Manage firmware images and per-device-type version targets."""


@firmware.command("upload")
@click.argument("device_type")
@click.argument("version")
@click.argument("firmware_file", type=click.Path(exists=True))
@click.pass_context
def firmware_upload(ctx, device_type, version, firmware_file):
    """Upload a firmware binary and set it as the target for a device type.

    Any device of this type that isn't already running VERSION will be
    told to download this image next time it queries the hub.
    """
    src  = Path(firmware_file)
    data = src.read_bytes()
    sha  = hashlib.sha256(data).hexdigest()
    dest = _data_dir(ctx) / "firmware" / f"{device_type}_{version}.bin"
    dest.write_bytes(data)

    _db(ctx).set_firmware_target(device_type, version, str(dest), len(data), sha)
    click.echo(f"Uploaded: {device_type}  v{version}")
    click.echo(f"  Size  : {len(data):,} bytes")
    click.echo(f"  SHA256: {sha}")
    click.echo(f"  Path  : {dest}")


@firmware.command("list")
@click.pass_context
def firmware_list(ctx):
    """List current firmware targets."""
    targets = _db(ctx).list_firmware_targets()
    if not targets:
        click.echo("No firmware targets set.")
        return
    click.echo(f"{'DEVICE TYPE':<28}  {'VERSION':<16}  {'SIZE':>10}  SHA256[:16]")
    click.echo("─" * 76)
    for t in targets:
        click.echo(
            f"{t.device_type:<28}  {t.target_version:<16}  "
            f"{t.size_bytes:>10,}  {t.sha256[:16]}"
        )


# ── firmware source / catalog (sync layer) ───────────────────────────────


@firmware.group("source")
def firmware_source():
    """Manage configured image sources (local dir, GitHub releases, ...)."""


@firmware_source.command("list")
@click.pass_context
def firmware_source_list(ctx):
    """List configured sources by reading sources.yaml."""
    p = _data_dir(ctx) / "sources.yaml"
    if not p.is_file():
        click.echo(f"No {p} — add one with `firmware source add` (or edit it directly).")
        return
    import yaml as _yaml
    raw = _yaml.safe_load(p.read_text()) or {}
    sources = raw.get("sources") or []
    if not sources:
        click.echo(f"{p}: zero sources configured.")
        return
    for s in sources:
        click.echo(f"  {s.get('name','?'):<24}  kind={s.get('kind','?'):<8}  "
                   f"poll={s.get('poll_seconds',0)}s")


@firmware_source.command("add")
@click.argument("name")
@click.option("--kind",         required=True,
              type=click.Choice(["local", "github"]))
@click.option("--root",         help="Filesystem root (kind=local)")
@click.option("--repo",         help="GitHub owner/name (kind=github)")
@click.option("--pat-env",      help="Env var holding a GitHub PAT (kind=github)")
@click.option("--poll-seconds", default=0, show_default=True, type=int)
@click.option("--channels",     help="Comma-separated channel filter (kind=github)")
@click.pass_context
def firmware_source_add(ctx, name, kind, root, repo, pat_env,
                         poll_seconds, channels):
    """Append a new source entry to ~/.nn-hub/sources.yaml."""
    import yaml as _yaml
    p = _data_dir(ctx) / "sources.yaml"
    raw = _yaml.safe_load(p.read_text()) if p.is_file() else {}
    raw = raw or {}
    raw.setdefault("sources", [])
    if any(s.get("name") == name for s in raw["sources"]):
        raise click.UsageError(f"source {name!r} already exists in {p}")
    entry = {"name": name, "kind": kind, "poll_seconds": poll_seconds}
    if kind == "local":
        if not root:
            raise click.UsageError("kind=local requires --root")
        entry["root"] = root
    elif kind == "github":
        if not repo:
            raise click.UsageError("kind=github requires --repo")
        entry["repo"] = repo
        if pat_env:
            entry["pat_env"] = pat_env
        if channels:
            entry["channels"] = [c.strip() for c in channels.split(",") if c.strip()]
    raw["sources"].append(entry)
    p.write_text(_yaml.safe_dump(raw, sort_keys=False))
    click.echo(f"Added source {name!r} to {p}.")
    click.echo("Restart the hub or POST /api/v1/firmware/sources/{name}/sync to pick it up.")


@firmware_source.command("sync")
@click.argument("name")
@click.pass_context
def firmware_source_sync(ctx, name):
    """Force a discovery sync on one source (via REST)."""
    import requests
    base  = ctx.obj["api_base"].rstrip("/")
    token = ctx.obj.get("api_token")
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    r = requests.post(f"{base}/firmware/sources/{name}/sync",
                      headers=headers, timeout=30)
    if r.status_code != 200:
        click.echo(f"sync failed: {r.status_code} {r.text}", err=True)
        ctx.exit(1)
    click.echo(r.json())


@firmware.command("catalog")
@click.option("--device-type", default=None,
              help="Filter to one device type")
@click.pass_context
def firmware_catalog_cmd(ctx, device_type):
    """List discovered images in the catalog (across all sources)."""
    rows = _db(ctx).list_catalog(device_type)
    if not rows:
        click.echo("Catalog is empty — run `firmware source sync <name>` first.")
        return
    click.echo(f"{'DEVICE TYPE':<24}  {'VERSION':<14}  "
               f"{'SOURCE':<20}  {'CACHED':<7}  SHA256[:16]")
    click.echo("─" * 88)
    for r in rows:
        click.echo(
            f"{r.device_type:<24}  {r.version:<14}  "
            f"{r.source_name:<20}  {('yes' if r.is_cached else 'no'):<7}  "
            f"{r.sha256[:16]}"
        )


@firmware.command("promote")
@click.argument("device_type")
@click.argument("version")
@click.option("--source", "source_name", default=None,
              help="Specific source to promote from (default: pick a cached one if available)")
@click.pass_context
def firmware_promote_cmd(ctx, device_type, version, source_name):
    """Promote a catalog entry to the active OTA target (via REST)."""
    import requests
    base  = ctx.obj["api_base"].rstrip("/")
    token = ctx.obj.get("api_token")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    body = {}
    if source_name:
        body["source"] = source_name
    r = requests.post(
        f"{base}/firmware/catalog/{device_type}/{version}/promote",
        headers=headers, json=body, timeout=60,
    )
    if r.status_code != 200:
        click.echo(f"promote failed: {r.status_code} {r.text}", err=True)
        ctx.exit(1)
    out = r.json()
    click.echo(f"Promoted {out['device_type']} → v{out['version']}")
    click.echo(f"  source: {out['source']}")
    click.echo(f"  path  : {out['path']}")
    click.echo(f"  size  : {out['size_bytes']:,} bytes")
    click.echo(f"  sha256: {out['sha256']}")


# ── log (device log persistence — Phase 4) ──────────────────────────────────


def _parse_since(s: Optional[str]) -> Optional[int]:
    """Parse '30m' / '2h' / '1d' / '<unix_ts>' into a unix-ts cutoff.
    None or empty returns None."""
    if not s:
        return None
    s = s.strip().lower()
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if s and s[-1] in multipliers:
        try:
            n = int(s[:-1])
        except ValueError:
            raise click.UsageError(f"bad --since: {s!r}")
        return int(time.time()) - n * multipliers[s[-1]]
    try:
        return int(s)
    except ValueError:
        raise click.UsageError(
            f"--since: expected like '30m', '2h', '1d', or a unix-ts (got {s!r})"
        )


def _open_log_store(ctx, *, rotate_min: int = 60):
    from .log_store import LogStore
    return LogStore(_data_dir(ctx) / "logs",
                    rotate_interval_sec=rotate_min * 60)


@cli.group()
def log():
    """Inspect persisted device logs (collected by `nn-hub serve`)."""


@log.command("show")
@click.option("--device", "-d", default=None,
              help="Filter by device name or id (default: all)")
@click.option("--level", "-l", default=None,
              help="Filter by level (inf | wrn | err | dbg)")
@click.option("--tag", "-t", default=None,
              help="Filter by module tag")
@click.option("--since", "-s", default=None,
              help="Only entries newer than (e.g. 30m, 2h, 1d, <unix-ts>)")
@click.option("--limit", "-n", default=50, show_default=True,
              help="Max number of entries to show (newest first)")
@click.pass_context
def log_show(ctx, device, level, tag, since, limit):
    """Show recent device log entries from the rotating SQLite store."""
    db = _db(ctx)
    device_id = None
    if device:
        d = _resolve_device(db, device)
        if d:
            device_id = d.id
        else:
            # Pass through unchanged so callers can also query by raw id.
            device_id = device

    since_ts = _parse_since(since)
    store = _open_log_store(ctx)
    rows = store.query(device_id=device_id, level=level, tag=tag,
                       since_ts=since_ts, limit=limit)
    if not rows:
        click.echo("(no matching log entries)")
        return
    # name lookup once per device for compact display
    id_to_name: dict[str, str] = {d.id: d.name for d in db.list_devices()}
    for r in rows:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["upload_ts"]))
        did = r["device_id"]
        nm = id_to_name.get(did, did[:16])
        line = (f"{ts}  {nm:18s}  <{r['level'] or '   ':3s}> "
                f"{r['tag']:14s}  {r['content']}")
        click.echo(line)


@log.command("files")
@click.pass_context
def log_files(ctx):
    """List the rotated log SQLite files (oldest first)."""
    store = _open_log_store(ctx)
    files = store.list_files()
    if not files:
        click.echo("(no log files yet)")
        return
    click.echo(f"{'FILE':40s}  {'SIZE':>10s}  ROWS")
    for p in files:
        sz = p.stat().st_size
        try:
            import sqlite3
            c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
            n = c.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            c.close()
        except Exception:
            n = "?"
        click.echo(f"{p.name:40s}  {sz:>10,}  {n}")


# ── events ────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--limit", default=50, show_default=True)
@click.pass_context
def events(ctx, limit):
    """Show recent hub event log."""
    evts = _db(ctx).recent_events(limit)
    if not evts:
        click.echo("No events.")
        return
    for e in evts:
        ts  = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(e["ts"]))
        dev = e["device_id"] or "—"
        click.echo(f"{ts}  {e['type']:<28}  {dev:<20}  {e['detail']}")


# ── telemetry ──────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("device_id", required=False, default=None)
@click.option("--limit", default=20, show_default=True,
              help="Number of rows to show")
@click.option("--field", "fields", multiple=True,
              help="Only show these data fields (repeatable)")
@click.pass_context
def telemetry(ctx, device_id, limit, fields):
    """Show recent telemetry.  Optionally filter by device and/or field names."""
    rows = _db(ctx).recent_telemetry(device_id, limit)
    if not rows:
        click.echo("No telemetry.")
        return

    click.echo(f"{'TIMESTAMP':<22}  {'DEVICE':<20}  DATA")
    click.echo("─" * 80)
    for r in rows:
        ts  = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["ts"]))
        dev = r["device_id"]
        data = r["payload"]
        data.pop("ts", None)  # already shown in timestamp column

        if fields:
            data = {k: v for k, v in data.items() if k in fields}

        click.echo(f"{ts}  {dev:<20}  {json.dumps(data)}")


# ── auto (automation) ─────────────────────────────────────────────────────────

@cli.group()
def auto():
    """Manage automations (compile YAML → per-device configs)."""


@auto.command("compile")
@click.option("--file", "yaml_file", default=None,
              help="Path to automations.yaml  [default: <data-dir>/automations.yaml]")
@click.pass_context
def auto_compile(ctx, yaml_file):
    """Compile automations.yaml into per-device config payloads."""
    data_dir = _data_dir(ctx)
    db = _db(ctx)

    path = Path(yaml_file) if yaml_file else data_dir / "automations.yaml"
    if not path.exists():
        click.echo(f"File not found: {path}", err=True)
        raise SystemExit(1)

    compiler = AutoCompiler(db)
    payloads = compiler.compile(path)

    click.echo(f"Compiled {len(payloads)} device configs from {path.name}:")
    for dev_name, payload in payloads.items():
        n_trg  = len(payload.get("triggers", []))
        n_cond = len(payload.get("conditions", []))
        n_act  = len(payload.get("actions", []))
        size   = len(json.dumps(payload))
        click.echo(f"  {dev_name:<20}  "
                   f"triggers={n_trg}  conditions={n_cond}  "
                   f"actions={n_act}  ({size} B)")


@auto.command("show")
@click.option("--device", default=None, help="Show config for one device only")
@click.pass_context
def auto_show(ctx, device):
    """Show compiled automation configs per device."""
    db = _db(ctx)

    if device:
        dev = db.get_device_by_name(device) or db.get_device(device)
        if not dev:
            click.echo(f"Device '{device}' not found", err=True)
            raise SystemExit(1)
        cfg = db.get_config(dev.id)
        if cfg:
            click.echo(json.dumps(cfg.payload, indent=2))
        else:
            click.echo("No config stored")
    else:
        devices = db.list_devices()
        for dev in devices:
            cfg = db.get_config(dev.id)
            if cfg and cfg.payload.get("v"):
                click.echo(f"─── {dev.name} ({dev.id[:8]}) "
                           f"v{cfg.version} ───")
                click.echo(json.dumps(cfg.payload, indent=2))
                click.echo()


# ── factory ───────────────────────────────────────────────────────────────────

@cli.group()
def factory():
    """Image factory — build, sign, and register firmware."""


@factory.command("start")
@click.option("--port", default=8100, show_default=True,
              help="HTTP port for factory service")
@click.option("--device-repo", envvar="NN_DEVICE_REPO", required=True,
              help="Path to the nn device repo (contains scripts/build.sh)")
@click.option("--host", default="127.0.0.1", show_default=True)
def factory_start(port, device_repo, host):
    """Start the factory build service (foreground)."""
    from .factory_service import run
    run(device_repo, host=host, port=port)


@factory.command("build")
@click.option("--app", required=True,
              help="App path relative to device repo (e.g. apps/mdns_ot_esp32c6)")
@click.option("--board", default="",
              help="Board target (auto-detected from .nn-build.yaml if omitted)")
@click.option("--version", "fw_version", required=True,
              help="Firmware version string")
@click.option("--factory-url", default="http://localhost:8100", show_default=True,
              help="Factory service URL")
@click.pass_context
def factory_build(ctx, app, board, fw_version, factory_url):
    """Build firmware via factory service and register it in the hub."""
    asyncio.run(_factory_build(ctx, app, board, fw_version, factory_url))


async def _factory_build(ctx, app, board, fw_version, factory_url):
    from .factory_client import FactoryClient

    client = FactoryClient(factory_url)

    if not await client.health():
        click.echo(f"Factory service not reachable at {factory_url}", err=True)
        click.echo("Start it with: nn-hub factory start --device-repo <path>", err=True)
        raise SystemExit(1)

    click.echo(f"Building {app} v{fw_version} ...")
    result = await client.build(app, board=board, version=fw_version)

    if result.get("status") != "ok":
        click.echo(f"Build failed: {result.get('message', 'unknown error')}", err=True)
        build_log = result.get("log", "")
        if build_log:
            click.echo("--- build log (last 1000 chars) ---")
            click.echo(build_log[-1000:])
        raise SystemExit(1)

    device_type = result["device_type"]
    size = result["size"]
    sha = result["sha256"]
    paths = result.get("paths", {})

    # Copy all variants to hub firmware directory
    fw_dir = _data_dir(ctx) / "firmware"
    fw_dir.mkdir(parents=True, exist_ok=True)

    # Default image (used by DB path and non-slot-aware requests)
    default_src = Path(result["path"])
    dest = fw_dir / f"{device_type}_{fw_version}.bin"
    shutil.copy2(default_src, dest)

    # Slot-specific variants for Direct XIP
    for slot_key in ("slot0", "slot1"):
        if slot_key in paths:
            slot_src = Path(paths[slot_key])
            slot_dest = fw_dir / f"{device_type}_{fw_version}_{slot_key}.bin"
            shutil.copy2(slot_src, slot_dest)

    # Register in hub DB (default path — CoAP server resolves slot variants)
    _db(ctx).set_firmware_target(device_type, fw_version, str(dest), size, sha)

    click.echo(f"Build OK: {device_type} v{fw_version}")
    click.echo(f"  Size  : {size:,} bytes")
    click.echo(f"  SHA256: {sha}")
    click.echo(f"  Path  : {dest}")
    if "slot0" in paths and "slot1" in paths:
        click.echo(f"  Slots : slot0 + slot1 (Direct XIP)")


@factory.command("status")
@click.option("--factory-url", default="http://localhost:8100", show_default=True)
def factory_status(factory_url):
    """Check factory service status."""
    asyncio.run(_factory_status(factory_url))


async def _factory_status(factory_url):
    from .factory_client import FactoryClient

    client = FactoryClient(factory_url)
    if not await client.health():
        click.echo(f"Factory service not reachable at {factory_url}")
        return

    status = await client.status()
    click.echo(f"Factory: {status.get('status', 'unknown')}")
    if status.get("app"):
        click.echo(f"  App  : {status['app']}")
        click.echo(f"  Board: {status.get('board', '?')}")


if __name__ == "__main__":
    cli()
