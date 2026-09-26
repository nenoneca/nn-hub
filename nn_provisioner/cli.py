"""nn-provisioner CLI — provision sensors and gateways against a remote hub.

The provisioner is a thin orchestrator:

  1. Reads the WiFi PSK + Thread dataset locally (from operator input
     + a `GET /api/v1/network` call to the hub).
  2. Reads the hub's identity from `--keys-dir` (P-256 + X25519
     private keys), or asks the hub to share its public identity via
     `GET /api/v1/hub/identity` for pin-verification.
  3. Performs the BLE GATT exchange directly with the device or
     gateway (no hub round-trip on the wire).
  4. POSTs the resulting pubkey back to the hub for registration.

This way the WiFi PSK never travels over REST — it goes BLE→device
only.  The hub can live anywhere reachable on HTTP.

Setup
-----
On the host that has the BLE radio (laptop, RPi, etc.):

    pip install nn-hub                  # provisioner depends on hub libs
    mkdir -p ~/.nn-hub
    scp hub:/var/lib/nn-hub/proto_p256_priv.bin ~/.nn-hub/
    scp hub:/var/lib/nn-hub/enc_key.bin         ~/.nn-hub/

Then::

    nn-provisioner device new \\
        --hub https://hub.example.com:8769/api/v1 \\
        --name sensor-01 --type sample_c6

    nn-provisioner gateway new \\
        --hub https://hub.example.com:8769/api/v1 \\
        --ssid MyWiFi --psk secret \\
        --hub-host hub.example.com --name gw-01
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import sys
from pathlib import Path
from typing import Optional

import click

from .hub_client import HubClient, HubClientError, HubIdentity


# ── helpers ─────────────────────────────────────────────────────────────────

DEFAULT_KEYS_DIR = Path.home() / ".nn-hub"
DEFAULT_HUB_URL  = "http://127.0.0.1:8769/api/v1"


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )


def _verify_hub_identity_match(keys_dir: Path, remote: HubIdentity) -> None:
    """Sanity-check that the hub URL we're pointed at owns the same
    keypair we have locally.  Catches a foot-gun where the operator
    copied keys from one hub but is talking to a different one — which
    would otherwise produce a confusing "device can't decrypt H2D"
    failure at runtime."""
    from hub.ble_gateway_provisioner import load_hub_identity

    local_id, local_p256 = load_hub_identity(keys_dir / "proto_p256_priv.bin")
    if local_id != remote.hub_id:
        raise click.ClickException(
            f"hub identity mismatch: local hub_id={local_id.hex()} "
            f"but remote /hub/identity reports hub_id={remote.hub_id.hex()}. "
            f"Are you talking to the right hub?")
    if local_p256 != remote.p256_pub:
        raise click.ClickException(
            "hub identity mismatch: P-256 pub differs between local "
            "keys and remote /hub/identity")


def _load_enc_priv(keys_dir: Path):
    """Load the hub's X25519 private key from keys_dir."""
    from hub.crypto import load_or_generate_enc_key
    if not (keys_dir / "enc_key.bin").exists():
        raise click.ClickException(
            f"missing {keys_dir / 'enc_key.bin'} — copy it from the hub "
            f"host before running the provisioner")
    return load_or_generate_enc_key(keys_dir)


# ── CLI root ────────────────────────────────────────────────────────────────

@click.group()
@click.option("--hub", "hub_url", default=DEFAULT_HUB_URL, show_default=True,
              envvar="NN_HUB_URL",
              help="Base REST URL of the hub.")
@click.option("--keys-dir", default=str(DEFAULT_KEYS_DIR), show_default=True,
              type=click.Path(path_type=Path),
              help="Directory holding the hub's P-256 + X25519 private keys.")
@click.option("--token", default=None, envvar="NN_HUB_API_TOKEN",
              help="Bearer token for the hub REST API (if configured).")
@click.option("-v", "--verbose", is_flag=True)
@click.pass_context
def cli(ctx, hub_url, keys_dir, token, verbose):
    """nn-provisioner — onboard sensors and gateways against a remote hub."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["hub_url"]  = hub_url
    ctx.obj["keys_dir"] = keys_dir
    ctx.obj["client"]   = HubClient(hub_url, token=token)


# ── subgroup: device ────────────────────────────────────────────────────────

@cli.group()
def device():
    """Sensor onboarding."""


@device.command("new")
@click.option("--name", required=True, help="Device name (label)")
@click.option("--type", "device_type", required=True,
              help="Device type, e.g. sample_c6")
@click.option("--addr", default=None,
              help="BLE MAC address (skips scan)")
@click.option("--scan-time", default=12.0, show_default=True, type=float)
@click.option("--gateway", default="",
              help="Gateway device_id to associate this device with")
@click.pass_context
def device_new(ctx, name, device_type, addr, scan_time, gateway):
    """Provision a fresh sensor over BLE and register it on the hub.

    Flow:

    \b
      1. Verify hub identity (keys_dir matches hub_url's /hub/identity)
      2. Fetch the active Thread dataset via GET /network
      3. BLE: scan/connect → exchange keys → write OT dataset + hub config
      4. POST /devices to register the device pubkey on the hub
    """
    asyncio.run(_device_new(ctx, name, device_type, addr, scan_time, gateway))


async def _device_new(ctx, name, device_type, ble_addr, scan_time, gateway):
    from hub import ble_provisioner as ble
    from hub.crypto import encode_hub_config, x25519_pubkey_bytes

    client: HubClient = ctx.obj["client"]
    keys_dir: Path    = ctx.obj["keys_dir"]

    click.echo(f"hub URL    : {ctx.obj['hub_url']}")
    click.echo(f"keys-dir   : {keys_dir}")

    # ── identity check ────────────────────────────────────────────
    try:
        remote = client.get_identity()
    except HubClientError as e:
        raise click.ClickException(f"hub identity probe: {e}")
    _verify_hub_identity_match(keys_dir, remote)
    enc_priv = _load_enc_priv(keys_dir)
    click.echo(f"hub_id     : {remote.hub_id.hex()}  ✓ identity pinned")

    # ── dataset ───────────────────────────────────────────────────
    net = client.get_network()
    if not net.get("configured"):
        raise click.ClickException(
            "hub has no Thread network yet — provision a gateway first or "
            "call POST /api/v1/network/init")
    dataset_bytes = bytes.fromhex(net["dataset_tlvs_hex"])
    click.echo(f"network    : {net['name']} ({len(dataset_bytes)} B dataset)")
    click.echo(f"hub X25519 : {x25519_pubkey_bytes(enc_priv).hex()[:16]}...")
    click.echo()

    hub_config = encode_hub_config(name, enc_priv)

    # ── BLE scan/connect ──────────────────────────────────────────
    if not ble_addr:
        click.echo(f"[BLE] scanning for provisioning peripherals "
                   f"({scan_time:.0f}s)...")
        devices = await ble.scan(scan_time=scan_time)
        if not devices:
            raise click.ClickException("no advertising devices found")
        if len(devices) == 1:
            ble_addr = devices[0].address
            click.echo(f"[BLE] one match: {ble_addr}")
        else:
            for i, d in enumerate(devices):
                click.echo(f"  [{i}] {d.address}  name={d.name!r}")
            idx = click.prompt("select device", type=int, default=0)
            ble_addr = devices[idx].address

    # ── BLE provision ─────────────────────────────────────────────
    click.echo(f"[BLE] provisioning {ble_addr} ...")
    result = await ble.provision(
        addr=ble_addr,
        dataset_bytes=dataset_bytes,
        hub_config_bytes=hub_config,
        hub_x25519_priv=enc_priv,
    )

    device_id   = result.device_x25519_pub.hex()[:16]
    enc_pub_b64 = base64.b64encode(result.device_x25519_pub).decode()
    sign_pub_b64 = base64.b64encode(result.device_ed25519_pub).decode()
    click.echo(f"  device_id    : {device_id}")
    click.echo(f"  X25519 pub   : {enc_pub_b64}")
    click.echo(f"  Ed25519 pub  : {sign_pub_b64}")

    # ── hub registration ──────────────────────────────────────────
    try:
        reg = client.register_device(device_id, name, device_type, sign_pub_b64)
    except HubClientError as e:
        raise click.ClickException(f"hub register: {e}")
    click.echo(f"\n✓ {name} registered on hub")
    click.echo(f"  hub said: {reg}")


# ── subgroup: gateway ───────────────────────────────────────────────────────

@cli.group()
def gateway():
    """Gateway onboarding."""


@gateway.command("new")
@click.option("--ssid",     required=True, help="WiFi SSID for the gateway")
@click.option("--psk",      required=True, help="WiFi PSK for the gateway")
@click.option("--hub-host", required=True,
              help="Hostname/IP the gateway will use to reach the hub")
@click.option("--name", default=None, help="Human-readable gateway label")
@click.option("--addr", default=None, help="BLE MAC (skip scan)")
@click.option("--scan-time", default=10.0, show_default=True, type=float)
@click.option("--mdns",  default="",
              help="Optional mDNS hostname to advertise the gateway under")
@click.pass_context
def gateway_new(ctx, ssid, psk, hub_host, name, addr, scan_time, mdns):
    """Provision a fresh gateway over BLE and register it on the hub."""
    asyncio.run(_gateway_new(ctx, ssid, psk, hub_host, name, addr,
                             scan_time, mdns))


async def _gateway_new(ctx, ssid, psk, hub_host, name, ble_addr,
                       scan_time, mdns):
    from hub.ble_gateway_provisioner import provision_gateway

    client: HubClient = ctx.obj["client"]
    keys_dir: Path    = ctx.obj["keys_dir"]

    click.echo(f"hub URL    : {ctx.obj['hub_url']}")
    click.echo(f"keys-dir   : {keys_dir}")
    click.echo(f"hub_host   : {hub_host}")
    click.echo(f"ssid       : {ssid!r}  (psk hidden)")
    click.echo()

    # ── identity check ────────────────────────────────────────────
    try:
        remote = client.get_identity()
    except HubClientError as e:
        raise click.ClickException(f"hub identity probe: {e}")
    _verify_hub_identity_match(keys_dir, remote)
    enc_priv = _load_enc_priv(keys_dir)

    # ── Thread dataset (must already exist on the hub) ────────────
    net = client.get_network()
    if not net.get("configured"):
        click.echo("hub has no network yet — initialising ...")
        try:
            client.network_init()
        except HubClientError as e:
            raise click.ClickException(f"network init: {e}")
        net = client.get_network()
    dataset = bytes.fromhex(net["dataset_tlvs_hex"])

    # ── BLE provisioning ──────────────────────────────────────────
    click.echo("[BLE] provisioning gateway ...")
    result = await provision_gateway(
        ssid=ssid, psk=psk, hub_host=hub_host,
        hub_x25519_priv=enc_priv,
        ot_dataset_tlvs=dataset,
        address=ble_addr,
        scan_time=scan_time,
        privkey_path=keys_dir / "proto_p256_priv.bin",
    )
    click.echo(f"  gateway_id : {result.gateway_id}")
    click.echo(f"  pubkey_b64 : {result.pubkey_b64}")

    # ── register on hub ───────────────────────────────────────────
    try:
        reg = client.register_gateway(result.gateway_id,
                                      name or result.gateway_id,
                                      result.pubkey_b64,
                                      mdns_addr=mdns)
    except HubClientError as e:
        raise click.ClickException(f"hub register: {e}")
    click.echo(f"\n✓ gateway registered on hub: {reg}")


# ── subgroup: hub ───────────────────────────────────────────────────────────

@cli.command()
@click.pass_context
def identity(ctx):
    """Print the hub's public identity (sanity check before provisioning)."""
    client: HubClient = ctx.obj["client"]
    try:
        ident = client.get_identity()
    except HubClientError as e:
        raise click.ClickException(str(e))
    click.echo(f"hub_id     : {ident.hub_id.hex()}")
    click.echo(f"p256_pub   : {ident.p256_pub.hex()[:32]}...")
    click.echo(f"x25519_pub : {ident.x25519_pub.hex()}")
    # And reflect-verify against local keys if present
    keys_dir: Path = ctx.obj["keys_dir"]
    if (keys_dir / "proto_p256_priv.bin").exists():
        try:
            _verify_hub_identity_match(keys_dir, ident)
            click.echo("local keys : ✓ match remote identity")
        except click.ClickException as e:
            click.echo(f"local keys : ✗ {e.message}", err=True)
            sys.exit(1)
    else:
        click.echo("local keys : (none in keys_dir — provisioning will fail)")


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
