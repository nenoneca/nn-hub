# `nn-provisioner` — Standalone BLE Provisioning Tool

`nn-provisioner` is a CLI that onboards sensors and gateways against an
nn-hub running anywhere reachable over HTTP. The hub itself no longer
needs BLE — only the host running `nn-provisioner` does.

This decouples three things that used to be coupled:

| Concern | Lives on |
|---|---|
| Hub REST API + DB + crypto + auto engine | hub host (cloud, PC, RPi, ...) |
| BLE radio + WiFi PSK entry + physical proximity | provisioner host (laptop / nearby RPi) |
| Sensor / gateway hardware | wherever the device is deployed |

## When to use it

| Setup | Tool |
|---|---|
| Hub on the same host that has BLE | `nn-hub device new` (built-in CLI) |
| Hub remote (cloud, separate server) | `nn-provisioner device new` (this) |
| Multiple sites with one shared hub | One `nn-provisioner` per site |

The two CLIs accept similar flags. Internally they share the same BLE
provisioning code (`hub.ble_provisioner` / `hub.ble_gateway_provisioner`),
so behaviour is identical.

## Install

On the host that has BLE access:

```bash
pip install nn-hub        # the provisioner ships as part of this wheel
which nn-provisioner      # → installed by the same pyproject entry-point
```

The provisioner needs the **hub's private keys** to encrypt provisioning
blobs end-to-end (so the WiFi PSK and Thread dataset don't have to
round-trip through the hub):

```bash
mkdir -p ~/.nn-hub
scp hub-server:/var/lib/nn-hub/proto_p256_priv.bin ~/.nn-hub/
scp hub-server:/var/lib/nn-hub/enc_key.bin         ~/.nn-hub/
```

**Security note**: copying these keys gives the provisioner the ability
to impersonate the hub on its protocol layer. Treat the provisioner
host the same way you'd treat the hub host. A future version will give
each provisioner its own keypair that the device firmware trusts
explicitly, so the hub's private key never has to leave the hub.

## Verify hub identity (sanity check)

```bash
nn-provisioner --hub http://hub.example.com:8769/api/v1 identity
```

Output:

```
hub_id     : 6d4feb08293967b1
p256_pub   : 047a8f67763b6acaba74b089ab...
x25519_pub : a5505f64d5490a339e07725703...
local keys : ✓ match remote identity
```

`local keys : ✗ ...` means the keys in `~/.nn-hub/` don't match the hub
at the given URL — you'd be onboarding devices to the wrong hub.

## Provision a sensor

```bash
nn-provisioner \
    --hub http://hub.example.com:8769/api/v1 \
    device new \
    --name sensor-01 \
    --type sample_c6
```

Flow:

1. Sanity-check `hub_id` matches `~/.nn-hub/proto_p256_priv.bin`.
2. `GET /api/v1/network` to retrieve the active Thread dataset.
3. BLE scan for sensors advertising the provisioning service UUID.
4. Connect, exchange ECIES envelopes, write dataset + hub config.
5. `POST /api/v1/devices` with the freshly-provisioned device's pubkey.

## Provision a gateway

```bash
nn-provisioner \
    --hub http://hub.example.com:8769/api/v1 \
    gateway new \
    --ssid MyWiFi \
    --psk hunter2 \
    --hub-host hub.example.com \
    --name kitchen-gw
```

The WiFi PSK is entered on the provisioner host and BLE-written
directly to the gateway. It never travels to the hub over REST.

## Connecting to a hub on the local network

If the hub is on the same LAN, use mDNS:

```bash
nn-provisioner --hub http://nn-hub.local:8769/api/v1 identity
```

## REST endpoints the provisioner uses

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/hub/identity` | Pin hub identity before provisioning |
| `GET` | `/api/v1/network` | Fetch Thread dataset TLVs |
| `POST` | `/api/v1/network/init` | Mint a Thread network if none exists |
| `POST` | `/api/v1/devices` | Register a provisioned sensor's pubkey |
| `POST` | `/api/v1/gateways` | Register a provisioned gateway's pubkey |

All require optional bearer auth if `--api-token` is configured on the
hub (passed via `--token` or `NN_HUB_API_TOKEN`).

## Limitations (v1)

- The provisioner needs a local copy of the hub's private keys (see
  Install section). Future versions will give each provisioner its own
  keypair that devices trust independently.
- BLE scan + GATT writes are blocking on Bluez stack quirks; for first
  pairing you may need `rfkill unblock bluetooth` and
  `bluetoothctl power on`.
- One device at a time. Concurrent BLE provisioning is possible but
  not exposed by the CLI yet.
