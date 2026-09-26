# nn-hub REST API guide

This document covers the operational REST surface exposed by `nn-hub serve`
on `http://<hub>:8769/api/v1/...`. Every workflow here is **API-driven** —
no USB/UART required after a device has been physically attached to a
gateway.

The same endpoints back the `nn-hub` CLI; you can use either.

## Quickstart

```bash
HUB=nn-hub.local:8769          # your hub address

# Provision a device (one-time setup over BLE).
nn-hub device new --name c6-s1 --type sample_c6

# Register a firmware image as the new target for sample_c6 devices.
curl -X POST -F "device_type=sample_c6" -F "version=2.4.0" \
     -F "file=@build/zephyr.signed.bin" \
     http://$HUB/api/v1/firmware

# OTA the device.
curl -X POST http://$HUB/api/v1/devices/c6-s1/ota

# Compile + push an automation rule.
curl -X POST -H 'Content-Type: application/x-yaml' \
     --data-binary @automations.yaml \
     http://$HUB/api/v1/auto/compile
curl -X POST http://$HUB/api/v1/auto/c6-s1/push
```

The rest of this document spells each of those out, with the data flow on
the wire.

---

## 1. Provisioning

A "provisioned" device means three pieces of state are wired together:

1. The Thread mesh dataset (network key, channel, PAN ID, mesh-local prefix)
   is loaded on the device.
2. The hub's X25519 public key is stored on the device (used to decrypt
   H2D ECIES envelopes).
3. The hub knows the device's identity: device-id (= first 8 bytes of the
   P-256 public key), P-256 public key, X25519 public key, device type,
   capabilities, and which gateway brokers traffic for it.

The primary path is BLE GATT pairing. Once paired, the hub pushes all
three pieces in one round trip.

### 1.1 BLE provisioning (recommended)

```bash
nn-hub device new --name c6-s1 --type sample_c6
```

Under the hood:

1. **Scan**: hub does a BLE scan and looks for our service UUID
   advertised by un-provisioned devices.
2. **Connect + ECDH key exchange**: hub and device perform an
   authenticated Diffie-Hellman over the GATT characteristic to
   establish an ephemeral session key.
3. **Provision payload**: hub sends, encrypted, the Thread operational
   dataset (TLV-encoded), the hub's X25519 public key, and the
   gateway's identity.
4. **Device-side persistence**: the device stores everything in NVS
   (Zephyr settings subsystem). On every subsequent boot, OpenThread
   auto-attaches and the device starts talking to the hub via the
   gateway.
5. **DB write**: hub registers the device in the `devices` table
   (identity + P-256 pub) and `provision_info` table (X25519 pub,
   device type, capabilities, coap address, gateway id).
6. **CoAP probe** (skipped with `--skip-coap`): hub does an
   encrypted GET_INFO over CoAP to confirm the device responds
   end-to-end.

Useful options:

| Option | Purpose |
|---|---|
| `--addr MAC` | Skip scan; connect to a specific BLE MAC. |
| `--dataset-hex HEX` | Provision with a specific operational dataset (default: hub's active dataset). |
| `--gateway DEVICE_ID` | Bind the device to a specific gateway. |
| `--scan-time SECONDS` | BLE scan duration. |
| `--skip-coap` | Skip the post-provisioning health check. |

### 1.2 Re-registering a device whose keys changed

If a device regenerated its identity (e.g. NVS got wiped) but you don't
have BLE access right now, you can re-register with the `register`
sub-command, supplying the new public key as base64:

```bash
nn-hub device register <new-device-id> <name> end_device <P256_PUBKEY_B64>
```

This only stores the identity row — Thread dataset and hub-key delivery
still need to happen via BLE or the gateway-net transport.

### 1.3 REST equivalent

```http
POST /api/v1/devices/new
Content-Type: application/json

{ "name": "c6-s1", "type": "sample_c6" }
```

Returns 201 with `{ "device_id", "device_name", ... }`.

### 1.4 Querying registered devices

```bash
curl http://$HUB/api/v1/devices
curl http://$HUB/api/v1/devices/c6-s1
curl http://$HUB/api/v1/gateways
```

`last_seen` (Unix epoch seconds) is updated whenever the hub authenticates
a D2H frame from that device or gateway.

### 1.5 Pending (unknown) devices

When a sensor's identity keys are regenerated (factory reset / NVS wipe)
its new pubkey isn't yet in the hub's `devices` table.  The sensor will
still emit D2H frames via the gateway; the hub now logs each unknown
`device_id` as a *pending* sighting and surfaces it on the API.

```bash
curl http://$HUB/api/v1/devices/pending
```

Returns an array, e.g.:

```json
[
  {
    "device_id":   "a1b2c3d4e5f60718",
    "first_seen":  1779544800,
    "last_seen":   1779544857,
    "via_gateway": "f31768bfdc2a56c0",
    "frame_count": 12,
    "hint": "Device is sending frames but is not enrolled in the hub. Recover via either: (a) BLE re-pair — `nn-hub device new` while the sensor advertises, or (b) Direct enroll — POST /api/v1/devices with this device_id and the sensor's X25519 pubkey if you already have it."
  }
]
```

Sightings are in-memory only (cleared on hub restart), capped at 64
entries, and auto-expire after 24 h of inactivity.  Endpoints that
reference a missing device (e.g. `GET /api/v1/devices/{dev}` and
`POST /api/v1/devices/{dev}/ota`) include `pending: true` plus the same
`hint` in their 404 body when the queried id matches a pending sighting,
so operators can recover without polling the dedicated endpoint.

`GET /api/v1/gateways` also exposes `pending_devices_seen` (total count)
to surface the issue on the gateway dashboard at a glance.

---

## 2. OTA firmware updates

Firmware management has two halves: the **firmware target** (what
version a device-type should be running) and the **OTA trigger** (tell a
specific device to pull the target).

### 2.1 Upload a firmware target

```bash
curl -X POST \
     -F "device_type=sample_c6" \
     -F "version=2.4.0" \
     -F "file=@/path/to/zephyr.signed.bin" \
     http://$HUB/api/v1/firmware
```

The body must be a multipart form with three fields:
`device_type`, `version`, and `file` (the MCUboot-signed binary).
Hub computes SHA-256, copies the file into the firmware directory,
and replaces the existing target row for that device type. There is
one active target per device type at a time.

List current targets:

```bash
curl http://$HUB/api/v1/firmware
```

### 2.2 Trigger OTA on a specific device

```bash
curl -X POST http://$HUB/api/v1/devices/c6-s1/ota
```

What the hub does:

1. Look up the device's registered gateway route and X25519 pub.
2. Build an H2D **OTA_HINT** frame (cmd `0x002E`), encrypted with
   ECIES against the device pubkey.
3. Send via the gateway, retrying up to 5× with exponential backoff
   (1, 2, 4, 8 s) — 65 s total budget.
4. Wait for the matching D2H ACK and return its `ack_status`.

What the device does, on its own dedicated workqueue (no main-loop
stalls):

1. `ota_client_check()` — D2H **OTA_CHECK** → H2D **OTA_MANIFEST**.
   Device tells hub its current version + type; hub replies with
   the target version + size + sha256, or "up to date".
2. `ota_client_download()` — repeated D2H **OTA_BLOCK_REQ** /
   H2D **OTA_BLOCK** for 2007 blocks of 512 B each (a 1 MB image).
   Bytes are streamed straight into the MCUboot secondary slot
   (`slot1`).
3. `ota_client_apply()` — `boot_request_upgrade(BOOT_UPGRADE_TEST)`
   writes BOOT_MAGIC to slot1's trailer, then `sys_reboot()`.
4. MCUboot copies slot1 → slot0 (OVERWRITE_ONLY mode) and boots
   the new image.
5. New app calls `ota_client_confirm()` → `boot_write_img_confirmed()`
   so the image becomes permanent and won't be reverted.

Each D2H carries the hub's ECDSA signature over the inner payload, so
the hub authenticates every block request.

### 2.3 Manual control from the device shell

You usually want hub-driven OTA, but the device shell also supports
manual control over the same protocol:

```
mesh ota check
mesh ota download
mesh ota apply
mesh ota status
mesh ota auto-apply on|off
mesh ota confirm    # after a test boot, mark permanent
```

`mesh ota auto-apply on` causes `download` to chain straight into
`apply` (useful when scripting from a single hint message).

### 2.4 What the device confirms

After a successful boot the device's `mesh ota status` should report:

```
Firmware  : 2.4.0+0 (sample_c6)
Confirmed : yes
Pending  : (none)
```

If `Confirmed: no (test)` shows up, the app hasn't yet called
`ota_client_confirm()` — MCUboot will revert on the next reboot.

### 2.5 Caveats

- **First-time deployment to an unpatched device** still needs a
  cold-cycle once. The `sys_reboot` warm-reset hang was fixed in
  the nn-zephyr SoC layer (`sys_arch_reboot` now calls
  `esp_rom_software_reset_system()` for a digital-core reset
  instead of CPU-only). The fix only takes effect once the
  patched image is *running*, so the very first OTA from an
  unpatched fleet member needs an operator power-cycle to let
  MCUboot finish the swap. Subsequent OTAs are fully automatic.
  See `feedback_mcuboot_warm_reset_swap_hang.md` in the agent's
  memory for the diagnosis.
- OTA downloads can be slow on devices still suffering PSA-141
  signing instability; expect 60–90 minutes for a 1 MB image until
  the PSA mitigations land.

---

## 3. Automations

Automations are HA-style "when trigger, do action" rules in YAML. The
hub compiles each YAML rule set into per-device binary configs and
pushes them to the devices, where a small auto-engine evaluates them.

### 3.1 YAML format

```yaml
automations:
  - id: btn_s2_to_led_s3
    trigger:
      - device: c6-s2
        field: button
        equals: 1
    condition: []
    action:
      - device: c6-s3
        field: led
        value: on
```

- `trigger.equals|above|below|not_equals THRESHOLD` — the device that
  owns the trigger evaluates the comparison every time the field
  updates.
- `condition` (optional) — a list of conditions; the action only
  runs if all condition devices report values matching their
  comparisons within `timeout_ms` of the trigger fire.
- `action` — one entry per actuator field to set. `value` is the
  raw value to write (YAML `on`/`off` becomes boolean true/false;
  numbers/strings come through as-is).

Multiple actions, multi-device fan-out, and chained automations
across three or more devices all work. See `tests/test_auto.py`
for more involved examples.

### 3.2 Compile

```bash
curl -X POST -H 'Content-Type: application/x-yaml' \
     --data-binary @automations.yaml \
     http://$HUB/api/v1/auto/compile
```

Returns:

```json
{
  "compiled": {
    "c6-s2": {"triggers": 1, "conditions": 0, "actions": 1, "binary_bytes": 87},
    "c6-s3": {"triggers": 1, "conditions": 0, "actions": 1, "binary_bytes": 118}
  },
  "device_count": 2
}
```

The compiler:

1. Validates each `device:` name exists in the device registry.
2. Validates each `field:` is in the device's declared capabilities
   (and that triggers reference sensor fields, actions reference
   actuator fields).
3. Resolves each name in `notify:` lists to the device's mesh-local
   IPv6.
4. Packs the rules into compact binary blobs (header + per-rule
   structs) and stores them in the `configs` table as
   `{ "v": 1, "auto_bin": "<base64>" }`.

Both per-device JSON (for inspection) and binary blob (for push) are
stored. `nn-hub auto show --device c6-s2` prints the JSON form.

The **CLI** `nn-hub auto compile` only writes the JSON form, not the
binary. Use the REST endpoint if you want a single round-trip that's
ready for push.

### 3.3 Push

After compile, the device-side rule storage is *stale* until the hub
explicitly pushes the new config:

```bash
curl -X POST http://$HUB/api/v1/auto/c6-s2/push
```

What happens:

1. Hub looks up the stored `auto_bin` for the device.
2. ECIES-encrypts it against the device's X25519 pubkey.
3. Sends as H2D **AUTO_PUSH** (cmd `0x0021`) via the gateway.
4. Device decrypts, calls `auto_engine_load()` to swap in the new
   rule set, and replies D2H **AUTO_ACK** with a 1-byte status:
   `0` = applied, otherwise an errno-style failure (e.g. -EINVAL,
   -ENOMEM, -EACCES if the hub pubkey wasn't yet stored).
5. The HTTP response surfaces the status:

```json
{ "device_id": "...", "device_name": "c6-s2", "version": 5,
  "blob_bytes": 87, "status": 0 }
```

Verify on the device:

```
mesh:~$ mesh auto status
Rules loaded: 2
```

You can also inspect the device-side rule list (which fields it
watches, where it sends notifications) via `auto_engine` info logs
emitted right after the push:

```
auto_engine: Loaded 2 automation rules (87 bytes)
auto_engine:   [0] btn_s2_to_led_s3  role=T  field=button  notify=1
auto_engine:   [1] btn_s1_to_led_s2  role=A  field=led    notify=0
```

`role=T` = trigger, `role=C` = condition, `role=A` = action.

### 3.4 At runtime

- A trigger fires when its device's field crosses the comparison
  (locally, via the actuator-cb). The device sends an `AUTO_NOTIFY`
  D2D-via-gateway message to each `notify:` peer.
- Each peer's auto-engine looks up the matching action rule. If
  conditions hold (or `condition: []`), the actuator-cb gets
  called → the LED turns on, the actuator updates the local
  `led` field, telemetry flows back to hub.
- The hub sees the value change via D2H telemetry and stores it.
  You can read the resulting field via:

  ```bash
  nn-hub device read c6-s3 led
  ```

  or REST: `GET /api/v1/devices/c6-s3/fields/led`.

### 3.5 Caveats

- A device only acts on rules it knows about. Always re-`push`
  after a `compile`.
- A push can fail with status `-13` (EACCES) if the device hasn't
  yet stored the hub's X25519 pubkey. Re-provision via BLE, or
  push the hub key into the device's settings store with the
  `mesh set-hub-pub <hex>` shell command (used only when BLE
  provisioning isn't available).
- The auto-engine is in-memory only — rules don't persist across
  reboot. The hub re-pushes when it sees a device come back online
  after a long absence; you can also re-push manually any time.
- Field writes from the hub (e.g. `nn-hub device write c6-s2 led 1`)
  go through the same FIELD_OP → actuator-cb path that local
  triggers use, so manual writes interleave naturally with
  automation.

---

## 4. Cross-references

- Protocol wire format: `docs/protocol/nn_proto.md` (cmd numbering,
  ECIES envelope format, retry semantics).
- Image factory: `docs/factory.md`.
- Auto-engine internals: `tests/test_auto.py` is the cleanest
  spec of what the compiler accepts and what each device-role
  payload looks like.
- Device-side shell commands: see `mesh --help` on a connected
  device.
