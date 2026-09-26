# Hub UI Design Brief

A self-contained brief for designing a web UI on top of the nn-hub
REST API. The intended reader is a designer (human or AI) with no
prior context for this codebase. Everything they need to start
sketching screens should be in this document.

> If you're feeding this to an AI designer, this whole file is the
> system context. Output should be Figma-style mockups, component
> trees, or React/Svelte/Vue scaffolds — pick whatever the tool
> produces best, this brief doesn't constrain the rendering stack.

## Table of contents

1. [Product overview](#1-product-overview)
2. [Personas + journeys](#2-personas--journeys)
3. [Glossary](#3-glossary)
4. [Information architecture](#4-information-architecture)
5. [User flows in detail](#5-user-flows-in-detail)
   - 5.1 First-time hub setup
   - 5.2 Provision a gateway
   - 5.3 Provision a sensor
   - 5.4 Recover a pending (unknown) device
   - 5.5 Upload firmware + trigger OTA
   - 5.6 Live control: read + write fields
   - 5.7 Edit + push automation rules
   - 5.8 View logs
   - 5.9 Factory: build firmware from source
6. [REST API reference](#6-rest-api-reference)
7. [State machines + async behaviour](#7-state-machines--async-behaviour)
8. [Error model + recovery hints](#8-error-model--recovery-hints)
9. [Suggested screens + components](#9-suggested-screens--components)
10. [Open questions for the designer](#10-open-questions-for-the-designer)

---

## 1. Product overview

The hub is the brain of a small wireless mesh network of sensors and
actuators. It does five things:

- **Identity + registry** — keeps a record of every gateway and sensor
  on the network: their public keys, names, types, last-seen times.
- **Provisioning** — onboarding flow that exchanges crypto keys and a
  Thread network configuration over BLE so a brand-new device can
  join the mesh.
- **OTA firmware updates** — accepts a firmware blob, hints the
  matching sensor to pull it down, and tracks the swap.
- **Field control** — read or write a "field" (a typed value, e.g.
  `led_state = 1`) on a specific sensor via an encrypted relay.
- **Automation engine** — operator writes YAML rules ("when sensor A's
  button toggles, set sensor B's LED"), the hub compiles them to
  per-device binary blobs and pushes them. Sensors execute rules
  locally on the mesh; no hub round-trip at runtime.

There's no cloud component. Everything runs on the hub host (a
Raspberry Pi, a PC, a Docker container, or eventually a managed cloud
service). The hub exposes a REST API on port `8769` and an
authenticated gateway protocol on port `8767`.

A small CLI (`nn-hub …`) wraps every operation; this UI replaces the
CLI for non-technical operators. There is *no* feature the UI needs
that the API can't already do.

---

## 2. Personas + journeys

| Persona | Journey | Frequency |
|---|---|---|
| **First-time installer** | Bring up the hub, mint a Thread network, provision the first gateway, provision a few sensors, write a "hello world" automation. | Once per install |
| **Operator (day-to-day)** | Glance at the dashboard, read a field, push a config update, trigger an OTA, recover a misbehaving device. | Daily |
| **Maintenance / on-call** | Investigate logs, diagnose a pending (unknown) device, roll back a bad OTA. | Rarely, but stressful |
| **Developer / factory** | Build firmware from source, upload signed binaries to the hub, sign off on a release. | Per release |

Design weight should be in this order: **operator > installer >
maintenance > developer**. The first two are the everyday surface.

---

## 3. Glossary

| Term | Meaning |
|---|---|
| **Hub** | The host running this UI + REST API. Owns the Thread network identity, all crypto keypairs, and the device registry. |
| **Gateway** | A small Linux daemon paired with a 802.15.4 radio chip (ESP32-C6 NCP). Sits between the hub (over WiFi/TCP) and the Thread mesh. One gateway per RF zone. |
| **Sensor / device** | An ESP32-C6 endpoint on the mesh. Has buttons, LEDs, ADC inputs. Runs a small Zephyr app; can be OTA'd. |
| **Pending device** | A sensor the hub has *seen* on the mesh (received an authenticated frame) but doesn't have in its registry. Indicates a key wipe / factory reset. |
| **Field** | A named, typed scalar value on a sensor (e.g. `led_state: u8`, `button: u8`). Readable and writable. |
| **Automation** | A YAML rule of the shape `when <trigger> if <condition> then <action>`. Compiled to a binary blob and pushed per-device. |
| **OTA hint** | The hub doesn't push firmware bytes. It sends a tiny "go fetch v1.0.1 from this URL" hint; the device pulls the blob over CoAP/HTTP. |
| **Thread network** | The 802.15.4 mesh. One per hub. Has a channel, PAN ID, network key, mesh-local prefix. Auto-generated on first gateway provisioning. |
| **Role** | What a Thread node is doing right now: `detached`, `child`, `router`, `leader`. UI should show role on every gateway/device card. |
| **RLOC16** | A 16-bit address each Thread node holds. Like an IP, just shorter. Mostly debug info. |
| **ML-EID** | Mesh-local IPv6 address — the routable handle for a Thread node. |
| **NCP** | Network co-processor — the radio chip the gateway talks to. Spinel protocol over UART. |
| **MCUboot** | The bootloader that swaps slot 0 ↔ slot 1 during OTA. |
| **Pubkey** | Two pubkeys per device: P-256 (signing) and X25519 (encryption). Both base64. |

---

## 4. Information architecture

Top-level entities:

```
Hub
├── Identity            (hub_id, P-256 pub, X25519 pub) — fixed per install
├── Network             (Thread mesh: channel, panid, dataset TLVs)
├── Gateways            [ { id, name, role, rloc16, mleid, last_seen } ]
│   └── Devices serviced
├── Devices             [ { id, name, type, last_seen, gateway_id } ]
│   ├── Provision info  (eui64, mdns_addr, coap_addr, capabilities)
│   ├── Fields          (dynamic, name → typed value)
│   ├── Automations     (compiled rule blob, version, updated_at)
│   └── OTA target      (firmware blob signed for this device's type)
├── Firmware targets    [ { device_type, version, size, sha256 } ]
├── Pending devices     [ { device_id, first_seen, last_seen, via_gateway } ]
└── Logs                (time-ordered, indexed by device_id + tag + level)
```

A "gateway" and a "device" are both registered identities with similar
shapes (id, name, last_seen, pubkey). The difference is functional:
gateways forward, sensors run automations. The UI should treat them as
two top-level lists.

---

## 5. User flows in detail

For every flow: what the user does, what the UI shows, which API calls
fire, what success/failure looks like.

### 5.1 First-time hub setup

**Goal:** the user opens the UI for the first time and gets to a
"there's nothing here yet, here's what to do next" state.

**Trigger:** the hub starts with an empty DB and no Thread network.

**Steps:**
1. UI calls `GET /api/v1/healthz` to confirm the hub is reachable.
2. UI calls `GET /api/v1/network` →
   - If `{"configured": false}`: show an empty-state screen with a
     prominent **Initialise Thread network** button.
   - If configured: skip to the dashboard.
3. User clicks **Initialise** → optional channel selector (default
   auto), optional name → `POST /api/v1/network/init` with
   `{name?, channel?}`.
4. UI shows the freshly-minted network: name, channel, PAN ID, dataset
   bytes, "0 gateways, 0 devices". Empty-state CTAs:
   - **Add gateway** (BLE) — opens the gateway provisioning wizard
     (§5.2)
   - **Add gateway** (network) — same, transport: net
   - **Upload firmware** — for OTA later (§5.5)

**Success state:** dashboard showing the configured network with
empty gateway/device lists.

**Failure state:** hub unreachable → show retry; auth missing → token
prompt (rare; bearer is optional).

### 5.2 Provision a gateway

**Goal:** turn a never-paired ESP32-C6 NCP board into an authenticated
gateway that's joined the hub's Thread network.

**Two transports** — UI offers a toggle:

- **BLE** (recommended when hub has a Bluetooth radio): hub scans for
  the gateway advertising the provisioning service, connects, does the
  GATT exchange.
- **Network** (the gateway is already booted in net-provision mode and
  advertising over mDNS `_nn-gw._tcp.local`): hub connects over TCP.

**Inputs the user provides:**
- WiFi SSID
- WiFi PSK (masked; "show" toggle)
- Hub host (the address the gateway will use to talk to the hub —
  usually the hub's own LAN IP, default-fillable from `window.location`)
- Optional: human label ("Kitchen gateway")
- Optional: explicit BLE MAC or net address (skips scan/mDNS)

**Steps:**
1. UI opens a wizard. Step 1: form with the inputs above.
2. UI calls `POST /api/v1/gateways/new` with the JSON body. **This is
   long-running (5–15 s for BLE).** The UI must show a progress
   indicator with substeps:
   - "Scanning…"
   - "Connecting…"
   - "Writing WiFi credentials…"
   - "Writing hub identity…"
   - "Applying…"
   - "Done — gateway authenticated"

   The API doesn't currently stream substeps; the UI can either fake
   them with a determinate-looking spinner or poll `GET /api/v1/gateways`
   for the new gateway's appearance.

3. On 201 success → UI navigates to the new gateway's detail page
   showing `id`, `pubkey`, `role` (initially `detached`, will move to
   `child` → `router` → `leader` over ~30 s).

**Open issue for design:** the BLE step takes long enough that the UI
should let the user back out and provision a different gateway. Either
toast-it ("provisioning in background") or block-modal it — your
call.

**Failure modes:**
- `400 missing 'ssid'/'psk'/'hub_host'` — form validation should
  prevent.
- `502 provisioning failed: …` — show the message verbatim, link to
  troubleshooting docs.
- Network timeout — common with weak BLE signal; offer retry.

### 5.3 Provision a sensor

Like §5.2 but simpler — no WiFi creds. Sensor is BLE-paired with the
hub (or a separate provisioner host running `nn-provisioner`, see
`provisioner.md`).

**Inputs:**
- Sensor name (label)
- Device type (dropdown: `sample_c6`, `…` — populated from
  `GET /api/v1/firmware` so the user sees only types that have a
  firmware target uploaded)
- Optional: BLE MAC (skip scan)
- Optional: associate with a specific gateway (dropdown of registered
  gateways; default = any)

**Steps:**
1. UI calls `POST /api/v1/devices/new` with the body. Long-running
   (5–15 s).
2. UI shows substeps similar to gateway provisioning.
3. On 201 success, sensor appears in the device list with `last_seen`
   updating shortly after.

**Visual hint:** colour-code the device card by `last_seen`:
- < 60 s ago → green
- < 5 min → amber
- > 5 min → red ("offline")

### 5.4 Recover a pending (unknown) device

**Trigger:** the hub sees a sensor on the mesh whose key isn't in the
registry (NVS wipe, factory reset, re-flash). The hub doesn't accept
its frames but records the sighting.

**Surface:**
- A persistent badge on the **Devices** page header:
  `🟡 2 pending devices`
- The badge links to `/devices/pending` which renders the response
  from `GET /api/v1/devices/pending`.

**Pending device card:** shows
- Device ID (16-hex; truncate display, click to copy)
- First seen / last seen
- Via gateway (link to gateway)
- Frame count
- A `Hint` text (already in the API response body) — render it
  verbatim.

**Recovery actions on the card:**
- **Re-pair via BLE** — opens the §5.3 sensor wizard pre-filled with
  this device ID (but device ID isn't an input there, so really this
  is "kick off a fresh BLE pair").
- **Enroll pubkey** — opens a form: paste the device's X25519 pubkey
  (operator copy-pastes from their dev/manufacturing toolchain),
  POSTs `/api/v1/devices`.

**Visual hint:** make this discoverable. A user who hits `GET
/api/v1/devices/{old-id}` for a wiped device gets a 404 with
`pending: true` + a hint in the body — the UI should auto-redirect to
the pending detail page in that case rather than showing "device
not found".

### 5.5 Upload firmware + trigger OTA

**Two screens chained:**

#### 5.5a — Firmware library

Page lists `GET /api/v1/firmware` results: one row per device type,
showing `device_type, target_version, size, sha256, updated_at`.

Top-right button: **Upload firmware…**
- File picker (`.signed.bin`)
- Device type dropdown (from existing devices)
- Version field (semver-ish)
- Submit → multipart `POST /api/v1/firmware`
- On success, the row updates with the new version.

Each row also has a **Where is this used?** button → links to a
filtered devices list (devices of that type).

#### 5.5b — Per-device OTA trigger

On the device detail page, an **Update firmware** button is enabled
*iff* there's a firmware target uploaded for this device's type AND
the target version differs from the device's reported version.

Clicking shows a confirm dialog:
> "Update **sensor-01** from v1.0.0 to **v1.0.1**?
> The device will reboot and apply the update; this typically takes
> 12–40 s. The device will be offline briefly during the swap."
> [Cancel] [Update]

Confirm → `POST /api/v1/devices/{dev}/ota` (returns immediately with
`ack_status`).

**UI then watches** for the version bump:
- Poll `GET /api/v1/devices/{dev}` every 3 s
- Compare `device_version` (TBD — needs a field; today logs report
  it) with `target_version`
- Show three states: **Hint sent**, **Applying** (when device drops
  off briefly), **Done** (when device re-appears with new version)
- Time out after 5 min with a "may have failed; check logs" CTA.

### 5.6 Live control: read + write fields

**Per-device "fields" panel** on the device detail page:

For a known device with `capabilities = ["led_state", "button"]`
(stored in provision_info), show one row per field:

```
led_state   [  0 ]  [ Refresh ] [ Write… ]   (read-only display + actions)
button      [  1 ]  [ Refresh ]
```

- **Refresh** → `GET /api/v1/devices/{dev}/field/{field}` →
  `{"name": "led_state", "value": 0}`. Cache for 30 s with a
  staleness indicator.
- **Write…** (only on writable fields — UI hint: ask the
  manufacturer; today this is "any field but `button`") opens a small
  modal with a numeric input → `PUT /api/v1/devices/{dev}/field/{field}`
  body `{"value": 1}`.

**Timing:** each field op is a CoAP round-trip through the gateway,
typically 200–800 ms. Show a small spinner, but no blocking modal.

**Discoverability:** `capabilities` is currently set at provisioning
time. New fields require re-provisioning. Flag this in copy — the UI
shouldn't show writes for fields not in `capabilities`.

### 5.7 Edit + push automation rules

This is the most "advanced" flow. Automations are written as YAML;
the UI should keep the YAML as the source of truth but render a
human view alongside.

**Page:** `/automations`
- Left pane: YAML editor (Monaco or CodeMirror; syntax highlighting +
  fold)
- Right pane: parsed view — a list of rules with `when → if → then`
  human-readable, with links to mentioned devices.
- Bottom toolbar:
  - **Compile** → `POST /api/v1/auto/compile` with body
    `{"yaml": "<full text>"}` *or* `Content-Type: application/x-yaml`
    raw body. Shows per-device summary:
    ```
    compiled:
      sensor-01: { triggers: 1, conditions: 0, actions: 0, binary_bytes: 87 }
      sensor-02: { triggers: 0, conditions: 0, actions: 1, binary_bytes: 64 }
    device_count: 2
    ```
  - **Push to all** → for each device listed in `compiled`, fire
    `POST /api/v1/auto/{dev}/push`. Show per-device status with a
    "retry" affordance on failure.
  - **Push to one** (on hover of a device row) → just that device.

**Sample YAML** the editor should ship with:
```yaml
automations:
  - alias: led_follows_button
    trigger:
      - device: sensor-01
        platform: state
        field: button
        to: 1
    action:
      - device: sensor-02
        service: set_field
        data: { field: led_state, value: 1 }
```

**State indicator per device:**
- Last compiled version (from `GET /api/v1/auto/{dev}` response —
  `version`, `updated_at`).
- Last push status (success/fail/never).

**Important:** compile is idempotent and side-effect-free
(generates the blob, stores in DB). Push actually delivers it. The UI
should make this two-stage; never auto-push on compile.

### 5.8 View logs

**Page:** `/logs`
- Filter bar: device (dropdown of registered devices), level
  (info/warn/err), tag (free text), since (date picker → epoch or
  "5m" / "1h" relative), limit (default 50, max 1000).
- Table: timestamp, device, level (coloured), tag, message.
- Live tail toggle: when on, refetch every 5 s with `since=last_ts`.
- Download log files: `GET /api/v1/log_files` returns the list of
  rotated SQLite files on disk; the UI can present these as
  downloadable, but the API doesn't currently stream them — show
  paths + sizes as info.

### 5.9 Factory: build firmware from source

The hub talks to a *separate* factory service on `:8100` (started by
`nn-hub factory start`). The UI may or may not want to expose this —
it's a developer flow.

If shown, put it behind a **Settings → Factory** tab.

**Inputs:**
- App name (dropdown of available device apps; today: `ble_ot_esp32c6`,
  `ncp_esp32c6`, …)
- Board target (auto-detected from app's `.nn-build.yaml`)
- Version string (semver)
- Factory URL (default `http://localhost:8100`)

**Action:** `POST <factory>/build` with body `{app, board, version}`.
Long-running (1-5 min). Poll `GET <factory>/status` every 2 s.

**On success:**
- Show the resulting `version`, `device_type`, `size`, `sha256`.
- **Promote** button → uploads the just-built binary to the hub via
  `POST /api/v1/firmware` (effectively automating §5.5a).

**Failure mode:** the build log (`result.log`, last 2 KB) should be
viewable in a modal — these failures are usually obvious typos in
prj.conf.

---

## 6. REST API reference

Base path: `/api/v1`. All endpoints return JSON unless otherwise
noted. Optional bearer auth: `Authorization: Bearer <token>` (only
required if the hub was started with `--api-token`).

### 6.1 Health + identity

#### `GET /healthz`
```json
{ "ok": true, "ts": 1779544800 }
```

#### `GET /hub/identity`
```json
{
  "hub_id":     "6d4feb08293967b1",
  "p256_pub":   "047a8f67…",   // 65 B uncompressed, hex
  "x25519_pub": "a5505f64…"    // 32 B, hex
}
```
Used by `nn-provisioner` to pin the hub before BLE-onboarding a device.

### 6.2 Network

#### `GET /network`
If never initialised:
```json
{ "configured": false, "message": "no network — auto-generated on first gateway provisioning" }
```
If initialised:
```json
{
  "configured":         true,
  "name":               "nn-mesh",
  "channel":            15,
  "panid":              13524,
  "extpanid_hex":       "0a1b2c…",
  "mesh_local_prefix":  "fd44:8b73:6d00:1::/64",
  "dataset_tlvs_bytes": 67,
  "dataset_tlvs_hex":   "0e08…",
  "gateway_count":      1,
  "device_count":       3
}
```

#### `POST /network/init`
Body: `{"channel"?: int, "name"?: str, "force"?: bool}`.
Returns the same shape as `GET /network` (configured=true).
`409` if already configured and `force` not set.

### 6.3 Gateways

#### `GET /gateways`
```json
[
  {
    "id":                   "f31768bfdc2a56c0",
    "name":                 "rpi4b-gw",
    "mdns_addr":            "",
    "registered_at":        1778766549,
    "last_seen":            1779544757,
    "role":                 3,
    "role_name":            "leader",     // detached|child|router|leader
    "rloc16":               1024,
    "mleid_hex":            "fd44…",
    "last_thread_state_at": 1779544757,
    "pending_devices_seen": 0
  }
]
```

#### `GET /gateways/{id}` — adds `devices_serviced` + `pending_devices_seen` (list).

#### `POST /gateways` — register a known-pubkey gateway (no BLE):
```json
{ "id": "<hex16>", "name": "<n>", "pubkey_b64": "<…>", "mdns_addr": "" }
```
Returns 201 with the gateway record.

#### `POST /gateways/new` — provision via BLE/net (long-running):
```json
{
  "ssid":      "MyWiFi",
  "psk":       "secret",
  "hub_host":  "hub.local",
  "transport": "ble",            // or "net"
  "addr":      "AA:BB:..." | null,
  "scan_time": 10.0,
  "name":      "Kitchen",
  "mdns":      ""
}
```
201 → `{ id, name, address, transport, pubkey_b64 }`.

### 6.4 Devices

#### `GET /devices`
List of:
```json
{
  "id":            "467a5423b223dfd5",
  "name":          "sensor-01",
  "type":          "sample_c6",
  "registered_at": 1778766549,
  "last_seen":     1779544851,
  "device_type":   "sample_c6",
  "mdns_addr":     "",
  "coap_addr":     "fd44:8b73:6d00:1:…",
  "gateway_id":    "f31768bfdc2a56c0",
  "eui64":         "…",
  "capabilities":  ["led_state", "button"],
  "provisioned_at":1778767000,
  "last_info_at":  1779540000
}
```

#### `GET /devices/{dev}` — same shape, single object. Accepts name or id. **404 body may include `{ "pending": true, "hint": "…", "pending_url": "/api/v1/devices/pending" }` if the queried id matches an unknown-device sighting** — the UI should surface this prominently.

#### `GET /devices/pending`
```json
[
  {
    "device_id":   "a1b2c3d4e5f60718",
    "first_seen":  1779544800,
    "last_seen":   1779544857,
    "via_gateway": "f31768bfdc2a56c0",
    "frame_count": 12,
    "hint":        "Device is sending frames but is not enrolled…"
  }
]
```

#### `POST /devices` — bare registration (no BLE):
```json
{ "id": "<hex16>", "name": "<n>", "type": "sample_c6", "pubkey_b64": "<opt>" }
```

#### `POST /devices/new` — provision via BLE (long-running):
```json
{
  "name":        "sensor-01",
  "device_type": "sample_c6",
  "gateway":     "<gw-id>",      // optional
  "addr":        "AA:BB:..." ,    // optional
  "scan_time":   12.0,
  "dataset_hex": ""               // optional override
}
```
201 → device record.

#### `POST /devices/{dev}/provision`
Updates the `provision_info` row. Body keys all optional; missing keys
preserve existing values: `device_type, enc_pubkey_b64, mdns_addr,
coap_addr, gateway_id, ble_addr, eui64, capabilities` (array).

### 6.5 Fields (live control)

#### `GET /devices/{dev}/field/{field}`
```json
{ "name": "led_state", "value": 0 }
```
404 if device unknown; 504 if device doesn't reply in time; 502 if the
relay path is broken.

#### `PUT /devices/{dev}/field/{field}`
Body: `{ "value": <numeric> }` → 200 with the new value echoed.

### 6.6 OTA

#### `GET /firmware`
```json
[
  {
    "device_type":    "sample_c6",
    "target_version": "1.0.1",
    "firmware_path":  "/var/lib/nn-hub/firmware/sample_c6-1.0.1.signed.bin",
    "size_bytes":     245760,
    "sha256":         "…",
    "updated_at":     1779540000
  }
]
```

#### `POST /firmware` — multipart upload
Form fields: `device_type` (text), `version` (text), `firmware` (file).
Returns the new target record.

#### `POST /devices/{dev}/ota` — trigger OTA on a single device
No body. Returns:
```json
{
  "device_id":    "…",
  "device_name":  "sensor-01",
  "device_type":  "sample_c6",
  "target_version": "1.0.1",
  "ack_status":   0      // 0 = device accepted; non-zero = errno
}
```
`ack_status` is *not* "OTA finished" — it's "device acknowledged the
hint". The actual download + swap takes 12-40 s; the UI polls for the
version bump (§5.5b).

### 6.7 Automations

#### `POST /auto/compile`
Body: `Content-Type: application/x-yaml` raw, *or* JSON
`{"yaml": "<text>"}`. Returns:
```json
{
  "compiled": {
    "sensor-01": { "triggers": 1, "conditions": 0, "actions": 0, "binary_bytes": 87 },
    "sensor-02": { "triggers": 0, "conditions": 0, "actions": 1, "binary_bytes": 64 }
  },
  "device_count": 2
}
```
Stores the compiled blob in the DB; doesn't push.

#### `GET /auto/{dev}`
```json
{ "device_id": "…", "device_name": "sensor-01", "version": 3, "updated_at": 1779540000, "payload": "<hex…>" }
```

#### `POST /auto/{dev}/push`
No body. Returns `{ "ack_status": 0 }` or an error. Async on the
device side (typically 200 ms).

### 6.8 Logs

#### `GET /logs?device=&level=&tag=&since=&limit=`
- `device` — name or id
- `level` — `debug|info|warn|error`
- `tag` — free-text (matches the `tag` column)
- `since` — ISO-8601 or `"5m"`/`"1h"` relative
- `limit` — int, capped at 1000

Returns rows:
```json
[
  { "ts": 1779544800, "device_id": "467a…", "level": "info", "tag": "auto_engine", "msg": "…" }
]
```

#### `GET /log_files` — rotation files on disk
```json
[ { "name": "logs_1779540000.db", "path": "/var/lib/nn-hub/logs/…", "size_bytes": 123456 } ]
```

### 6.9 Factory service (separate process, default `:8100`)

Not part of the hub REST API. Documented for completeness.

#### `POST /build`
Body: `{ "app": "ble_ot_esp32c6", "board": "esp32c6_devkitc/esp32c6/hpcore"?, "version": "1.0.1"? }`.
Returns `{status:"ok"|"error", version, device_type, size, sha256, path, paths}` or `{status:"error", message, log}`.

#### `GET /status` → `{status:"idle"|"building", …}`
#### `GET /health` → `{ok: true}`

### 6.10 Error model

Every non-2xx response carries:
```json
{ "err": "<human message>", "...maybe extra keys..." }
```
404 device responses may add `pending: true`, `hint: "…"`,
`pending_url: "/api/v1/devices/pending"` when relevant — render the
hint as the user-facing message.

Status code conventions:
- `400` — bad input (missing field, bad JSON)
- `401` — bearer token required/wrong
- `404` — entity not found
- `409` — conflict (e.g. network already exists, build in progress)
- `502` — upstream relay error (gateway / device unreachable)
- `503` — proto_router not started (rare; transient)
- `504` — device/gateway didn't reply in time (very common — OFFER RETRY)

---

## 7. State machines + async behaviour

### 7.1 Async operations

| Operation | Latency | UI pattern |
|---|---|---|
| Read field | 200–800 ms | inline spinner |
| Write field | 200–800 ms | inline spinner + checkmark |
| Provision device/gateway | 5–15 s | wizard with progress |
| Trigger OTA | <1 s ack; 12–40 s apply | poll for version bump |
| Compile automation | <500 ms | block UI briefly |
| Push automation | 200–800 ms per device | per-row spinner |
| Factory build | 1–5 min | progress + log tail |

### 7.2 Polling cadences

| What | Interval | Stop condition |
|---|---|---|
| Dashboard refresh | 5 s | tab hidden |
| Field value (live tail) | 10 s | view changes |
| OTA progress | 3 s | version matches target, or 5 min timeout |
| Pending devices badge | 30 s | always running |
| Factory build status | 2 s | status != "building" |
| Logs (live tail toggle) | 5 s | toggle off |

### 7.3 Gateway / device life cycle

```
                  ┌──── (BLE) ──── provisioning ─────┐
unknown ──────────┤                                  ▼
                  └─── (net) ───── provisioning ───► registered
                                                       │
                                                       ├── role: detached
                                                       ├── role: child
                                                       ├── role: router
                                                       └── role: leader (gateways only)

(NVS wipe at any point) ─► pending (visible in /devices/pending,
                                     not in /devices)
```

UI should reflect role on every gateway card; for devices, role is
less meaningful but `last_seen` freshness is the indicator.

---

## 8. Error model + recovery hints

The hub puts recovery hints in error bodies. Render them.

**Pending device** (404 with hint):
```json
{
  "err": "device not found",
  "pending": true,
  "hint": "Device is sending frames but is not enrolled in the hub. Recover via either: (a) BLE re-pair — `nn-hub device new` while the sensor advertises, or (b) Direct enroll — POST /api/v1/devices with this device_id and the sensor's X25519 pubkey if you already have it.",
  "pending_url": "/api/v1/devices/pending"
}
```
UI behaviour: instead of "device not found" toast, navigate to the
pending detail page with the hint surfaced as a yellow banner.

**504 Timeout** on OTA / field op: very common, almost always
transient. UI should offer a one-click retry, not bury under a
generic error.

**409 Conflict** on `network/init`: surface `force=true` checkbox.

---

## 9. Suggested screens + components

This is a starting point; the designer should feel free to recompose.

### Screens (sitemap)

```
/                                  Dashboard (overview)
/network                           Thread network details
/gateways                          List
/gateways/:id                      Detail + serviced devices
/devices                           List + pending badge
/devices/pending                   Pending sightings
/devices/:id                       Detail
  ├── tabs:
  │   ├── Overview      (status, last_seen, role)
  │   ├── Fields        (live read/write, §5.6)
  │   ├── Firmware      (current + target version, §5.5b)
  │   ├── Automations   (rules referencing this device, §5.7)
  │   └── Logs          (filtered to this device)
/firmware                          Firmware library, §5.5a
/automations                       YAML editor, §5.7
/logs                              Global log view, §5.8
/settings
  ├── Hub identity                 Public keys, copy buttons
  ├── Network init / reset
  ├── Factory                      §5.9
  └── API token                    (if configured)
```

### Components

- **DeviceCard** — id, name, type, role, last_seen freshness dot,
  click → detail.
- **GatewayCard** — same + role + RLOC16 + serviced-device count.
- **HealthBadge** — green/amber/red based on last_seen.
- **PendingBanner** — top-of-page yellow band when pending count > 0.
- **ProvisionWizard** — multi-step, used for gateway + device flows.
- **FieldRow** — name, current value, refresh+write actions.
- **YamlEditor** — Monaco wrapper with `application/x-yaml` mode.
- **CompileResultPanel** — per-device row showing triggers/conditions/actions/bytes.
- **PushResultRow** — per-device success/fail + retry.
- **FirmwareUploader** — drag-drop file + type + version → multipart POST.
- **OtaProgress** — three-state (hint sent / applying / done) + countdown.
- **LogTable** — virtualised; filter bar; live tail toggle.
- **CopyableHex** — truncated display, click to copy full value (used
  for device_id, mleid_hex, sha256, pubkey).

### Visual conventions

- Time values: always show as **relative** ("3 m ago") in lists, full
  timestamp in detail. Source is `last_seen` unix epoch.
- Roles: `detached` red, `child` amber, `router`/`leader` green.
- Hex IDs: monospace font, truncated to first 8 chars in tables.
- Long-running ops: show explicit ETA when possible ("typically 12–40 s").

---

## 10. Open questions for the designer

These are real ambiguities — pick what feels right and we'll iterate.

1. **Single-pane dashboard or per-domain pages?** This brief assumes
   per-domain (gateways/devices/firmware/automations). A
   single-pane dashboard with collapsible sections may work better
   for small installs (1 gateway, 3 sensors).
2. **Real-time updates** — the API is poll-only. Is that OK, or do
   you want me to add a WebSocket / SSE stream? Most pages refresh
   every 5–10 s, which is fine for "operator glance" use cases.
3. **Mobile vs desktop priority?** The provisioning flows are
   physically-tied (operator standing near the device); mobile
   matters. Maintenance is desk work.
4. **Branding / theme** — none currently. Bring your own.
5. **Multi-user / RBAC** — out of scope for v1. The hub has a single
   optional bearer token. Designs that assume per-user accounts
   should be flagged as "needs API work".
6. **Automation editor: YAML-only or visual builder?** YAML is the
   power-user surface; a constrained visual builder is friendlier.
   This brief assumes YAML-primary with a parsed sidebar; happy to
   reverse it.
7. **Factory tab visibility** — hide for non-developer installs?
   Maybe gate behind a "developer mode" toggle in settings.

---

## Appendix A — Quick reference: every endpoint in one table

| Method | Path | Purpose | Async? |
|---|---|---|---|
| GET  | /healthz                            | Liveness | no |
| GET  | /hub/identity                       | Pin hub identity (for nn-provisioner) | no |
| GET  | /network                            | Thread network status | no |
| POST | /network/init                       | Mint or reset network | no |
| GET  | /gateways                           | List gateways | no |
| GET  | /gateways/{id}                      | Detail + serviced devices | no |
| POST | /gateways                           | Register a known-pubkey gateway | no |
| POST | /gateways/new                       | Provision via BLE/net | **yes, 5-15s** |
| GET  | /devices                            | List devices | no |
| GET  | /devices/pending                    | Unknown-device sightings | no |
| GET  | /devices/{id}                       | Detail (404 may carry pending hint) | no |
| POST | /devices                            | Register a known-pubkey device | no |
| POST | /devices/new                        | Provision via BLE | **yes, 5-15s** |
| POST | /devices/{id}/provision             | Update provision_info | no |
| GET  | /devices/{id}/field/{field}         | Read field | **yes, 200-800ms** |
| PUT  | /devices/{id}/field/{field}         | Write field | **yes, 200-800ms** |
| POST | /devices/{id}/ota                   | Trigger OTA hint | <1s ack, 12-40s apply |
| GET  | /firmware                           | Firmware library | no |
| POST | /firmware                           | Upload + register binary | <2s |
| POST | /auto/compile                       | Compile YAML to per-device blobs | <500ms |
| GET  | /auto/{dev}                         | Show stored config | no |
| POST | /auto/{dev}/push                    | Push to device | 200-800ms |
| GET  | /logs                               | Query logs | no |
| GET  | /log_files                          | List rotation files | no |

## Appendix B — Sample dashboard layout (text mockup)

```
┌──────────────────────────────────────────────────────────────────┐
│ nn-hub                                            [⚙ Settings]    │
├──────────────────────────────────────────────────────────────────┤
│                                                                    │
│  Thread network: nn-mesh   ch 15   pan 0xab12   1 gateway 3 dev   │
│                                                                    │
│  ┌── Gateways ──────────────────────────────────────────────────┐ │
│  │ rpi4b-gw    leader   rloc16:0x0400   serving: 3 devices  ●   │ │
│  │ [+ Add gateway]                                                │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  🟡 2 pending devices — review                                     │
│                                                                    │
│  ┌── Devices ───────────────────────────────────────────────────┐ │
│  │ sensor-01  sample_c6  v1.0.0  ● 3s   button=1  led=1          │ │
│  │ sensor-02  sample_c6  v1.0.0  ● 5s   button=0  led=0          │ │
│  │ sensor-03  sample_c6  v0.9.8  ● 2m   button=0  led=0  ⚠ update │ │
│  │ [+ Add device]                                                 │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ┌── Recent activity ───────────────────────────────────────────┐ │
│  │ 12:03:42  sensor-01  auto  rule led_follows_button fired      │ │
│  │ 12:03:42  sensor-02  auto  applied {led_state: 1}              │ │
│  │ 11:58:09  rpi4b-gw   thread role=leader                        │ │
│  │ [View all logs]                                                │ │
│  └──────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

---

## Appendix C — v2 addendum (2026-06-04)

Captures product changes and deployment decisions made since the
original brief.  **Designers: read this section after the main brief
— it amends sections 1, 6, 7, 9, and 10 in places.**

### C.1 Deployment topology change

The webapp is **not** part of the hub process.  It runs in its own
sibling LXC container (`nn-web` alongside `nn-hub` and `nn-gw`) and
talks to the hub exclusively via the REST API at port 8769.

```
┌─────────────────────────────────────────────┐
│ Raspberry Pi host (or any Linux host)       │
│                                             │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐ │
│  │  nn-gw   │←─→│  nn-hub  │←─→│  nn-web  │ │
│  │   LXC    │   │   LXC    │   │   LXC    │ │
│  │          │   │          │   │          │ │
│  │ Spinel   │   │ REST :8769│  │ HTTP :80 │ │
│  │ + TCP    │   │ TCP  :8767│  │ (browser)│ │
│  └──────────┘   └──────────┘   └──────────┘ │
│       ↕               ↕              ↕      │
│   Thread radio     SQLite DB      static SPA │
└─────────────────────────────────────────────┘
```

Implications for the designer:
- The webapp is a **plain SPA + small BFF**, not a hub subsystem.
- The BFF is the only thing that knows the bearer token (see §C.2).
- Real-time streaming, if any, must be implemented in the BFF; the
  hub is HTTP poll-only today.

### C.2 Authentication state — **OPEN today, planned closed**

The hub's REST API supports bearer-token auth in code
(`api.py` middleware, `--api-token` CLI flag, `NN_HUB_API_TOKEN` env),
but the deployed systemd unit does NOT pass a token:

```
$ curl -s -o /dev/null -w "%{http_code}\n" http://hub:8769/api/v1/devices
200
```

For UI design purposes, **assume v1 of the webapp ships with auth ON**.
The product-level requirements:

1. **Operator-facing login** — webapp prompts for a password (single
   user for v1, see §10 in the main brief for RBAC out-of-scope).
2. **Token never crosses the browser** — the BFF holds the hub API
   token in process memory or env, browser sends a session cookie.
3. **Brute-force protection** — short-lived sessions (1h), rate-limit
   login.
4. **First-run flow** — on first boot of `nn-web`, force the operator
   to set a password before showing any data.

The first-run password setup is a new screen not in §9 of the main
brief.  Sketch:

```
Welcome to nn-web
─────────────────────────────────────────────
You're connecting to hub: rpi4b.local (192.0.2.10)

Set the operator password:
  [_________________________________]
  [_________________________________]   (confirm)

  [ Continue → ]
```

After setting, the webapp BFF generates a hub API token (via the hub's
CLI inside the container, or an out-of-band manual step for v1), stores
it, and the operator never sees it again.

### C.3 Recent hub capabilities to surface in the UI

#### a) `ml_eid` self-refresh — replaces stale-address troubleshooting

The hub now auto-refreshes each device's stored ML-EID every 5 s via
a new D2G `DEVICE_THREAD_STATE` frame from the gateway.  The
`provision_info.coap_addr` field is **renamed `ml_eid`** in the REST
API response.

Designer impact:
- The "device offline / mesh stale" status badge can now be much
  simpler.  Pre-refresh, an operator sometimes saw "ml_eid stale,
  click to refresh" — that affordance is gone.
- The device-detail page should show the current `ml_eid` as
  read-only metadata (under "Mesh address"), not as an editable
  field.
- REST API field name in JSON responses: `ml_eid` (was `coap_addr`).
  POST `/api/v1/devices/:id/provision` accepts both keys during the
  transition window — designer shouldn't need to worry about this.

#### b) Automation off-rules — paired hot/cool support

Automations now correctly encode `value: off` / `value: false` /
`value: 0` etc. as 0.0 (previously they all silently became 1.0).
This unlocks the natural pattern of paired triggers:

```yaml
- id: room_hot
  trigger: [{device: temp-1, field: temperature, above: 25}]
  action:  [{device: fan, field: power, value: on}]
- id: room_cool
  trigger: [{device: temp-1, field: temperature, below: 25}]
  action:  [{device: fan, field: power, value: off}]
```

Designer impact:
- The visual automation builder (if pursued — see open question §10.6
  of the main brief) needs to surface **paired rules** as a first-class
  concept: a single "When temp > 25 turn fan ON / when temp < 25 turn
  fan OFF" card should compile to two YAML rules under the hood.
- The YAML editor should validate `value:` field — accepted aliases:
  `on`/`off`/`true`/`false`/`yes`/`no`/`0`/`1` or any number.  Anything
  else is a compile error.

### C.4 Visual style references

The original brief said "Branding / theme — none currently. Bring your
own."  Refined direction:

**Reference apps to study:**
1. **Home Assistant** (https://www.home-assistant.io/) — card-based
   dashboards grouped by area, big toggles, status pills, dark theme
   default.  Closest match to the device-card-grid layout in §9.
   Borrow: card grid responsive breakpoints, toggle pill component,
   "Areas" → our "Gateways" mental model.
2. **Amazon Alexa app** (mobile) — bottom-nav, big icons, voice-y
   copy, "Routines" tab pattern.  Borrow: bottom-nav for mobile
   breakpoint, "Routines" naming for the Automations tab if the
   YAML-first brand feels too technical for non-developer operators.
3. **Apple Home** — minimalist, room-tabs, dimmable accent colors.
   Borrow: emphasis on a single hero metric per device card
   (temperature OR LED state, not both at once).

**Concrete style cues:**
- Default theme: dark, OLED-friendly (#000 or #0a0a0a background).
- Accent: a single brand color used for state-on / active CTA only —
  default to `oklch(70% 0.18 250)` (a calm electric blue) but bring
  your own.
- Status colors: green (≤ 30 s last-seen), amber (≤ 5 min), red (> 5
  min OR explicitly offline).  Use icons + color for accessibility.
- Typography: system font stack first (`ui-sans-serif`, `system-ui`,
  …).  No web-fonts for the dashboard; load fonts only on settings /
  docs pages.
- Iconography: Lucide or Phosphor.  No emojis in the UI chrome.
- Animation: prefer none; tasteful 150 ms ease-out on toggle state
  changes only.

### C.5 New screens not in the main brief

- **Login** (§C.2) — username/password, "remember on this device"
  checkbox writes a 30-day cookie.
- **First-run setup** (§C.2) — set password, optionally connect to
  hub host if not auto-discovered.
- **Mesh map** (optional, stretch) — a graph view of the Thread
  network: leader at center, routers ringed around, end-devices as
  leaves.  Pulls from `GET /api/v1/gateways` + per-device `ml_eid`s.
  Inspired by Home Assistant's Zigbee2MQTT integration map.

### C.6 Open questions added since the original brief

8. **BFF or direct browser → hub?** The brief implicitly assumed
   browser → hub direct.  Now that we're in a sister LXC, a BFF lets
   us hold the bearer token securely.  Pick BFF unless you have a
   strong reason not to.
9. **State streaming** — re-asking question §10.2 now that we have a
   BFF: should the BFF maintain a long-lived poll loop and push to
   the browser via SSE / WebSocket?  Operator-glance ("did the LED
   just flicker?") would benefit; provisioning flows don't need it.
10. **Mobile-first or desktop-first**?  §10.3 was open; refined
    answer: **mobile-first** for the dashboard + device-control
    surfaces, **desktop-first** for the YAML automation editor and
    the factory tab.  Two breakpoints, no in-between optimisation.
11. **Live-temperature display** — c6-sN sensors emit a sinewave
    temperature every 5 s (lab default; real deployments would
    replace the simulator with a real probe).  Should the device-detail
    page render a sparkline?  Browser-side downsampling of 5-second
    samples → 1 hour ≈ 720 points, fine for canvas.

### C.7 Deliverables format for the Claude Design agent

When importing this brief into Claude Design:

1. Feed the **entire `ui-design-brief.md` file** as system context
   (the main brief + this addendum).
2. Ask for outputs in this order:
   a. A site map / route table reconciling §9 with §C.5.
   b. A component inventory updating §9's list with login,
      first-run, and mesh-map components.
   c. Wireframes for the dashboard (mobile + desktop), device-detail
      page (with sparkline if §C.6.11 is yes), and the paired-rule
      automation card (§C.3.b).
   d. A theme spec (color tokens, typography scale, spacing scale)
      conforming to §C.4.
   e. An interaction spec for the toggle component (the most used
      primitive — `led on/off`, `fan on/off`, etc.) covering loading,
      success, failure, and the H2D-wedge case (request times out
      with no D2H back — see §7 of the main brief).

3. Hard constraints the designer must respect:
   - No assumption of a cloud component.  Everything is LAN-local.
   - No assumption of multi-user / RBAC for v1.
   - No reliance on push notifications; we have no mobile app yet.
   - The hub's REST API as documented in §6 + Appendix A is the
     contract — don't propose endpoints that don't exist; mark UI
     features that need new endpoints as "🆕 needs API work".

