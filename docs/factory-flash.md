# Factory-Flash Bundle for Sensors + Gateway NCPs

Goal: a manufacturer flashes one signed binary onto each fresh ESP32-C6
chip before shipping. End users never need to JTAG or USB-flash; all
subsequent firmware updates land via the hub's OTA REST endpoint
(`POST /api/v1/devices/{dev}/ota`).

## What makes the current build OTA-updatable from a cold start

The MCUboot warm-reset hang on ESP32-C6 was the only previous blocker.
That patch lives in the **app's** Zephyr SoC code, not MCUboot itself:

```
device/third_party/zephyr/soc/espressif/esp32c6/soc.c
   sys_arch_reboot:
     - removed: esp_restart() / esp_rom_software_reset_cpu(0)  (CPU-only, 0x0C)
     + added:   esp_rom_software_reset_system()                (digital-core, 0x03)
```

Because every app built from `main` includes this patched SoC layer, any
firmware shipped as the factory image is OTA-updatable on first boot —
the app calls `sys_reboot()` during OTA apply, the patched SoC code does
the right reset, MCUboot completes the swap, the new slot 0 boots, no
operator cold-cycle needed.

This was verified by 10 consecutive `mesh ota apply` iterations on c6-s3
(zero hangs) and several full hub-initiated OTAs on c6-s1, c6-s2, c6-s3.

## Factory bundle contents

For each chip type, the factory needs one **merged** binary:

| Region | Source | Notes |
|---|---|---|
| Bootloader (MCUboot) | `build/<app>/zephyr/zephyr.signed.bin`'s bootloader slot | Same MCUboot for all apps |
| Slot 0 (app v1.0.0) | `build/<app>/zephyr/zephyr.signed.bin` | App's first shipping version |
| Slot 1 | (empty / 0xFF) | Will hold downloaded OTA images |
| NVS | (empty) | Forces fresh key gen + BLE pairing on first boot |
| Storage partition | (empty) | OT dataset, hub pubkey written during BLE pair |

For ESP32-C6, esptool produces this as one `.bin` for flashing at
`0x0`. The existing `device/scripts/flash_esp32c6.sh` already builds
this bundle; the factory recipe just calls it with `--erase` to ensure
NVS starts blank.

## Factory recipe

For each sensor chip:

```bash
# 1. Build the shipping app once per release
cd device
./scripts/build.sh --app ble_ot_esp32c6 --version 1.0.0 --board esp32c6_devkitc

# 2. Per-chip flash (idempotent; --erase wipes any previous identity)
./scripts/flash_esp32c6.sh --port /dev/ttyACM0 --erase

# 3. Smoke-test: confirm chip boots and advertises BLE provisioning service
timeout 30 bluetoothctl scan le | grep -q nn-prov-XXXX && echo OK || echo FAIL
```

For NCP chips (gateways), substitute `ncp_esp32c6` for `ble_ot_esp32c6`.

## Verification (factory acceptance test)

A chip that just came off the factory line should pass:

1. **Boots cleanly** — UART banner shows `Booting Zephyr OS build vX.Y.Z`.
2. **Advertises BLE service** — `e7f01001-…` UUID visible to a BLE
   scanner within 5 s of boot.
3. **Accepts pairing** — `nn-provisioner device new --addr <mac>` (or
   `POST /api/v1/devices/new` if the hub has BLE access) completes
   without error.
4. **Joins Thread network** — sensor's UART or `GET /api/v1/devices/{id}`
   shows last_seen updating within 30 s.
5. **OTA round-trip** — upload a v1.0.1 firmware target via
   `POST /api/v1/firmware`, trigger `POST /api/v1/devices/{id}/ota`,
   confirm the device reports v1.0.1 within 60 s and is `Confirmed: yes`.

Step 5 is the gate that proves the patched MCUboot path works on a
brand-new chip. **This is already passing on our dev chips** (c6-s1,
c6-s2, c6-s3 were all JTAG-flashed exactly once and have been OTA'd
multiple times). The factory test just formalizes it.

## Per-chip uniqueness

The factory image is byte-identical for every chip. Uniqueness comes
from first-boot:

- `nn_proto` P-256 + X25519 keypairs auto-generated and persisted to NVS
  on first boot (no factory step needed).
- Device ID is `sha256(P-256 pub)[:8]` — uniquely derived, no factory
  injection.
- Optional: pre-generate factory keypairs and burn them via efuse for
  attestation. Not in v1.

## What gets shipped, what gets discarded

| Artifact | Ships? |
|---|---|
| Bootloader binary | Yes |
| App slot 0 (v1.0.0 signed) | Yes |
| Signing private key | **No** — stays at factory |
| App signing public key | Embedded in MCUboot, ships |
| Hub pubkey | **No** — written via BLE at end-user pairing |
| Thread dataset | **No** — same |

## OTA path constraints

For an OTA to succeed on a factory-flashed chip:

- New image must be signed by the same `MCUBOOT_SIGNATURE_KEY_FILE` used
  for the slot-0 image. Don't lose this key.
- Slot 1 must be big enough for the new image. Today it equals slot 0,
  so any v1.X.Y can replace v1.0.0 as long as the app fits the partition.
- Hub must already have the chip's pubkey in its `devices` table
  (delivered via BLE pairing). Until pairing completes, the chip can't
  decrypt H2D OTA_HINT frames.

## Reflash recovery

If a chip's NVS becomes corrupt (rare; usually requires a power
brown-out mid-write):

- End user runs `nn-provisioner device new --addr <ble-mac>` again.
- The chip's regenerated pubkey shows up at `GET /api/v1/devices/pending`
  on the hub.
- Hub's 404 response on `GET /api/v1/devices/{old-id}` carries a `hint`
  pointing the operator at the recovery flow.

End user never needs JTAG/UART.
