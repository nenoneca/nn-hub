# Image Factory

The image factory is the hub's **provider / sync layer** for firmware images.
It does **not** build firmware itself. Builds happen upstream (CI, a dev
workstation, or — for now — the legacy `build.sh` flow). The factory's job
is to **discover** pre-built MCUboot-signed binaries from configured sources,
**cache** them locally, **verify** them, and **promote** an operator-chosen
image into the OTA-serving `firmware_targets` table.

The legacy `nn-hub factory start` build service still exists for dev use, but
it is treated as one more way to land an image in the local cache. The
operator-facing surface is the catalog.

## Architecture

```
   sources.yaml ──► FirmwareCatalog ──► firmware_catalog table
                          │
                          ├─► sync (boot + periodic poll + on-demand)
                          │
                          └─► promote(device_type, version)
                                ├─► ensure_cached() — download + verify
                                └─► firmware_targets row (OTA serves this)
```

Source backends today:

| Kind     | Use case                              | Implementation                                |
|----------|---------------------------------------|------------------------------------------------|
| `local`  | Self-hosted artifact dir, CI mount    | `hub/firmware_sources/local.py`                |
| `github` | Public or PAT-accessed GH releases    | `hub/firmware_sources/github.py`               |
| _future_ | S3 / MinIO / OCI registry            | _slot for future backends_                     |

## Configuration: `~/.nn-hub/sources.yaml`

```yaml
sources:
  - name: lab-local
    kind: local
    root: /srv/firmware
    poll_seconds: 0          # 0 = no polling (sync at boot + on demand)

  - name: prod-github
    kind: github
    repo: chalos/nn-fw-sample-c6
    pat_env: NN_HUB_GH_PAT   # optional; anonymous is fine for public repos
    poll_seconds: 300
    channels: [stable]       # optional release-channel filter
```

`name` must be unique. Missing `sources.yaml` is a quiet no-op — the hub
starts with zero sources and the operator can populate it later via CLI
or by hand-editing.

## Image conventions

Every image must publish exactly two artifacts at its source:

- `image.signed.bin` — the MCUboot-signed binary (must start with the
  MCUboot v1 magic `0x96f3b83d`)
- `manifest.json`    — sidecar describing the binary

### `manifest.json` schema (v1)

```json
{
  "schema":          1,
  "device_type":     "sample_c6",
  "version":         "2.13.0",
  "sha256":          "<64-char lowercase hex>",
  "size_bytes":      1028188,
  "mcuboot": {
    "image_magic":   "0x96f3b83d",
    "slot_size_bytes": 524288
  },
  "build_meta": {
    "git_sha":       "cafef00d",
    "built_at":      "2026-06-07T12:00:00Z",
    "tool_chain":    "zephyr 4.0 / esp-idf 5.3"
  },
  "signature_uri":   null,
  "signing_key_id":  null
}
```

Required: `device_type`, `version`, `sha256`, `size_bytes`. Everything else
is optional. Unknown fields are preserved verbatim in the catalog DB row
for forward compatibility.

The last two fields (`signature_uri`, `signing_key_id`) are **reserved for a
future detached-signature flow** (Option C in the design discussion).
Today the hub only verifies sha256 + MCUboot magic header — the
cryptographic trust check is performed by MCUboot on the device at swap
time, which is where it actually matters.

### Layout conventions

**Local FS**:
```
<root>/
  <device_type>/
    <version>/
      image.signed.bin
      manifest.json
```

**GitHub Releases** (one repo per device_type):
- Repo: `chalos/nn-fw-<device_type>` (the prefix is just convention — the
  manifest's `device_type` field is authoritative)
- Tag: `v<version>` — stable. `v<version>-<channel>.<n>` for pre-releases.
  e.g. `v2.13.0`, `v2.14.0-beta.1`, `v2.14.0-rc.2`.
- Assets per release: `image.signed.bin`, `manifest.json`. Other assets
  (release notes, debug symbols, …) are ignored.
- Drafts are skipped.
- The `channels:` filter, if present, keeps only releases whose channel
  matches (e.g. `[stable, beta]`). A release with no `-suffix` is `stable`.

## CLI

```
# Configure
nn-hub firmware source add lab-local --kind local --root /srv/firmware
nn-hub firmware source add prod-github --kind github \
    --repo chalos/nn-fw-sample-c6 --pat-env NN_HUB_GH_PAT \
    --poll-seconds 300 --channels stable

nn-hub firmware source list

# Discover
nn-hub firmware source sync lab-local

# Browse what's available
nn-hub firmware catalog
nn-hub firmware catalog --device-type sample_c6

# Activate one for OTA
nn-hub firmware promote sample_c6 2.13.0
nn-hub firmware promote sample_c6 2.13.0 --source prod-github
```

## REST API

```
GET  /api/v1/firmware/sources
    → [{name, kind, poll_seconds, last_sync_at, last_error}, ...]

POST /api/v1/firmware/sources/{name}/sync
    → {source, discovered: <count>}

GET  /api/v1/firmware/catalog                    (?device_type=...)
    → [{device_type, version, source_name, asset_uri, sha256, size_bytes,
        is_cached, local_path, downloaded_at, first_seen_at, last_seen_at}, ...]

POST /api/v1/firmware/catalog/{device_type}/{version}/promote
     body: optional {"source": "<source_name>"}
    → {device_type, version, path, size_bytes, sha256, source}
```

The legacy endpoints (`GET /api/v1/firmware`, `POST /api/v1/firmware`) are
unchanged: they serve `firmware_targets` (the active OTA target) and allow
direct binary upload as an escape hatch.

## DB schema

Two tables, two layers:

```sql
-- Layer 1: catalog of all discovered images, across all sources.
CREATE TABLE firmware_catalog (
    device_type    TEXT    NOT NULL,
    version        TEXT    NOT NULL,
    source_name    TEXT    NOT NULL,
    asset_uri      TEXT    NOT NULL,
    manifest_json  TEXT    NOT NULL DEFAULT '{}',
    sha256         TEXT    NOT NULL DEFAULT '',
    size_bytes     INTEGER NOT NULL DEFAULT 0,
    downloaded_at  INTEGER,            -- null until fetch + verify succeed
    local_path     TEXT,               -- null until cached locally
    first_seen_at  INTEGER NOT NULL,
    last_seen_at   INTEGER NOT NULL,
    PRIMARY KEY (device_type, version, source_name)
);

-- Layer 2: the single active OTA target per device_type.
CREATE TABLE firmware_targets (
    device_type    TEXT    PRIMARY KEY,
    target_version TEXT    NOT NULL,
    firmware_path  TEXT    NOT NULL,
    size_bytes     INTEGER NOT NULL DEFAULT 0,
    sha256         TEXT    NOT NULL DEFAULT '',
    updated_at     INTEGER NOT NULL
);
```

A single image may appear in multiple catalog rows (same `(device_type,
version)` from different sources). Promotion picks one: explicit
`source_name` if given, otherwise a cached row is preferred, otherwise
the most-recently-seen.

## Cache layout

```
~/.nn-hub/firmware-cache/
  <device_type>/
    <version>/
      image.<sha8>.signed.bin
```

The `sha8` in the filename means a re-promotion is a no-op when the
catalog row already points at a valid cached file. If the cached file is
ever found to disagree with the manifest's sha, the file is re-downloaded
on next `ensure_cached()`.

## Verification

At fetch time, the source backend:

1. Reads the bytes (over network for `github`, over filesystem for `local`)
2. Asserts `len(data) == manifest.size_bytes`
3. Asserts `sha256(data) == manifest.sha256`
4. Asserts `data[:4] == MCUBOOT_IMAGE_MAGIC` (`b"\x3d\xb8\xf3\x96"`)
5. Atomically writes to the cache dir (tmp + fsync + rename)

A failed check raises `ManifestError`; the catalog row is left as
"not cached" so a future promote can retry.

The MCUboot signature itself is **not** verified by the hub. The device's
bootloader checks it at swap time using the device-side root-of-trust key.
This keeps the hub a dumb pipe: a compromised hub can ship a *broken*
image (DoS via bad OTA) but cannot ship a *malicious* image — devices
reject unsigned / wrong-key images at boot.

## Migration path from the legacy build service

`nn-hub factory start` (the standalone build server at `localhost:8100`)
still works and is the path used by `device/build.sh`. It writes signed
binaries into the hub's old firmware directory and registers them directly
in `firmware_targets`, bypassing the catalog. This is fine for solo dev
and lets the legacy path keep working while the catalog is the way most
people interact with the hub.

The natural next step (not yet implemented) is to make the build service
write its output into a `local:` source directory instead of registering
directly. Then there's exactly one ingestion path and the catalog has
every image. Until that lands, the two paths co-exist; in case of a
conflict, the catalog wins because promotion overwrites `firmware_targets`.

## Open work

- **Hub-side detached-sig verification (Option C in the design discussion).**
  The manifest schema already reserves `signature_uri` + `signing_key_id`.
  Implementation: hub holds public keys of trusted release-signers, verifies
  before promoting. Useful when running multiple hubs in environments where
  the operator doesn't fully control the source.
- **Webhook-based GitHub sync.** Today GH polling is at `poll_seconds`
  cadence (300s by default for `prod-github`). A `POST /webhook/github`
  endpoint with HMAC verification would let GH push events trigger an
  immediate sync. Requires a publicly reachable hub or a tunnel.
- **Retention policy.** Cache grows unbounded. A simple "keep last N
  versions per device_type" sweep on boot would solve it.
- **Local FS notify.** `LocalDirSource` polls today. `inotify` /
  `fsevents` would catch new images instantly.
