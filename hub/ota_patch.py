"""
ota_patch — binary-delta OTA patch generation (detools / heatshrink).

The hub has every firmware version cached in the catalog, so it can
compute an optimal binary delta from a device's *running* image to the
*target* image and ship a tiny patch instead of the whole image.

Measured on real sample_c6 binaries: a real code-change update that is
~1 MB as a full image (and ~92% of blocks under fixed-block chunk-diff)
becomes a ~35 KB heatshrink patch — a ~29x reduction.

Patches are generated lazily and cached as a sidecar next to the target
image:  <target_image>.from-<from_version>.patch

Compression: heatshrink (window=8, lookahead=7) — the same decoder the
device vendors.  It needs ~0.5 KB RAM on the device, vs LZMA's 64 KB+
(which doesn't fit the C6).
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# Must match the device-side heatshrink_config.h.
HEATSHRINK_WINDOW_SZ2 = 8
HEATSHRINK_LOOKAHEAD_SZ2 = 7


def _norm(version: str) -> str:
    """Strip a Zephyr +tweak suffix so '0.0.2+0' keys the same as '0.0.2'."""
    return version.split("+", 1)[0]


def patch_sidecar_path(target_image: Path, from_version: str) -> Path:
    return target_image.with_suffix(
        target_image.suffix + f".from-{_norm(from_version)}.patch")


def generate_patch(from_image: Path, to_image: Path,
                   out_patch: Path) -> Optional[dict]:
    """Create a heatshrink detools patch from_image → to_image at
    out_patch (atomic).  Returns {size_bytes, sha256} or None if detools
    isn't available.  Raises on a real generation/verification failure."""
    try:
        import detools
    except ImportError:
        log.warning("detools not installed — patch generation unavailable")
        return None

    out_patch.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_patch.with_suffix(out_patch.suffix + ".tmp")
    with open(from_image, "rb") as ff, open(to_image, "rb") as tf, \
            open(tmp, "wb") as pf:
        detools.create_patch(
            ff, tf, pf,
            compression="heatshrink",
            # Sequential patch type → streamable apply on the device
            # (no random seeks into the patch itself).
            patch_type="sequential",
        )

    # Verify the patch reconstructs the target byte-for-byte before we
    # publish it.  A bad patch would otherwise only fail on the device
    # after a full transfer + flash cycle.
    recon = out_patch.with_suffix(out_patch.suffix + ".verify")
    try:
        with open(from_image, "rb") as ff, open(tmp, "rb") as pf, \
                open(recon, "wb") as of:
            detools.apply_patch(ff, pf, of)
        if recon.read_bytes() != to_image.read_bytes():
            raise ValueError("patch round-trip did not reproduce target")
    finally:
        recon.unlink(missing_ok=True)

    data = tmp.read_bytes()
    tmp.replace(out_patch)
    info = {
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    log.info("generated patch %s (%d B, %.1f%% of target)",
             out_patch.name, len(data),
             100.0 * len(data) / max(1, to_image.stat().st_size))
    return info


def ensure_patch(db, cache_dir: Path, device_type: str,
                 from_version: str, target_image: Path) -> Optional[dict]:
    """Resolve from_version's cached image via the catalog and produce
    (or reuse) a patch to target_image.  Returns
    {path, size_bytes, sha256} or None when no from-image is cached
    (device on an unknown build → caller falls back to full/chunk-diff).
    """
    from_version = _norm(from_version)
    sidecar = patch_sidecar_path(target_image, from_version)
    sha_sidecar = sidecar.with_suffix(sidecar.suffix + ".sha")

    if sidecar.is_file() and sha_sidecar.is_file():
        return {
            "path": sidecar,
            "size_bytes": sidecar.stat().st_size,
            "sha256": sha_sidecar.read_text().strip(),
        }

    # Find a cached image for the from-version.
    entry = db.get_catalog_entry(device_type, from_version)
    if entry is None or not entry.is_cached or not entry.local_path:
        log.info("no cached image for %s/%s — cannot build patch",
                 device_type, from_version)
        return None
    from_image = Path(entry.local_path)
    if not from_image.is_file():
        return None

    info = generate_patch(from_image, target_image, sidecar)
    if info is None:
        return None
    sha_sidecar.write_text(info["sha256"])
    return {"path": sidecar, **info}
