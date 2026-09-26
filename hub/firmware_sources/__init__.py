"""
firmware_sources — pluggable image providers for the factory layer.

The hub doesn't build firmware itself (build.sh is a separate path used in
dev).  At runtime it discovers ready-to-deploy MCUboot-signed images from
configured sources and serves the operator's chosen target via OTA.

Each backend implements FirmwareSource.  Today: LocalDirSource (filesystem)
and GitHubReleasesSource (GitHub releases).  Future: S3Source, MinioSource.
"""

from .base import FirmwareSource, ImageDescriptor, Manifest, ManifestError
from .local import LocalDirSource
from .github import GitHubReleasesSource

__all__ = [
    "FirmwareSource",
    "ImageDescriptor",
    "Manifest",
    "ManifestError",
    "LocalDirSource",
    "GitHubReleasesSource",
    "build_source",
]


def build_source(spec: dict) -> FirmwareSource:
    """Instantiate a source from a sources.yaml entry.

    Expected shape:
      {name: str, kind: "local"|"github", ...kind-specific...}
    """
    name = spec["name"]
    kind = spec["kind"]
    if kind == "local":
        return LocalDirSource(
            name=name,
            root=spec["root"],
        )
    if kind == "github":
        return GitHubReleasesSource(
            name=name,
            repo=spec["repo"],
            pat_env=spec.get("pat_env"),
            channels=spec.get("channels"),
        )
    raise ValueError(f"unknown firmware source kind: {kind!r}")
