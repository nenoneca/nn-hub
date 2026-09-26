"""Tests for hub.firmware_sources.GitHubReleasesSource.

Uses an in-process aiohttp.test_utils stub server so we don't hit GitHub.
"""

import asyncio
import hashlib
import json
from pathlib import Path

import pytest
from aiohttp import web

from hub.firmware_sources import GitHubReleasesSource
from hub.firmware_sources.base import MCUBOOT_IMAGE_MAGIC, ManifestError


pytestmark = pytest.mark.asyncio


def _make_image_bytes() -> bytes:
    return MCUBOOT_IMAGE_MAGIC + b"payload-" * 32


async def _start_stub_github(releases: list[dict],
                              assets: dict[str, bytes]) -> tuple[web.AppRunner, str]:
    """Stand up a tiny aiohttp app that mimics the GitHub releases API.

    `releases` is the list returned from /repos/{repo}/releases.
    `assets` maps asset URL path → raw bytes.
    """
    app = web.Application()

    async def list_releases(request):
        return web.json_response(releases)

    async def serve_asset(request):
        key = request.path
        if key not in assets:
            return web.Response(status=404)
        return web.Response(body=assets[key], content_type="application/octet-stream")

    app.router.add_get("/repos/{owner}/{name}/releases", list_releases)
    app.router.add_get("/asset/{name}", serve_asset)
    app.router.add_get("/asset/{name}/{tag}", serve_asset)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="127.0.0.1", port=0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"http://127.0.0.1:{port}"


async def test_list_available_parses_releases_and_fetches_manifests(tmp_path: Path):
    img = _make_image_bytes()
    sha = hashlib.sha256(img).hexdigest()
    manifest = json.dumps({
        "schema": 1,
        "device_type": "sample_c6",
        "version": "2.13.0",
        "sha256": sha,
        "size_bytes": len(img),
        "mcuboot": {"image_magic": "0x96f3b83d"},
    }).encode()

    releases = [{
        "tag_name": "v2.13.0",
        "draft": False,
        "assets": [
            {"name": "image.signed.bin", "url": "PLACEHOLDER_BIN"},
            {"name": "manifest.json",    "url": "PLACEHOLDER_MANIFEST"},
        ],
    }]
    # rewrite asset URLs once the stub server is up.
    runner, base = await _start_stub_github(
        releases=releases,
        assets={"/asset/image.signed.bin": img,
                "/asset/manifest.json":    manifest},
    )
    try:
        releases[0]["assets"][0]["url"] = f"{base}/asset/image.signed.bin"
        releases[0]["assets"][1]["url"] = f"{base}/asset/manifest.json"

        src = GitHubReleasesSource(
            name="prod",
            repo="chalos/nn-fw-sample-c6",
            api_base=base,
        )
        images = await src.list_available()
        assert len(images) == 1
        img_desc = images[0]
        assert img_desc.device_type == "sample_c6"
        assert img_desc.version == "2.13.0"
        assert img_desc.sha256 == sha

        cache = tmp_path / "img.bin"
        path = await src.fetch(img_desc, cache)
        assert path.read_bytes() == img
    finally:
        await runner.cleanup()


async def test_list_available_skips_drafts_and_unmatched_tags(tmp_path: Path):
    img = _make_image_bytes()
    sha = hashlib.sha256(img).hexdigest()
    manifest = json.dumps({
        "device_type": "sample_c6", "version": "2.13.0",
        "sha256": sha, "size_bytes": len(img),
    }).encode()

    releases = [
        # draft — skip
        {"tag_name": "v2.14.0", "draft": True, "assets": []},
        # bad tag shape — skip
        {"tag_name": "release-x", "draft": False, "assets": []},
        # the only valid one
        {"tag_name": "v2.13.0", "draft": False,
         "assets": [
             {"name": "image.signed.bin", "url": "PLACEHOLDER_BIN"},
             {"name": "manifest.json",    "url": "PLACEHOLDER_MAN"},
         ]},
    ]
    runner, base = await _start_stub_github(
        releases=releases,
        assets={"/asset/image.signed.bin": img,
                "/asset/manifest.json":    manifest},
    )
    try:
        releases[2]["assets"][0]["url"] = f"{base}/asset/image.signed.bin"
        releases[2]["assets"][1]["url"] = f"{base}/asset/manifest.json"

        src = GitHubReleasesSource(
            name="prod",
            repo="chalos/nn-fw-sample-c6",
            api_base=base,
        )
        images = await src.list_available()
        assert len(images) == 1
        assert images[0].version == "2.13.0"
    finally:
        await runner.cleanup()


async def test_channels_filter(tmp_path: Path):
    img = _make_image_bytes()
    sha = hashlib.sha256(img).hexdigest()

    def manifest_for(version: str) -> bytes:
        return json.dumps({
            "device_type": "sample_c6", "version": version,
            "sha256": sha, "size_bytes": len(img),
        }).encode()

    releases = [
        {"tag_name": "v2.13.0",        "draft": False,
         "assets": [
             {"name": "image.signed.bin", "url": "PLACEHOLDER_BIN"},
             {"name": "manifest.json",    "url": "PLACEHOLDER_STABLE"},
         ]},
        {"tag_name": "v2.14.0-beta.1", "draft": False,
         "assets": [
             {"name": "image.signed.bin", "url": "PLACEHOLDER_BIN"},
             {"name": "manifest.json",    "url": "PLACEHOLDER_BETA"},
         ]},
    ]
    runner, base = await _start_stub_github(
        releases=releases,
        assets={"/asset/image.signed.bin":  img,
                "/asset/manifest_stable":   manifest_for("2.13.0"),
                "/asset/manifest_beta":     manifest_for("2.14.0-beta.1")},
    )
    try:
        releases[0]["assets"][0]["url"] = f"{base}/asset/image.signed.bin"
        releases[0]["assets"][1]["url"] = f"{base}/asset/manifest_stable"
        releases[1]["assets"][0]["url"] = f"{base}/asset/image.signed.bin"
        releases[1]["assets"][1]["url"] = f"{base}/asset/manifest_beta"

        # No channel filter — both included.
        src_all = GitHubReleasesSource("prod", "chalos/nn-fw-sample-c6",
                                        api_base=base)
        assert len(await src_all.list_available()) == 2

        # stable-only.
        src_stable = GitHubReleasesSource("prod", "chalos/nn-fw-sample-c6",
                                           channels=["stable"], api_base=base)
        rs = await src_stable.list_available()
        assert len(rs) == 1
        assert rs[0].version == "2.13.0"
    finally:
        await runner.cleanup()


async def test_404_repo_returns_empty_not_raise(tmp_path: Path):
    async def list_releases(request):
        return web.Response(status=404)
    app = web.Application()
    app.router.add_get("/repos/{owner}/{name}/releases", list_releases)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="127.0.0.1", port=0)
    await site.start()
    base = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
    try:
        src = GitHubReleasesSource("prod", "chalos/nope", api_base=base)
        assert await src.list_available() == []
    finally:
        await runner.cleanup()


async def test_parse_tag():
    p = GitHubReleasesSource._parse_tag
    assert p("v2.13.0") == ("2.13.0", "stable")
    assert p("v2.14.0-beta.1") == ("2.14.0-beta.1", "beta")
    assert p("v2.14.0-rc.2")   == ("2.14.0-rc.2",   "rc")
    assert p("release-x")      is None
    assert p("not-a-tag")      is None
