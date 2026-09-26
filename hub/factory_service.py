"""
factory_service — standalone HTTP build server for nn device firmware.

Wraps the device repo's build.sh + MCUboot signing into a simple HTTP API.
Runs as a standalone aiohttp server on localhost:8100.

Endpoints:
  POST /build   — trigger a firmware build
  GET  /status  — current build state
  GET  /health  — liveness check
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import shutil
import time
from pathlib import Path

import yaml
from aiohttp import web

log = logging.getLogger(__name__)

BUILD_SCRIPT = "scripts/build.sh"


class FactoryService:
    def __init__(self, device_repo: str | Path, output_dir: str | Path | None = None):
        self.device_repo = Path(device_repo).resolve()
        self.output_dir = Path(output_dir).resolve() if output_dir else self.device_repo / "out"
        self._lock = asyncio.Lock()
        self._status: dict = {"status": "idle"}

        if not (self.device_repo / BUILD_SCRIPT).exists():
            raise FileNotFoundError(
                f"build.sh not found at {self.device_repo / BUILD_SCRIPT}"
            )

    def _load_build_yaml(self, app: str) -> dict:
        """Load .nn-build.yaml from the app directory if it exists."""
        path = self.device_repo / app / ".nn-build.yaml"
        if path.exists():
            return yaml.safe_load(path.read_text()) or {}
        return {}

    def _find_signed_bins(self, app: str, board: str) -> dict[str, Path]:
        """Locate signed binaries in the build output.

        Returns a dict with keys "default", and optionally "slot0", "slot1"
        for Direct XIP builds.
        """
        app_name = Path(app).name
        board_slug = board.replace("/", "_")
        build_base = self.output_dir / app_name / board_slug
        result = {}

        # Main app image (slot0 for Direct XIP, or the only image for swap/overwrite)
        base = build_base / app_name / "zephyr"
        for name in ["zephyr.signed.bin", "zephyr.bin"]:
            p = base / name
            if p.exists():
                result["default"] = p
                result["slot0"] = p
                break

        # Direct XIP: slot1 variant
        slot1 = build_base / f"{app_name}_slot1_variant" / "zephyr" / "zephyr.signed.bin"
        if slot1.exists():
            result["slot1"] = slot1

        return result

    async def _do_build(self, app: str, board: str, version: str | None) -> dict:
        """Run build.sh and return result metadata."""
        # Clean previous build so version updates take effect
        app_name = Path(app).name
        board_slug = board.replace("/", "_")
        build_dir = self.output_dir / app_name / board_slug
        if build_dir.exists():
            shutil.rmtree(build_dir)

        cmd = [
            "bash", str(self.device_repo / BUILD_SCRIPT),
            "--app", app,
            "--board", board,
        ]
        if version:
            cmd += ["--version", version]

        log.info("Build: %s", " ".join(cmd))
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(self.device_repo),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await proc.communicate()
        build_log = stdout.decode(errors="replace")

        if proc.returncode != 0:
            log.error("Build failed (rc=%d):\n%s", proc.returncode, build_log[-2000:])
            return {
                "status": "error",
                "message": f"build failed (rc={proc.returncode})",
                "log": build_log[-2000:],
            }

        bins = self._find_signed_bins(app, board)
        if not bins:
            return {
                "status": "error",
                "message": "build succeeded but signed binary not found",
            }

        build_yaml = self._load_build_yaml(app)
        device_type = build_yaml.get("device_type", Path(app).name)
        result_version = version or "0.0.0"

        # Use the default (slot0) image for metadata
        default_bin = bins.get("default") or bins.get("slot0")
        data = default_bin.read_bytes()
        sha = hashlib.sha256(data).hexdigest()

        # Report all available variants
        paths = {k: str(v) for k, v in bins.items()}

        return {
            "status": "ok",
            "version": result_version,
            "device_type": device_type,
            "size": len(data),
            "sha256": sha,
            "path": str(default_bin),
            "paths": paths,
        }

    async def handle_build(self, request: web.Request) -> web.Response:
        body = await request.json()
        app = body.get("app", "")
        board = body.get("board", "")
        version = body.get("version")

        if not app:
            return web.json_response(
                {"status": "error", "message": "missing 'app' field"}, status=400
            )

        # Auto-detect board from .nn-build.yaml if not provided
        if not board:
            build_yaml = self._load_build_yaml(app)
            board = build_yaml.get("board", "")
            if not board:
                return web.json_response(
                    {"status": "error",
                     "message": "missing 'board' field and no .nn-build.yaml"},
                    status=400,
                )

        if not self._lock.locked():
            async with self._lock:
                self._status = {
                    "status": "building",
                    "app": app,
                    "board": board,
                    "started": int(time.time()),
                }
                try:
                    result = await self._do_build(app, board, version)
                finally:
                    self._status = {"status": "idle"}
                return web.json_response(result)
        else:
            return web.json_response(
                {"status": "error", "message": "build already in progress"},
                status=409,
            )

    async def handle_status(self, request: web.Request) -> web.Response:
        return web.json_response(self._status)

    async def handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    def create_app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/build", self.handle_build)
        app.router.add_get("/status", self.handle_status)
        app.router.add_get("/health", self.handle_health)
        return app


def run(device_repo: str, host: str = "127.0.0.1", port: int = 8100,
        output_dir: str | None = None):
    """Start the factory service (blocking)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    svc = FactoryService(device_repo, output_dir)
    app = svc.create_app()
    log.info("Factory service: http://%s:%d  device_repo=%s", host, port, device_repo)
    web.run_app(app, host=host, port=port, print=lambda _: None)
