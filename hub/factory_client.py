"""
factory_client — async HTTP client for the nn factory service.

Usage:
    client = FactoryClient("http://localhost:8100")
    result = await client.build("apps/mdns_ot_esp32c6", version="0.1.0")
"""

from __future__ import annotations

import aiohttp


class FactoryClient:
    def __init__(self, base_url: str = "http://localhost:8100"):
        self.base_url = base_url.rstrip("/")

    async def health(self) -> bool:
        """Check if the factory service is reachable."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}/health", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    return resp.status == 200
        except (aiohttp.ClientError, OSError):
            return False

    async def status(self) -> dict:
        """Get current factory status."""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/status") as resp:
                return await resp.json()

    async def build(self, app: str, board: str = "", version: str = "") -> dict:
        """Trigger a firmware build.

        Returns dict with keys: status, version, device_type, size, sha256, path
        On error: status="error", message=...
        """
        payload = {"app": app}
        if board:
            payload["board"] = board
        if version:
            payload["version"] = version

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}/build",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=600),
            ) as resp:
                return await resp.json()
