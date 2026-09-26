"""Thin REST client for talking to an nn-hub from the provisioner.

Wraps the JSON endpoints the provisioner depends on:

  GET  /hub/identity      → hub_id + P-256 pub + X25519 pub
  GET  /network           → Thread dataset TLVs (hex)
  POST /devices           → register a device pubkey
  POST /gateways          → register a gateway pubkey

The standalone `nn-provisioner device new` / `gateway new` flows pull
the hub identity + dataset from the hub, do the BLE GATT writes to the
device, then POST the resulting pubkey back to the hub.
"""
from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Optional


class HubClientError(RuntimeError):
    pass


@dataclass
class HubIdentity:
    hub_id:     bytes  # 8 B
    p256_pub:   bytes  # 65 B (uncompressed)
    x25519_pub: bytes  # 32 B


class HubClient:
    """Synchronous JSON client.  Provisioner flows are short-lived and
    not in the hot path, so we use blocking urllib rather than pulling
    in another async stack just for these few calls."""

    def __init__(self, base_url: str, token: Optional[str] = None,
                 timeout: float = 15.0):
        # Strip trailing slash so we can always concat with "/...".
        self._base    = base_url.rstrip("/")
        self._token   = token
        self._timeout = timeout

    # ── helpers ─────────────────────────────────────────────────────

    def _request(self, method: str, path: str,
                 body: Optional[dict] = None) -> dict:
        url = self._base + path
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if self._token:
            req.add_header("Authorization", f"Bearer {self._token}")
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as r:
                raw = r.read()
        except urllib.error.HTTPError as e:
            try:
                body = json.loads(e.read().decode())
            except Exception:
                body = {}
            raise HubClientError(
                f"{method} {path} → HTTP {e.code}: {body.get('err', e.reason)}")
        except urllib.error.URLError as e:
            raise HubClientError(f"{method} {path}: {e.reason}")
        try:
            return json.loads(raw.decode()) if raw else {}
        except Exception as e:
            raise HubClientError(f"{method} {path}: bad JSON: {e}")

    # ── identity / network ──────────────────────────────────────────

    def get_identity(self) -> HubIdentity:
        d = self._request("GET", "/hub/identity")
        try:
            return HubIdentity(
                hub_id     = bytes.fromhex(d["hub_id"]),
                p256_pub   = bytes.fromhex(d["p256_pub"]),
                x25519_pub = bytes.fromhex(d["x25519_pub"]),
            )
        except (KeyError, ValueError) as e:
            raise HubClientError(f"malformed identity payload: {e}")

    def get_network(self) -> dict:
        """Returns the network record including `dataset_tlvs_hex` if
        a network has been minted, or `{"configured": false}` if not."""
        return self._request("GET", "/network")

    # ── registration ────────────────────────────────────────────────

    def register_device(self, id_hex: str, name: str, type_str: str,
                        pubkey_b64: str) -> dict:
        return self._request("POST", "/devices", body={
            "id":         id_hex,
            "name":       name,
            "type":       type_str,
            "pubkey_b64": pubkey_b64,
        })

    def register_gateway(self, id_hex: str, name: str,
                         pubkey_b64: str, mdns_addr: str = "") -> dict:
        return self._request("POST", "/gateways", body={
            "id":         id_hex,
            "name":       name,
            "pubkey_b64": pubkey_b64,
            "mdns_addr":  mdns_addr,
        })

    def network_init(self, force: bool = False) -> dict:
        return self._request("POST", "/network/init",
                             body={"force": force})
