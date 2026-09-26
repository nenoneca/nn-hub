"""WS-ticket auth contract.

The reverse proxy exempts WebSocket paths from basic auth (browsers
never attach saved HTTP credentials to a socket handshake), so the hub
gates them with tickets instead.  The contract these tests pin down:

- a socket without a ticket is CHALLENGED (401) — auth happens once,
  at the authenticated /api/v1/ws-ticket mint;
- a minted ticket is REUSABLE for any number of sockets;
- reuse ends when the ticket EXPIRES (server TTL);
- reuse ends on HUB RESTART (the store is in-memory by design).
"""

import time
from pathlib import Path

import pytest
import pytest_asyncio
from aiohttp import WSServerHandshakeError
from aiohttp.test_utils import TestClient, TestServer

import hub.api as api_mod
from hub.api import make_app
from hub.db import DB
from hub.log_store import LogStore

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def client(tmp_path: Path):
    db = DB(tmp_path / "h.db")
    log_store = LogStore(tmp_path / "logs", rotate_interval_sec=3600)
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    app = make_app(db, log_store, X25519PrivateKey.generate())
    c = TestClient(TestServer(app))
    await c.start_server()
    # each test starts from a clean ticket store
    api_mod._WS_TICKETS.clear()
    yield c
    await c.close()


async def _connect(client, ticket=None):
    """Try the fleet socket; returns the handshake status code."""
    url = "/api/v1/fleet/ws" + (f"?ticket={ticket}" if ticket else "")
    try:
        ws = await client.ws_connect(url)
    except WSServerHandshakeError as e:
        return e.status
    # No proto_router in the test app: the socket opens (ticket accepted),
    # reports the error in-band and closes — that still counts as 101.
    await ws.close()
    return 101


async def test_no_ticket_is_challenged(client):
    assert await _connect(client) == 401


async def test_garbage_ticket_is_challenged(client):
    assert await _connect(client, "not-a-real-ticket") == 401


async def test_ticket_minted_once_reused_many_times(client):
    r = await client.get("/api/v1/ws-ticket")
    assert r.status == 200
    j = await r.json()
    ticket, ttl = j["ticket"], j["ttl"]
    assert ttl > 0
    # one mint, many sockets — reuse must NOT consume the ticket
    for _ in range(5):
        assert await _connect(client, ticket) == 101


async def test_ticket_expires(client):
    r = await client.get("/api/v1/ws-ticket")
    ticket = (await r.json())["ticket"]
    assert await _connect(client, ticket) == 101
    # age it past the TTL server-side (no sleeping in tests)
    api_mod._WS_TICKETS[ticket] = time.time() - 1
    assert await _connect(client, ticket) == 401
    # an expired ticket is also dropped from the store on sight
    assert ticket not in api_mod._WS_TICKETS


async def test_hub_restart_invalidates(client):
    r = await client.get("/api/v1/ws-ticket")
    ticket = (await r.json())["ticket"]
    assert await _connect(client, ticket) == 101
    # a restart clears the in-memory store — same effect as clear()
    api_mod._WS_TICKETS.clear()
    assert await _connect(client, ticket) == 401
    # ...and a fresh mint works again afterwards
    r = await client.get("/api/v1/ws-ticket")
    assert await _connect(client, (await r.json())["ticket"]) == 101
