"""A GATT server can apply a written value and still fail to answer.

The BeagleY Linux camera did exactly that on 2026-09-10: nn-setupd stored the
CONFIG value -- its own log said "CONFIG applied" -- and bluetoothd then
returned ATT 0x0e because the device's write handler deadlocked before sending
its D-Bus reply.  Aborting on that error discards a device that is in fact
configured, so the CONFIG write tolerates it exactly as the WIFI write already
did.  What must NOT be tolerated is silence: if neither write is acknowledged
and no status notification arrives, provisioning still fails.
"""
import asyncio
import pytest
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

from bleak.exc import BleakError
from hub import media_provisioner as mp

from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PublicKey  # noqa: E402
from cryptography.hazmat.primitives import serialization

STATUS_SUCCESS = mp.STATUS_SUCCESS
_DEVICE_PUB = X25519PrivateKey.generate().public_key().public_bytes(
    encoding=serialization.Encoding.Raw,
    format=serialization.PublicFormat.Raw)


class FakeClient:
    """Minimal BleakClient stand-in: records writes, fails the chosen ones."""

    def __init__(self, fail_uuids=(), notify_status=STATUS_SUCCESS):
        self._fail = {u.lower() for u in fail_uuids}
        self._notify_status = notify_status
        self.writes = []
        self.mtu_size = 517

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def read_gatt_char(self, uuid):
        # A real curve point: the hub encrypts the Wi-Fi envelope TO this key,
        # so 32 zero bytes would fail in the ECDH rather than in the code
        # under test.
        return bytearray(_DEVICE_PUB)

    async def start_notify(self, uuid, cb):
        if self._notify_status is not None:
            asyncio.get_running_loop().call_soon(
                cb, None, bytearray([self._notify_status]))

    async def stop_notify(self, uuid):
        pass

    async def write_gatt_char(self, uuid, data, response=True):
        self.writes.append((uuid.lower(), bytes(data)))
        if uuid.lower() in self._fail:
            raise BleakError("Operation failed with ATT error: 0x0e "
                             "(Unlikely Error)")


def _run(monkeypatch, fail_uuids=(), notify_status=STATUS_SUCCESS):
    client = FakeClient(fail_uuids, notify_status)
    monkeypatch.setattr(mp, "BleakClient", lambda *a, **k: client)
    monkeypatch.setattr(mp.ble_adapter, "kwargs", lambda a: {})
    coro = mp.provision(
        "AA:BB:CC:DD:EE:FF", name="cam4", ssid="net", password="pw",
        hub_x25519_priv=X25519PrivateKey.generate(),
        stream_host="10.0.0.1", stream_port=8894,
        hub_host="10.0.0.1", hub_port=8772)
    return client, asyncio.run(coro)


def test_config_write_error_does_not_abort_provisioning(monkeypatch):
    """CONFIG errors, device still reports SUCCESS: the WIFI write must happen."""
    client, result = _run(monkeypatch, fail_uuids=[mp.CONFIG_CHAR_UUID])
    written = [u for u, _ in client.writes]
    assert mp.WIFI_CHAR_UUID.lower() in written, (
        "a failed CONFIG write must not stop the WIFI write")
    assert result.unconfirmed is False


def test_both_writes_failing_without_status_still_fails(monkeypatch):
    """Tolerance is not blanket: no ack and no notify is a real failure."""
    with pytest.raises(RuntimeError, match="not acknowledged"):
        _run(monkeypatch,
             fail_uuids=[mp.CONFIG_CHAR_UUID, mp.WIFI_CHAR_UUID],
             notify_status=None)


def test_config_acked_but_no_status_is_unconfirmed_not_success(monkeypatch):
    """Writes landed, device never confirmed: report UNCONFIRMED, not success."""
    _, result = _run(monkeypatch, notify_status=None)
    assert result.unconfirmed is True


# ── a job must never end with error="" (2026-09-14) ──────────────────────────
#
# One did: hub log stopped at "[BLE] Connecting to ...", the board saw the
# central connect and drop 2.8 s later with no CONFIG write, and the job said
# state=error error="".  str() of a bare asyncio.TimeoutError or BleakError is
# empty; the provisioner now names the phase the link was in and the exception
# type, and the job runner falls back to type + step for anything else.

class _DropOnConnect(FakeClient):
    def __init__(self, exc):
        super().__init__()
        self._exc = exc

    async def __aenter__(self):
        raise self._exc


def _run_client(monkeypatch, client):
    monkeypatch.setattr(mp, "BleakClient", lambda *a, **k: client)
    monkeypatch.setattr(mp.ble_adapter, "kwargs", lambda a: {})
    return asyncio.run(mp.provision(
        "AA:BB:CC:DD:EE:FF", name="cam3", ssid="net", password="pw",
        hub_x25519_priv=X25519PrivateKey.generate(),
        stream_host="10.0.0.1", stream_port=8894,
        hub_host="10.0.0.1", hub_port=8772))


def test_connect_timeout_names_phase_and_type(monkeypatch):
    with pytest.raises(RuntimeError) as ei:
        _run_client(monkeypatch, _DropOnConnect(asyncio.TimeoutError()))
    msg = str(ei.value)
    assert "connecting" in msg and "TimeoutError" in msg and "AA:BB:CC:DD:EE:FF" in msg


def test_link_drop_during_pubkey_read_names_phase(monkeypatch):
    class _DropOnRead(FakeClient):
        async def read_gatt_char(self, uuid):
            raise BleakError("")                    # str() == ""
    with pytest.raises(RuntimeError) as ei:
        _run_client(monkeypatch, _DropOnRead())
    msg = str(ei.value)
    assert "reading the device pubkey" in msg and "BleakError" in msg


def test_job_error_text_never_empty():
    from hub.api import _job_error_text
    t = _job_error_text(asyncio.TimeoutError(), "gateway identity attached (dormant)")
    assert "TimeoutError" in t and "gateway identity attached" in t
    assert _job_error_text(ValueError("bad slot"), "scan/connect") == "bad slot"  # real text kept
    assert _job_error_text(BleakError(""), None).startswith("BleakError")


def test_exc_text_never_empty():
    assert mp.exc_text(asyncio.TimeoutError()) == "TimeoutError"
    assert mp.exc_text(BleakError("ATT 0x0e")) == "BleakError: ATT 0x0e"
