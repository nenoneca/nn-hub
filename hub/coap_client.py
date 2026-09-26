"""
Hub-initiated CoAP client — used to probe registered devices.

GET_INFO request/response (protocol v2)
-----------------------------------------
Hub → device:  POST coap://[device]/info
  Body: JSON ECIES v2 envelope (see crypto.py)
  Plaintext: b'{"cmd":"get_info"}'

Device → hub:  2.05 Content
  Body: JSON ECIES v2 envelope
  Plaintext (current):   {"name": "<str>"}
  Plaintext (future):    {"name": "..", "firmware": "..", "version": ".."}

Key derivation uses static-static + ephemeral X25519 DH (see crypto.py).

Device-side implementation notes
----------------------------------
  CoAP resource:  POST /info
  Handler must:
    1. Parse JSON envelope {v, epk, nonce, ct} from body
    2. Compute shared secrets:
         shared_e = ECDH(device_x25519_priv, epk)
         shared_s = ECDH(device_x25519_priv, hub_x25519_pub)
    3. Derive AES key: HKDF-SHA256(shared_e||shared_s, salt=epk, info="nn-hub-v2-h2d")
    4. AES-256-GCM decrypt; confirm cmd == "get_info"
    5. Build response JSON {name: ...}
    6. Encrypt response:
         resp_ephem = ephemeral_x25519()
         shared_e2 = ECDH(resp_ephem_priv, hub_x25519_pub)
         shared_s2 = ECDH(device_x25519_priv, hub_x25519_pub)
         aes_key2  = HKDF-SHA256(shared_e2||shared_s2, salt=resp_epk,
                                  info="nn-hub-v2-d2h")
    7. Return 2.05 Content with JSON envelope body
"""

from __future__ import annotations
import json
import logging
import socket

import aiocoap
import aiocoap.numbers

from .crypto import encrypt, decrypt, x25519_pubkey_bytes
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

log = logging.getLogger("hub.coap_client")

GET_INFO_TIMEOUT = 10.0  # seconds


async def get_device_info(router,
                          device_id: str,
                          timeout: float = 10.0) -> dict:
    """
    Ask the device to describe itself via the nn_proto control plane.
    Sends H2D INFO_QUERY (empty body), awaits matching D2H INFO_REPLY
    whose body is a JSON doc.  Replaces the legacy
    `coap://[device]/info` ECIES path.

    *router*: HubProtoRouter instance.
    *device_id*: 16-hex device id (hub DB key).
    """
    from . import proto
    reply = await router.request_h2d(
        device_id,
        req_cmd=proto.Cmd.INFO_QUERY,
        body=b"",
        expected_reply_cmd=proto.Cmd.INFO_REPLY,
        timeout=timeout,
    )
    text = reply.decode("utf-8", errors="replace")
    return json.loads(text)


async def _send_field_via_relay(router,
                                device_id: str,
                                device_x25519_pub: bytes,
                                hub_enc_priv: X25519PrivateKey,
                                payload_obj: dict,
                                timeout: float = 10.0) -> dict:
    """Internal helper: encrypt *payload_obj* for *device_id*, send via
    the gateway-routed nn_proto FIELD_OP, await FIELD_REPLY, decrypt the
    response envelope, return the parsed JSON dict.

    *router* is an instance of ``hub.proto_router.HubProtoRouter``.
    """
    plaintext = json.dumps(payload_obj).encode()

    # Phase 3: prefer the symmetric-session path (FIELD_OP_S) — zero
    # asymmetric crypto on the device, ~ms instead of ~seconds.  Fall
    # back to the legacy ECIES path when no verified session exists
    # (old firmware, or handshake not completed yet this boot).
    import asyncio as _aio
    try:
        reply_plain = await router.request_field_sealed(
            device_id, plaintext, timeout=timeout)
        log.info("[relay] FIELD_OP_S %s → device=%s (sealed)",
                 payload_obj.get("op"), device_id)
        return json.loads(reply_plain)
    except KeyError:
        pass                      # no session — legacy path below
    except _aio.TimeoutError:
        raise                     # transport-level; legacy would fare no better
    except Exception as e:        # open/seal error — be safe, use legacy
        log.warning("[relay] sealed path failed (%s) — ECIES fallback", e)

    envelope  = encrypt(plaintext, device_x25519_pub, hub_enc_priv,
                        direction="h2d")
    env_bytes = json.dumps(envelope).encode()

    log.info("[relay] FIELD_OP %s → device=%s", payload_obj.get("op"),
             device_id)
    reply_env_bytes = await router.request_field(device_id, env_bytes,
                                                 timeout=timeout)
    reply_envelope = json.loads(reply_env_bytes)
    plaintext_resp = decrypt(reply_envelope, hub_enc_priv,
                             device_x25519_pub, direction="d2h")
    return json.loads(plaintext_resp)


async def get_field(router,
                    device_id: str,
                    name: str,
                    device_x25519_pub: bytes,
                    hub_enc_priv: X25519PrivateKey,
                    timeout: float = 10.0) -> dict:
    """Read the cached value of *name* on the device, via gateway relay.

    Returns the device's parsed JSON response.  On success it has
    keys ``name`` and ``value`` (where ``value`` is ``None`` if the
    field has never been written).  On error it has key ``err``.
    """
    return await _send_field_via_relay(
        router, device_id, device_x25519_pub, hub_enc_priv,
        {"op": "get", "name": name}, timeout=timeout,
    )


async def set_field(router,
                    device_id: str,
                    name: str,
                    value: float,
                    device_x25519_pub: bytes,
                    hub_enc_priv: X25519PrivateKey,
                    timeout: float = 10.0) -> dict:
    """Write *value* to the actuator-typed field *name* on the device,
    via gateway relay.

    Returns the device's parsed JSON response (echoes ``name``/``value``
    on success, or includes ``err`` for failure cases like
    ``read_only`` (sensor type), ``out_of_range``, ``unknown_field``).
    """
    return await _send_field_via_relay(
        router, device_id, device_x25519_pub, hub_enc_priv,
        {"op": "set", "name": name, "value": value}, timeout=timeout,
    )


async def resolve_mdns(hostname: str, timeout: float = 5.0) -> str | None:
    """
    Resolve *hostname* (e.g. "sensor-01.local") to an IPv6 or IPv4 address.
    Uses the system resolver (avahi-daemon on Linux handles .local lookups).
    Returns the first result or None on failure.
    """
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        infos = await asyncio.wait_for(
            loop.getaddrinfo(hostname, None, proto=socket.IPPROTO_UDP),
            timeout=timeout,
        )
        if infos:
            return infos[0][4][0]   # first address
    except (OSError, asyncio.TimeoutError):
        pass
    return None
