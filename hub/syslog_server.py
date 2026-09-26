"""
UDP syslog receiver for device log messages.

Zephyr's log_backend_net sends RFC 5424 syslog messages over UDP.
This receiver listens on a configurable port (default 514) and calls
a callback for each received message.

Usage:
    server = SyslogServer(log_cb=my_callback)
    await server.start()

Default callback prints to console:
    [syslog 192.0.2.23] <14>1 2026-04-13T01:23:45Z sensor-03 ...
"""

from __future__ import annotations
import asyncio
import logging
import socket
from typing import Callable

log = logging.getLogger("hub.syslog")

_DEFAULT_PORT = 5514


class SyslogServer:
    def __init__(self, bind_host: str = "::", bind_port: int = _DEFAULT_PORT,
                 log_cb: Callable | None = None,
                 db=None, log_store=None):
        self._host   = bind_host
        self._port   = bind_port
        self._log_cb = log_cb or self._default_cb
        self._db = db
        self._log_store = log_store

    @staticmethod
    def _default_cb(remote: str, text: str) -> None:
        print(f"[syslog {remote}] {text}")

    def _device_id_from_remote(self, remote_host: str) -> str:
        if not self._db:
            return ""
        try:
            for d in self._db.list_devices():
                pi = self._db.get_provision_info(d.id)
                if pi and pi.ml_eid and pi.ml_eid == remote_host:
                    return d.id
        except Exception as e:
            log.debug("device-id lookup failed: %s", e)
        return ""

    async def start(self) -> None:
        loop = asyncio.get_event_loop()

        # Create a UDP socket
        sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Accept both IPv4 and IPv6
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        sock.bind((self._host, self._port))
        sock.settimeout(1.0)  # non-blocking via timeout

        log.info("Syslog server listening on udp://[%s]:%d",
                 self._host, self._port)

        while True:
            try:
                data, addr = await loop.run_in_executor(
                    None, sock.recvfrom, 4096
                )
            except socket.timeout:
                await asyncio.sleep(0)
                continue

            remote = addr[0]
            if remote.startswith("::ffff:"):
                remote = remote[7:]

            text = data.decode("utf-8", errors="replace").strip()
            if self._log_store is not None:
                try:
                    device_id = self._device_id_from_remote(remote)
                    self._log_store.append_text(
                        device_id=device_id or remote, text=text,
                    )
                except Exception as e:
                    log.warning("log_store.append_text: %s", e)
            try:
                self._log_cb(remote, text)
            except Exception as e:
                log.warning("syslog_cb error: %s", e)
