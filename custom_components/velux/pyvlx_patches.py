"""Runtime fixes for connection handling in pyvlx.

pyvlx (0.2.33 up to at least 0.2.36) has several problems in its reconnect
logic which leave all entities permanently unavailable until the KLF 200
and/or Home Assistant is restarted:

1. Deadlock on reconnect: ``PyVLX.send_frame()`` calls ``ensure_connected()``
   while ``ApiEvent.do_api_call()`` already holds ``api_call_semaphore``.
   If the connection was lost in the meantime, ``connect()`` runs inside the
   semaphore and its own API calls (``password_enter`` ...) wait forever for
   the very same semaphore. From then on every API call (heartbeat, commands,
   reboot button) blocks forever.
2. Concurrent reconnects: ``ensure_connected()`` is not serialized, so two
   tasks (e.g. heartbeat + user command) can open two TLS sessions at once.
   The first transport is overwritten without being closed and keeps one of
   the very few session slots of the KLF 200 occupied.
3. Stale transports kill the active one: when an orphaned transport is
   closed, its ``connection_lost`` callback calls ``disconnect()`` on the
   shared connection object and thereby closes the *current* transport.
4. Half-open sessions: if ``connect()`` fails after the TCP/TLS connection
   was established (e.g. login timeout), ``connected`` stays ``True`` on an
   unauthenticated socket and no reconnect is ever attempted.
5. The heartbeat loop only catches ``OSError``/``PyVLXException``; any other
   exception silently ends the task and with it every automatic reconnect.

The patches below are applied once when the integration is loaded.
"""
from __future__ import annotations

import asyncio
import contextvars
import logging

from pyvlx import PyVLX
from pyvlx.connection import Connection, TCPTransport
from pyvlx.exception import PyVLXException
from pyvlx.heartbeat import Heartbeat

LOGGER = logging.getLogger(__package__)

_PATCHED_ATTR = "_my_velux_patched"
# True while the current task runs a (re)connect guarded by ensure_connected().
_IN_CONNECT: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "my_velux_in_connect", default=False
)


def _get_connect_lock(pyvlx: PyVLX) -> asyncio.Lock:
    lock = getattr(pyvlx, "_my_velux_connect_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        pyvlx._my_velux_connect_lock = lock  # pylint: disable=protected-access
    return lock


async def _ensure_connected(self: PyVLX) -> None:
    """Connect if necessary; serialized and safe against re-entry (fix 2, 4)."""
    if _IN_CONNECT.get():
        # API calls issued by connect() itself (login, version, ...).
        return
    lock = _get_connect_lock(self)
    if self.connection.connected and not lock.locked():
        return
    async with lock:
        if self.connection.connected:
            return
        LOGGER.debug("KLF 200 not connected, reconnecting")
        token = _IN_CONNECT.set(True)
        try:
            await self.connect()
        except BaseException:
            # Do not leave a half-open, unauthenticated session behind.
            if self.connection.connected or self.connection.transport is not None:
                self.connection.disconnect()
            raise
        finally:
            _IN_CONNECT.reset(token)


async def _send_frame(self: PyVLX, frame) -> None:
    """Send frame without connecting from inside the API semaphore (fix 1)."""
    if not self.connection.connected or self.connection.transport is None:
        if _IN_CONNECT.get():
            raise PyVLXException("Connection to KLF 200 lost during connect")
        raise PyVLXException("Not connected to KLF 200")
    self.connection.write(frame)


async def _connection_connect(self: Connection) -> None:
    """Open the TLS connection; close stale transports first (fix 2, 3)."""
    if self.transport is not None:
        LOGGER.debug("Closing stale KLF 200 transport before reconnecting")
        stale = self.transport
        self.transport = None
        self.connected = False
        stale.close()

    own: dict[str, object] = {}

    def on_lost() -> None:
        # Only react if the transport that was lost is still the active one.
        if own.get("transport") is not None and self.transport is own["transport"]:
            self.disconnect()
        else:
            LOGGER.debug("Ignoring connection_lost of a stale KLF 200 transport")

    tcp_client = TCPTransport(self.frame_received_cb, connection_lost_cb=on_lost)
    loop = asyncio.get_running_loop()
    assert self.config.host is not None
    try:
        async with asyncio.timeout(self.CONNECT_TIMEOUT):
            transport, _ = await loop.create_connection(
                lambda: tcp_client,
                host=self.config.host,
                port=self.config.port,
                ssl=self.create_ssl_context(),
            )
    except asyncio.TimeoutError as error:
        self.transport = None
        self.connected = False
        raise PyVLXException(
            f"Socket connection to KLF 200 timed out after {self.CONNECT_TIMEOUT} seconds"
        ) from error
    except OSError as error:  # ssl.SSLError is a subclass of OSError
        self.transport = None
        self.connected = False
        raise PyVLXException(
            f"Failed to open socket connection to KLF 200: {error}"
        ) from error

    own["transport"] = transport
    self.transport = transport
    self.connected = True
    self.connection_counter += 1
    LOGGER.debug("Number of connections since last HA start: %s", self.connection_counter)
    for connection_opened_cb in self.connection_opened_cbs:
        task = asyncio.create_task(connection_opened_cb())
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)


async def _heartbeat_run(self: Heartbeat) -> None:
    """Heartbeat loop that survives unexpected exceptions (fix 5)."""
    LOGGER.debug("Heartbeat: started")
    while True:
        await asyncio.sleep(self.interval)
        try:
            await self.pulse()
        except asyncio.CancelledError:
            raise
        except (OSError, PyVLXException) as err:
            LOGGER.debug("Heartbeat: pulsing failed: %s", err)
        except Exception:  # pylint: disable=broad-except
            LOGGER.exception("Heartbeat: unexpected error, continuing")


def apply_pyvlx_patches() -> None:
    """Apply all patches once."""
    if getattr(PyVLX, _PATCHED_ATTR, False):
        return
    PyVLX.ensure_connected = _ensure_connected
    PyVLX.send_frame = _send_frame
    Connection.connect = _connection_connect
    Heartbeat._run = _heartbeat_run  # pylint: disable=protected-access
    setattr(PyVLX, _PATCHED_ATTR, True)
    LOGGER.debug("Applied pyvlx connection handling patches")
