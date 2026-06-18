"""Idle connection reaper for pymodbus TCP/RTU-over-TCP servers.

Periodically scans a ModbusTcpServer's active connections and closes any
that have not seen Modbus PDU activity within a configurable timeout.

This prevents half-open or abandoned TCP connections from accumulating
and exhausting connection slots on the downstream slave bridges.
"""

import asyncio
import logging
import time
from typing import Callable

from pymodbus.pdu import ModbusPDU
from pymodbus.server import ModbusTcpServer

logger = logging.getLogger(__name__)

# Default idle timeout in seconds. Connections with no PDU activity for
# longer than this are closed.
DEFAULT_IDLE_TIMEOUT: float = 60.0

# How often the reaper checks for idle connections (seconds).
_SCAN_INTERVAL: float = 10.0


class IdleConnectionReaper:
    """Tracks per-connection activity and closes idle ones.

    Usage:
        1. Create an instance with the target server, timeout, and a label.
        2. Call `install()` to monkey-patch the server's callback_new_connection
           so that each new handler gets per-connection PDU activity tracking.
        3. Call `start(loop)` to begin periodic scanning on the server's event loop.
        4. Call `stop()` to cancel the scanning task.

    The reaper intercepts each new connection handler to wrap its trace_pdu
    callback with per-connection activity tracking. On each scan, connections
    that have exceeded the idle timeout are closed.
    """

    def __init__(
        self,
        server: ModbusTcpServer,
        idle_timeout: float = DEFAULT_IDLE_TIMEOUT,
        server_label: str = "",
    ) -> None:
        self._server = server
        self._idle_timeout = idle_timeout
        self._label = server_label or "modbus-server"
        # Maps connection unique_id -> last activity monotonic timestamp
        self._last_activity: dict[str, float] = {}
        self._task: asyncio.Task | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._original_callback_new_connection: Callable | None = None

    def install(self) -> None:
        """Patch the server to track per-connection PDU activity.

        Must be called before the server starts accepting connections.
        """
        self._original_callback_new_connection = self._server.callback_new_connection
        self._server.callback_new_connection = self._wrapped_callback_new_connection  # type: ignore[method-assign]

    def _wrapped_callback_new_connection(self):
        """Intercept new connection creation to wrap trace_pdu with activity tracking."""
        handler = self._original_callback_new_connection()

        # Record connection birth
        conn_id = handler.unique_id
        self._last_activity[conn_id] = time.monotonic()

        # Wrap the handler's trace_pdu to record per-connection activity
        original_trace_pdu = handler.trace_pdu

        def _tracking_trace_pdu(sending: bool, pdu: ModbusPDU) -> ModbusPDU:
            self._last_activity[conn_id] = time.monotonic()
            return original_trace_pdu(sending, pdu)

        handler.trace_pdu = _tracking_trace_pdu

        # Wrap the handler's callback_disconnected to clean up tracking
        original_disconnected = handler.callback_disconnected

        def _tracking_disconnected(exc=None):
            self._last_activity.pop(conn_id, None)
            return original_disconnected(exc)

        handler.callback_disconnected = _tracking_disconnected

        return handler

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        """Start the periodic idle-scan task on the given event loop."""
        if self._task is not None:
            return
        self._loop = loop
        self._task = loop.create_task(self._scan_loop(), name=f"idle-reaper-{self._label}")

    def stop(self) -> None:
        """Cancel the periodic scan task (thread-safe).

        The task runs on the server's dedicated event loop thread, so we schedule
        cancellation via call_soon_threadsafe to avoid cross-thread race conditions.
        """
        task = self._task
        if task is not None:
            self._task = None
            loop = self._loop
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(task.cancel)
            else:
                task.cancel()

    async def _scan_loop(self) -> None:
        """Periodically scan for and close idle connections."""
        try:
            while True:
                await asyncio.sleep(_SCAN_INTERVAL)
                self._reap_idle_connections()
        except asyncio.CancelledError:
            pass

    def _reap_idle_connections(self) -> None:
        """Check all active connections and close any that have been idle too long."""
        now = time.monotonic()
        active = getattr(self._server, "active_connections", None)
        if not active:
            return

        to_close: list[str] = []
        for conn_id in list(active.keys()):
            last_seen = self._last_activity.get(conn_id)
            if last_seen is None:
                # Connection exists but we never tracked it (shouldn't happen with install()).
                # Give it a full timeout window from now.
                self._last_activity[conn_id] = now
                continue
            idle_seconds = now - last_seen
            if idle_seconds >= self._idle_timeout:
                to_close.append(conn_id)

        for conn_id in to_close:
            handler = active.get(conn_id)
            if handler is None:
                self._last_activity.pop(conn_id, None)
                continue
            idle_seconds = now - self._last_activity.get(conn_id, now)
            logger.info(
                "[%s] Closing idle connection %s (idle %.1fs, timeout %.1fs).",
                self._label,
                conn_id,
                idle_seconds,
                self._idle_timeout,
            )
            try:
                handler.close()
            except Exception:
                logger.debug(
                    "[%s] Error closing idle connection %s",
                    self._label,
                    conn_id,
                    exc_info=True,
                )
            self._last_activity.pop(conn_id, None)
