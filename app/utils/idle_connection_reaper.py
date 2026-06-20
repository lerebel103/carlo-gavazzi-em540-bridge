"""Idle connection reaper for pymodbus TCP/RTU-over-TCP servers.

Periodically scans a ModbusTcpServer's active connections and closes any
that have not seen Modbus PDU activity within a configurable timeout.

Also enforces a maximum connection limit — new connections beyond the cap
are immediately closed to prevent resource exhaustion on constrained hosts.

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

# Default maximum concurrent connections per server. 0 = unlimited.
DEFAULT_MAX_CONNECTIONS: int = 15

# How often the reaper checks for idle connections (seconds).
_SCAN_INTERVAL: float = 10.0


class IdleConnectionReaper:
    """Tracks per-connection activity, closes idle ones, and enforces a connection cap.

    Usage:
        1. Create an instance with the target server, timeout, max connections, and a label.
        2. Call `install()` to monkey-patch the server's callback_new_connection
           so that each new handler gets per-connection PDU activity tracking
           and the connection cap is enforced.
        3. Call `start(loop)` to begin periodic scanning on the server's event loop.
        4. Call `stop()` to cancel the scanning task.

    The reaper intercepts each new connection handler to wrap its trace_pdu
    callback with per-connection activity tracking. On each scan, connections
    that have exceeded the idle timeout are closed.

    When max_connections is reached, new connections are immediately closed
    with a log warning. This prevents resource exhaustion from runaway clients.
    """

    def __init__(
        self,
        server: ModbusTcpServer,
        idle_timeout: float = DEFAULT_IDLE_TIMEOUT,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        server_label: str = "",
    ) -> None:
        self._server = server
        self._idle_timeout = idle_timeout
        self._max_connections = max_connections
        self._label = server_label or "modbus-server"
        # Maps connection unique_id -> last activity monotonic timestamp
        self._last_activity: dict[str, float] = {}
        self._task: asyncio.Task | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._original_callback_new_connection: Callable | None = None
        self._rejected_connection_count: int = 0

    @property
    def rejected_connection_count(self) -> int:
        """Total number of connections rejected due to the max-connections cap."""
        return self._rejected_connection_count

    def install(self) -> None:
        """Patch the server to track per-connection PDU activity.

        Must be called before the server starts accepting connections.
        Raises RuntimeError if called more than once.
        """
        if self._original_callback_new_connection is not None:
            raise RuntimeError(f"IdleConnectionReaper already installed on {self._label}")
        self._original_callback_new_connection = self._server.callback_new_connection
        self._server.callback_new_connection = self._wrapped_callback_new_connection  # type: ignore[method-assign]

    def _pop_rejected_handler(self, active: dict, unique_id) -> None:
        """Best-effort removal of a rejected handler from active_connections."""
        try:
            active.pop(unique_id, None)
        except Exception:
            logger.debug(
                "[%s] Error removing rejected connection %s from active_connections",
                self._label,
                unique_id,
                exc_info=True,
            )

    def _wrapped_callback_new_connection(self):
        """Intercept new connection creation to wrap trace_pdu with activity tracking.

        Also enforces the max-connections cap: if the limit is reached, the new
        connection is immediately closed after creation.

        Note: pymodbus expects this factory to return a protocol handler instance,
        so the handler is always returned even when rejected. In the rejection path
        the handler has already been closed and its disconnect callback invoked, so
        the returned object is inert and will not service requests.
        """
        handler = self._original_callback_new_connection()

        # Enforce connection cap
        if self._max_connections > 0:
            active = getattr(self._server, "active_connections", None)
            active_count = len(active) if active else 0
            # pymodbus may or may not have inserted the new handler into
            # active_connections by the time this callback runs, and the ordering
            # is not guaranteed across versions. Count only the *other* connections
            # (excluding this new handler) so the cap is enforced consistently:
            # reject when there are already max_connections established connections.
            handler_already_tracked = bool(active) and handler.unique_id in active
            existing_count = active_count - (1 if handler_already_tracked else 0)
            if existing_count >= self._max_connections:
                self._rejected_connection_count += 1
                logger.warning(
                    "[%s] Rejecting connection %s: at capacity (%d/%d, %d rejected total).",
                    self._label,
                    handler.unique_id,
                    existing_count,
                    self._max_connections,
                    self._rejected_connection_count,
                )
                try:
                    handler.close()
                    try:
                        handler.callback_disconnected(None)
                    except Exception:
                        pass
                except Exception:
                    logger.debug(
                        "[%s] Error closing rejected connection %s",
                        self._label,
                        handler.unique_id,
                        exc_info=True,
                    )
                # Best-effort removal from active_connections: close() may not trigger
                # the server's normal connection-lost cleanup, which would otherwise
                # leave the rejected handler counted as active and keep the server
                # permanently "at capacity".
                #
                # The insertion ordering is not guaranteed: pymodbus may insert this
                # handler into active_connections before OR after this callback
                # returns. An immediate pop handles the insert-before case; a
                # deferred pop scheduled on the event loop handles the insert-after
                # case (it runs once control returns to the loop, by which point the
                # handler has been inserted).
                if active is not None:
                    self._pop_rejected_handler(active, handler.unique_id)
                    try:
                        loop = asyncio.get_running_loop()
                        loop.call_soon(self._pop_rejected_handler, active, handler.unique_id)
                    except RuntimeError:
                        # No running loop (should not happen in the server context);
                        # the immediate pop above is the best we can do.
                        pass
                return handler

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
                # pymodbus does not call callback_disconnected when close() is invoked
                # explicitly (is_closing flag prevents connection_lost from firing).
                # Manually trigger it so trace_connect(False) fires and stats update.
                try:
                    handler.callback_disconnected(None)
                except Exception:
                    pass
            except Exception:
                logger.debug(
                    "[%s] Error closing idle connection %s",
                    self._label,
                    conn_id,
                    exc_info=True,
                )
            self._last_activity.pop(conn_id, None)
