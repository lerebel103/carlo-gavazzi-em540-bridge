"""Tests for IdleConnectionReaper."""

import time
import unittest
from unittest.mock import MagicMock

from app.utils.idle_connection_reaper import IdleConnectionReaper


def _make_mock_server():
    """Create a mock ModbusTcpServer with active_connections dict."""
    server = MagicMock()
    server.active_connections = {}
    server.callback_new_connection = MagicMock()
    return server


def _make_mock_handler(unique_id="conn-1"):
    """Create a mock connection handler."""
    handler = MagicMock()
    handler.unique_id = unique_id
    handler.trace_pdu = MagicMock(side_effect=lambda sending, pdu: pdu)
    handler.callback_disconnected = MagicMock()
    handler.close = MagicMock()
    return handler


class TestIdleConnectionReaper(unittest.TestCase):
    def setUp(self):
        self.server = _make_mock_server()
        self.reaper = IdleConnectionReaper(self.server, idle_timeout=1.0, server_label="test")

    def test_install_patches_callback_new_connection(self):
        """install() should replace the server's callback_new_connection."""
        original = self.server.callback_new_connection
        self.reaper.install()
        self.assertNotEqual(self.server.callback_new_connection, original)

    def test_new_connection_is_tracked(self):
        """After install, new connections should be tracked with a birth timestamp."""
        handler = _make_mock_handler("conn-42")
        self.server.callback_new_connection.return_value = handler
        self.reaper.install()

        # Simulate a new connection
        result = self.server.callback_new_connection()
        self.assertIs(result, handler)
        self.assertIn("conn-42", self.reaper._last_activity)

    def test_pdu_activity_updates_timestamp(self):
        """PDU activity on a tracked connection should refresh its last-activity time."""
        handler = _make_mock_handler("conn-1")
        self.server.callback_new_connection.return_value = handler
        self.reaper.install()

        self.server.callback_new_connection()
        initial_time = self.reaper._last_activity["conn-1"]

        # Simulate time passing and PDU activity
        time.sleep(0.01)
        handler.trace_pdu(True, MagicMock())
        self.assertGreater(self.reaper._last_activity["conn-1"], initial_time)

    def test_disconnect_cleans_up_tracking(self):
        """When a connection disconnects, its tracking entry should be removed."""
        handler = _make_mock_handler("conn-1")
        self.server.callback_new_connection.return_value = handler
        self.reaper.install()

        self.server.callback_new_connection()
        self.assertIn("conn-1", self.reaper._last_activity)

        # Simulate disconnect
        handler.callback_disconnected()
        self.assertNotIn("conn-1", self.reaper._last_activity)

    def test_idle_connection_is_reaped(self):
        """Connections idle beyond the timeout should be closed and disconnected."""
        handler = _make_mock_handler("conn-1")
        original_disconnected = handler.callback_disconnected
        self.server.callback_new_connection.return_value = handler
        self.reaper.install()

        self.server.callback_new_connection()
        # Put the connection in active_connections (simulating pymodbus behaviour)
        self.server.active_connections["conn-1"] = handler

        # Backdate the activity to exceed timeout
        self.reaper._last_activity["conn-1"] = time.monotonic() - 120.0

        # Run the reap
        self.reaper._reap_idle_connections()

        handler.close.assert_called_once()
        # The original callback_disconnected should be called (via our wrapper)
        # since pymodbus won't fire it on explicit close().
        original_disconnected.assert_called_once_with(None)
        self.assertNotIn("conn-1", self.reaper._last_activity)

    def test_active_connection_is_not_reaped(self):
        """Connections with recent activity should not be closed."""
        handler = _make_mock_handler("conn-1")
        self.server.callback_new_connection.return_value = handler
        self.reaper.install()

        self.server.callback_new_connection()
        self.server.active_connections["conn-1"] = handler

        # Activity is fresh (just set by connection birth)
        self.reaper._reap_idle_connections()

        handler.close.assert_not_called()
        self.assertIn("conn-1", self.reaper._last_activity)

    def test_untracked_connection_gets_grace_period(self):
        """Connections in active_connections but not tracked get a full timeout window."""
        handler = _make_mock_handler("conn-new")
        self.server.active_connections["conn-new"] = handler

        # Reap should NOT close it — it gets a fresh timestamp
        self.reaper._reap_idle_connections()
        handler.close.assert_not_called()
        self.assertIn("conn-new", self.reaper._last_activity)

    def test_stop_is_thread_safe(self):
        """stop() should use call_soon_threadsafe when loop is running."""
        loop = MagicMock()
        loop.is_running.return_value = True
        loop.create_task = MagicMock(return_value=MagicMock())
        self.reaper.start(loop)

        task = self.reaper._task
        self.reaper.stop()

        loop.call_soon_threadsafe.assert_called_once_with(task.cancel)
        self.assertIsNone(self.reaper._task)
