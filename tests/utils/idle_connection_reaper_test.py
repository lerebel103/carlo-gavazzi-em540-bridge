"""Tests for IdleConnectionReaper."""

import time
import unittest
from unittest.mock import MagicMock, patch

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

        with patch("app.utils.idle_connection_reaper.time.monotonic", return_value=100.0):
            self.server.callback_new_connection()
        self.assertEqual(self.reaper._last_activity["conn-1"], 100.0)

        # Simulate PDU activity at a later time
        with patch("app.utils.idle_connection_reaper.time.monotonic", return_value=105.0):
            handler.trace_pdu(True, MagicMock())
        self.assertEqual(self.reaper._last_activity["conn-1"], 105.0)

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

    def test_double_install_raises(self):
        """Calling install() twice should raise RuntimeError."""
        self.reaper.install()
        with self.assertRaises(RuntimeError):
            self.reaper.install()

    def test_connection_rejected_when_at_capacity(self):
        """New connections beyond max_connections should be immediately closed."""
        server = _make_mock_server()
        reaper = IdleConnectionReaper(server, idle_timeout=1.0, max_connections=2, server_label="test-cap")
        reaper.install()

        # Set up handlers
        handler1 = _make_mock_handler("conn-1")
        handler2 = _make_mock_handler("conn-2")
        handler3 = _make_mock_handler("conn-3")

        # Simulate first two connections (within cap)
        reaper._original_callback_new_connection.return_value = handler1
        server.active_connections["conn-1"] = handler1
        server.callback_new_connection()
        handler1.close.assert_not_called()

        reaper._original_callback_new_connection.return_value = handler2
        server.active_connections["conn-2"] = handler2
        server.callback_new_connection()
        handler2.close.assert_not_called()

        # Third connection should be rejected (3 in active_connections > max of 2)
        reaper._original_callback_new_connection.return_value = handler3
        server.active_connections["conn-3"] = handler3
        server.callback_new_connection()
        handler3.close.assert_called_once()
        self.assertEqual(reaper.rejected_connection_count, 1)
        # Rejected connection should NOT be tracked for idle reaping
        self.assertNotIn("conn-3", reaper._last_activity)

    def test_connection_cap_consistent_when_handler_not_yet_in_active(self):
        """Cap must hold even if pymodbus inserts the handler into active_connections
        *after* this callback runs (the count then excludes the new handler)."""
        server = _make_mock_server()
        reaper = IdleConnectionReaper(server, idle_timeout=1.0, max_connections=2, server_label="test-order")
        reaper.install()

        # Two established connections already present, new handler NOT yet inserted.
        server.active_connections["conn-1"] = _make_mock_handler("conn-1")
        server.active_connections["conn-2"] = _make_mock_handler("conn-2")

        handler3 = _make_mock_handler("conn-3")
        reaper._original_callback_new_connection.return_value = handler3
        # Note: conn-3 deliberately NOT added to active_connections before the call.
        server.callback_new_connection()

        # With 2 existing established connections and a cap of 2, the 3rd must be rejected
        # even though active_connections does not yet contain it.
        handler3.close.assert_called_once()
        self.assertEqual(reaper.rejected_connection_count, 1)

    def test_rejected_connection_removed_from_active_connections(self):
        """A rejected handler must be popped from active_connections so it does not
        keep the server permanently at capacity."""
        server = _make_mock_server()
        reaper = IdleConnectionReaper(server, idle_timeout=1.0, max_connections=1, server_label="test-pop")
        reaper.install()

        server.active_connections["conn-1"] = _make_mock_handler("conn-1")

        handler2 = _make_mock_handler("conn-2")
        reaper._original_callback_new_connection.return_value = handler2
        server.active_connections["conn-2"] = handler2  # pymodbus inserted it
        server.callback_new_connection()

        handler2.close.assert_called_once()
        # The rejected handler must no longer occupy a slot.
        self.assertNotIn("conn-2", server.active_connections)

    def test_connection_accepted_after_disconnect_frees_slot(self):
        """After a disconnect frees a slot, new connections should be accepted."""
        server = _make_mock_server()
        reaper = IdleConnectionReaper(server, idle_timeout=1.0, max_connections=1, server_label="test-cap")
        reaper.install()

        handler1 = _make_mock_handler("conn-1")
        reaper._original_callback_new_connection.return_value = handler1
        server.active_connections["conn-1"] = handler1
        server.callback_new_connection()
        handler1.close.assert_not_called()

        # Simulate disconnect (remove from active_connections)
        del server.active_connections["conn-1"]
        handler1.callback_disconnected()

        # New connection should be accepted
        handler2 = _make_mock_handler("conn-2")
        reaper._original_callback_new_connection.return_value = handler2
        server.active_connections["conn-2"] = handler2
        server.callback_new_connection()
        handler2.close.assert_not_called()
        self.assertIn("conn-2", reaper._last_activity)
        self.assertEqual(reaper.rejected_connection_count, 0)
