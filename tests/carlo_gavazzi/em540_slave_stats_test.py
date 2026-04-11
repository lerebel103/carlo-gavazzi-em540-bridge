"""Tests for EM540SlaveStats client connection tracking.

Validates: Requirements 13.1, 13.2, 13.3
"""

import unittest

from app.carlo_gavazzi.em540_slave_stats import EM540SlaveStats


class TestEM540SlaveStats(unittest.TestCase):
    """Validates: Requirements 13.1, 13.2, 13.3"""

    # --- Requirement 13.1: all counts initialize to zero ---

    def test_initial_rtu_client_count_is_zero(self):
        stats = EM540SlaveStats()
        self.assertEqual(stats.rtu_client_count, 0)

    def test_initial_rtu_client_disconnect_count_is_zero(self):
        stats = EM540SlaveStats()
        self.assertEqual(stats.rtu_client_disconnect_count, 0)

    def test_initial_tcp_client_count_is_zero(self):
        stats = EM540SlaveStats()
        self.assertEqual(stats.tcp_client_count, 0)

    def test_initial_tcp_client_disconnect_count_is_zero(self):
        stats = EM540SlaveStats()
        self.assertEqual(stats.tcp_client_disconnect_count, 0)

    # --- Requirement 13.2: listener notified on changed() ---

    def test_listener_called_on_changed(self):
        stats = EM540SlaveStats()
        called = []

        stats.add_listener(lambda s: called.append(s))
        stats.changed()

        self.assertEqual(len(called), 1)
        self.assertIs(called[0], stats)

    def test_changed_without_listeners_does_not_raise(self):
        stats = EM540SlaveStats()
        stats.changed()  # should not raise

    # --- Requirement 13.3: multiple listeners all notified ---

    def test_multiple_listeners_all_notified(self):
        stats = EM540SlaveStats()
        results_a = []
        results_b = []
        results_c = []

        stats.add_listener(lambda s: results_a.append(s))
        stats.add_listener(lambda s: results_b.append(s))
        stats.add_listener(lambda s: results_c.append(s))

        stats.changed()

        self.assertEqual(len(results_a), 1)
        self.assertEqual(len(results_b), 1)
        self.assertEqual(len(results_c), 1)
        self.assertIs(results_a[0], stats)
        self.assertIs(results_b[0], stats)
        self.assertIs(results_c[0], stats)


if __name__ == "__main__":
    unittest.main()
