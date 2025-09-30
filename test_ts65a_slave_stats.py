import unittest
import time

from ts65a_slave_stats import Ts65aSlaveStats

class TestTs65aSlaveStats(unittest.TestCase):
    def setUp(self):
        self.stats = Ts65aSlaveStats()
        self.stats.grid_feed_in_hard_limit=-100.0

    def test_initial_values(self):
        self.assertEqual(self.stats.grid_feed_in_hard_limit, -100.0)
        self.assertEqual(self.stats.tcp_client_count, 0)
        self.assertEqual(self.stats.tcp_client_disconnect_count, 0)
        self.assertEqual(self.stats.min_power_w, 0.0)
        self.assertEqual(self.stats.max_power_w, 0.0)
        self.assertEqual(self.stats.power_over_feed_in_limit_count, 0)
        self.assertEqual(self.stats.power_over_feed_limit_max_duration_sec, 0.0)

    def test_add_listener_and_changed(self):
        called = []
        def listener(obj):
            called.append(obj)
        self.stats.add_listener(listener)
        self.stats.changed()
        self.assertEqual(len(called), 1)
        self.assertIs(called[0], self.stats)

    def test_check_power_over_feed_in_limit_under_limit(self):
        result = self.stats.check_power_over_feed_in_limit(-150.0)
        self.assertTrue(result)
        self.assertEqual(self.stats.power_over_feed_in_limit_count, 1)

    def test_check_power_over_feed_in_limit_over_limit(self):
        self.stats.check_power_over_feed_in_limit(-150.0)
        result = self.stats.check_power_over_feed_in_limit(-50.0)
        self.assertFalse(result)

    def test_over_feed_in_limit_duration(self):
        self.stats.check_power_over_feed_in_limit(-150.0)
        time.sleep(0.01)
        self.stats.check_power_over_feed_in_limit(-125.0)
        time.sleep(0.01)
        self.stats.check_power_over_feed_in_limit(-120.0)

        # Still only one event
        self.assertEqual(self.stats.power_over_feed_in_limit_count, 1)
        self.assertGreater(self.stats.power_over_feed_limit_max_duration_sec, 0)

    def test_reset_over_limit_timer(self):
        self.stats.check_power_over_feed_in_limit(-150.0)
        was_over = self.stats._reset_over_limit_timer()
        self.assertTrue(was_over)
        was_over = self.stats._reset_over_limit_timer()
        self.assertFalse(was_over)

    # Test that the over limit condition resets when power goes back under the limit
    def test_over_limit_condition_resets(self):
        res = self.stats.check_power_over_feed_in_limit(-150.0)
        self.assertTrue(res)
        self.assertTrue(self.stats._last_over_limit_time is not None)

        res = self.stats.check_power_over_feed_in_limit(-50.0)
        self.assertTrue(self.stats._last_over_limit_time is None)
        self.assertFalse(res)

    # Test min and max power updates
    def test_min_max_power_updates(self):
        self.stats.check_power_over_feed_in_limit(-150.0)
        self.assertEqual(self.stats.min_power_w, -150.0)
        self.assertEqual(self.stats.max_power_w, 0.0)

        self.stats.check_power_over_feed_in_limit(-200.0)
        self.assertEqual(self.stats.min_power_w, -200.0)

        # Max power has to be above 0 to update
        self.stats.check_power_over_feed_in_limit(-50.0)
        self.assertEqual(self.stats.max_power_w, 0.0)
        self.stats.check_power_over_feed_in_limit(50.0)
        self.assertEqual(self.stats.max_power_w, 50.0)


if __name__ == "__main__":
    unittest.main()
