import unittest

from carlo_gravazzi.meter_data import MeterData
from fronius.ts65a_slave_stats import Ts65aSlaveStats


class TestTs65aSlaveStats(unittest.TestCase):
    def setUp(self):
        self.stats = Ts65aSlaveStats()
        self.data = MeterData()
        self.data._timestamp = 100.0
        self.data.system.power = -100.0

    def test_add_listener_and_changed(self):
        called = []
        def listener(stats):
            called.append(stats)
        self.stats.add_listener(listener)
        self.stats.changed()
        self.assertEqual(len(called), 1)
        self.assertIs(called[0], self.stats)

    def test_over_feed_in_limit_increments_count(self):
        self.stats.grid_feed_in_hard_limit = 0
        self.data.system.power = -10
        self.stats.check_power_over_feed_in_limit(self.data)
        self.assertEqual(self.stats.power_over_feed_in_limit_count, 1)
        self.assertEqual(self.stats._over_limit_start_time, self.data.timestamp)

    def test_reset_over_limit_timer_accumulates_duration(self):
        self.stats._over_limit_start_time = 50.0
        self.stats.power_over_feed_limit_max_duration_sec = 10.0
        self.stats._reset_over_limit_timer(70.0)
        self.assertEqual(self.stats._over_limit_start_time, None)
        self.assertEqual(self.stats.power_over_feed_limit_max_duration_sec, 20.0)

    def test_check_power_over_feed_in_limit_true(self):
        self.stats.grid_feed_in_hard_limit = 0
        self.data.system.power = -1
        result = self.stats.check_power_over_feed_in_limit(self.data)
        self.assertTrue(result)

    def test_check_power_over_feed_in_limit_false(self):
        self.stats.grid_feed_in_hard_limit = 0
        self.data.system.power = 1
        result = self.stats.check_power_over_feed_in_limit(self.data)
        self.assertFalse(result)

if __name__ == "__main__":
    unittest.main()