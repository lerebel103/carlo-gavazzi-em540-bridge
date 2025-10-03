import unittest

from carlo_gavazzi.meter_data import MeterData
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

    # Generate a test that will emulate the power going over the limit for a given duration.
    # Then ensure this is recorded as an event, and the duration is recorded correctly.
    def test_power_over_feed_in_limit_duration(self):
        self.stats.grid_feed_in_hard_limit = 0
        self.data.system.power = -10
        self.stats.check_power_over_feed_in_limit(self.data)
        self.assertEqual(self.stats.power_over_feed_in_limit_count, 1)

        # Simulate time passing while still over the limit
        self.data._timestamp += 5.0
        self.stats.check_power_over_feed_in_limit(self.data)
        self.assertEqual(self.stats.power_over_feed_in_limit_count, 1)
        self.assertEqual(self.stats._over_limit_start_time, 100.0)

        # Now simulate power going back within limits
        self.data.system.power = 10
        self.data._timestamp += 1.0
        self.stats.check_power_over_feed_in_limit(self.data)
        self.assertEqual(self.stats.power_over_feed_in_limit_count, 1)
        self.assertEqual(self.stats.power_over_feed_limit_max_duration_sec, 6.0)
        self.assertIsNone(self.stats._over_limit_start_time)

        # Simulate going over the limit again
        self.data.system.power = -20
        self.data._timestamp += 2.0
        self.stats.check_power_over_feed_in_limit(self.data)
        self.assertEqual(self.stats.power_over_feed_in_limit_count, 2)
        self.assertEqual(self.stats._over_limit_start_time, 108.0)
        # max duration should remain the same
        self.assertEqual(self.stats.power_over_feed_limit_max_duration_sec, 6.0)



if __name__ == "__main__":
    unittest.main()
