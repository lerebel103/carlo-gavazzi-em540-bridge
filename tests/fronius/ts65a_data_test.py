import math
import unittest
from unittest.mock import MagicMock

from app.fronius.ts65a_data import RunningAverage, Ts65aMeterData


class TestRunningAverage(unittest.TestCase):
    def test_initial_mean_is_zero(self):
        avg = RunningAverage(3.0)
        self.assertEqual(avg.mean, 0.0)

    def test_add_and_mean_within_window(self):
        # 10s window; samples at t=0,1,2 all fall inside it.
        avg = RunningAverage(10.0)
        avg.add(10, 0.0)
        self.assertEqual(avg.mean, 10.0)
        avg.add(20, 1.0)
        self.assertEqual(avg.mean, 15.0)
        avg.add(30, 2.0)
        self.assertEqual(avg.mean, 20.0)

    def test_evicts_samples_older_than_window(self):
        # 2s window: a sample at t=0 is dropped once a sample at t=2.5 arrives.
        avg = RunningAverage(2.0)
        avg.add(5, 0.0)
        avg.add(15, 1.0)
        avg.add(25, 2.5)  # cutoff = 0.5, so t=0 (value 5) is evicted
        self.assertEqual(avg.mean, (15 + 25) / 2)

    def test_window_zero_keeps_only_latest(self):
        # Window of 0 disables smoothing: mean == latest instantaneous value.
        avg = RunningAverage(0.0)
        avg.add(5, 0.0)
        self.assertEqual(avg.mean, 5.0)
        avg.add(15, 0.1)
        self.assertEqual(avg.mean, 15.0)
        avg.add(25, 0.2)
        self.assertEqual(avg.mean, 25.0)

    def test_set_window_shrink_applies_immediately(self):
        avg = RunningAverage(10.0)
        avg.add(10, 0.0)
        avg.add(20, 1.0)
        avg.add(30, 2.0)
        self.assertEqual(avg.mean, 20.0)
        # Shrink to 0.5s relative to newest sample (t=2.0) -> only t=2.0 remains.
        avg.set_window(0.5)
        self.assertEqual(avg.mean, 30.0)

    def test_reset(self):
        avg = RunningAverage(2.0)
        avg.add(1, 0.0)
        avg.add(2, 0.5)
        avg.reset()
        self.assertEqual(avg.mean, 0.0)
        self.assertEqual(len(avg.values), 0)


class MockPhase:
    def __init__(
        self,
        current=1,
        line_neutral_voltage=2,
        line_line_voltage=3,
        power=4,
        apparent_power=5,
        reactive_power=6,
        power_factor=0.7,
    ):
        self.current = current
        self.line_neutral_voltage = line_neutral_voltage
        self.line_line_voltage = line_line_voltage
        self.power = power
        self.apparent_power = apparent_power
        self.reactive_power = reactive_power
        self.power_factor = power_factor


class MockSystem:
    def __init__(self):
        self.An = 10
        self.line_neutral_voltage = 20
        self.line_line_voltage = 30
        self.frequency = 50
        self.power = 100
        self.apparent_power = 110
        self.reactive_power = 120
        self.power_factor = 0.95


class MockOtherEnergies:
    def __init__(self):
        self.kwh_neg_total = 1
        self.kwh_plus_total = 2
        self.kwh_plus_l1 = 3
        self.kwh_plus_l2 = 4
        self.kwh_plus_l3 = 5


class MockData:
    def __init__(self):
        self.system = MockSystem()
        self.phases = [MockPhase(), MockPhase(), MockPhase()]
        self.other_energies = MockOtherEnergies()
        # Upstream frame time; drives the time-windowed averages.
        self.timestamp = 0.0


class TestTs65aMeterData(unittest.TestCase):
    def setUp(self):
        self.stats = MagicMock()
        self.stats.check_power_over_feed_in_limit.return_value = False
        self.logger = MagicMock()
        self.data = MockData()
        # 10s smoothing window; tests feed samples at the same/close timestamps
        # so they remain within the window and the mean equals the fed value.
        self.meter = Ts65aMeterData(10.0, -1000, self.logger, self.stats)

    def test_initial_values(self):
        self.assertEqual(self.meter.wh_neg_total, 0)
        self.assertEqual(self.meter.wh_plus_total, 0)
        self.assertEqual(self.meter.current_an, 0.0)
        self.assertEqual(self.meter.voltage_ln, 0.0)

    def test_update_running_averages(self):
        self.meter.update(self.data)
        self.assertEqual(self.meter.current_an, 10.0)
        self.assertEqual(self.meter.voltage_ln, 20.0)
        self.assertEqual(self.meter.frequency, 50.0)
        self.assertEqual(self.meter.power, 100.0)
        self.assertEqual(self.meter.wh_neg_total, 1000)
        self.assertEqual(self.meter.wh_plus_total, 2000)
        self.assertEqual(self.meter.wh_plus_l1, 3000)
        self.assertEqual(self.meter.wh_plus_l2, 4000)
        self.assertEqual(self.meter.wh_plus_l3, 5000)
        self.assertEqual(self.meter.voltage_ll, 30.0)
        self.assertEqual(self.meter.power_a, 4.0)
        self.assertEqual(self.meter.reactive_power, 120.0)
        # Apparent power and power factor are DERIVED from smoothed P/Q, not the
        # meter's raw S (110) / PF (0.95). S = hypot(100, 120); PF = P / S.
        expected_s = math.hypot(100.0, 120.0)
        self.assertAlmostEqual(self.meter.apparent_power, expected_s, places=6)
        self.assertAlmostEqual(self.meter.power_factor, 100.0 / expected_s, places=6)

    def test_reset_means_called_on_limit(self):
        self.stats.check_power_over_feed_in_limit.return_value = False
        self.meter.update(self.data)
        self.meter.update(self.data)
        self.meter.update(self.data)
        self.logger.debug.assert_not_called()
        self.assertEqual(len(self.meter._power.values), 3)

        # Now go above the limit, and we should see a reset and the latest values stored
        self.stats.check_power_over_feed_in_limit.return_value = True
        self.data.system.power = -1001
        self.meter.update(self.data)

        self.logger.debug.assert_called()
        # After reset, running averages should be reset to only the latest value.
        # values are (timestamp, value) tuples under the time-windowed average.
        self.assertEqual(len(self.meter._power.values), 1)
        self.assertEqual(self.meter._power.values[0][1], -1001)

    # Add a unit test to ensure that power is negative when the system power factor is negative
    def test_power_factor_sign_matches_system_power_mean(self):
        # Use an instantaneous (no-smoothing) window so the served sign reflects
        # the most recent sample without depending on window roll-off timing.
        meter = Ts65aMeterData(0.0, -1000, self.logger, self.stats)
        # Feed several negative values
        for _ in range(5):
            self.data.system.power = -500
            self.data.system.power_factor = -0.8
            meter.update(self.data)
        self.assertLess(meter.power, 0)
        self.assertLess(meter.power_factor, 0)

        # Feed several positive values
        for _ in range(5):
            self.data.system.power = 500
            self.data.system.power_factor = 0.8
            meter.update(self.data)
        self.assertGreater(meter.power, 0)
        self.assertGreater(meter.power_factor, 0)

    # Do the same for a, b, and c phases
    def test_phase_power_factor_sign_matches_phase_power_mean(self):
        # Instantaneous window; sign follows the latest sample (see above).
        meter = Ts65aMeterData(0.0, -1000, self.logger, self.stats)
        # Feed several negative values for phase A
        for _ in range(5):
            self.data.phases[0].power = -200
            self.data.phases[0].power_factor = -0.9
            meter.update(self.data)
        self.assertLess(meter.power_a, 0)
        self.assertLess(meter.power_factor_a, 0)

        # Feed several positive values for phase A
        for _ in range(5):
            self.data.phases[0].power = 200
            self.data.phases[0].power_factor = 0.9
            meter.update(self.data)
        self.assertGreater(meter.power_a, 0)
        self.assertGreater(meter.power_factor_a, 0)

        # Repeat for phase B
        for _ in range(5):
            self.data.phases[1].power = -300
            self.data.phases[1].power_factor = -0.85
            meter.update(self.data)
        self.assertLess(meter.power_b, 0)
        self.assertLess(meter.power_factor_b, 0)

        for _ in range(5):
            self.data.phases[1].power = 300
            self.data.phases[1].power_factor = 0.85
            meter.update(self.data)
        self.assertGreater(meter.power_b, 0)
        self.assertGreater(meter.power_factor_b, 0)

        # Repeat for phase C
        for _ in range(5):
            self.data.phases[2].power = -400
            self.data.phases[2].power_factor = -0.75
            meter.update(self.data)
        self.assertLess(meter.power_c, 0)
        self.assertLess(meter.power_factor_c, 0)

        for _ in range(5):
            self.data.phases[2].power = 400
            self.data.phases[2].power_factor = 0.75
            meter.update(self.data)
        self.assertGreater(meter.power_c, 0)
        self.assertGreater(meter.power_factor_c, 0)

    def test_reconfigure_updates_window_and_grid_feed_in_limit(self):
        # Feed two samples 1s apart inside the current 10s window.
        self.data.timestamp = 0.0
        self.meter.update(self.data)
        self.data.timestamp = 1.0
        self.meter.update(self.data)
        self.assertEqual(len(self.meter._power.values), 2)

        # Shrink the window to 0.5s and update the limit. The shrink applies
        # immediately relative to the newest sample (t=1.0), evicting t=0.0.
        self.meter.reconfigure(0.5, -2500)
        self.assertEqual(self.meter.stats.grid_feed_in_hard_limit, -2500)
        self.assertEqual(self.meter._power.window_seconds, 0.5)
        self.assertEqual(len(self.meter._power.values), 1)

    def test_reconfigure_to_zero_disables_smoothing(self):
        # Prime with a couple of samples, then disable smoothing dynamically.
        self.data.timestamp = 0.0
        self.data.system.power = 100
        self.meter.update(self.data)
        self.data.timestamp = 0.1
        self.data.system.power = 200
        self.meter.update(self.data)

        self.meter.reconfigure(0.0, -1000)
        # With window 0, only the latest sample is retained -> instantaneous.
        self.data.timestamp = 0.2
        self.data.system.power = 321
        self.meter.update(self.data)
        self.assertEqual(self.meter.power, 321)
        self.assertEqual(len(self.meter._power.values), 1)

    def test_power_triangle_consistent_under_rotating_load(self):
        """S and PF must satisfy the power triangle on the smoothed output.

        Regression test for the independent-averaging bug: when the real/reactive
        split rotates across the averaging window, smoothing S and PF in their own
        windows made S >> sqrt(P^2 + Q^2) and PF implausibly low. Deriving S and PF
        from the smoothed P/Q must keep S == sqrt(P^2 + Q^2) and PF == P / S for the
        system and every phase, regardless of how the load moves within the window.
        """
        # Each frame has the SAME apparent magnitude but a rotating P/Q split —
        # the worst case for independent averaging.
        frames = [
            (100.0, 0.0),  # purely real
            (0.0, 100.0),  # purely reactive
            (70.71, 70.71),  # 45 degrees
        ]
        for p, q in frames:
            self.data.system.power = p
            self.data.system.reactive_power = q
            for ph in self.data.phases:
                ph.power = p
                ph.reactive_power = q
            self.meter.update(self.data)

        def assert_triangle(p_val, q_val, s_val, pf_val):
            expected_s = math.hypot(p_val, q_val)
            self.assertAlmostEqual(s_val, expected_s, places=6)
            self.assertGreaterEqual(s_val + 1e-9, abs(p_val))  # S >= |P|
            expected_pf = p_val / expected_s if expected_s else 1.0
            self.assertAlmostEqual(pf_val, expected_pf, places=6)

        assert_triangle(self.meter.power, self.meter.reactive_power, self.meter.apparent_power, self.meter.power_factor)
        assert_triangle(
            self.meter.power_a, self.meter.reactive_power_a, self.meter.apparent_power_a, self.meter.power_factor_a
        )
        assert_triangle(
            self.meter.power_b, self.meter.reactive_power_b, self.meter.apparent_power_b, self.meter.power_factor_b
        )
        assert_triangle(
            self.meter.power_c, self.meter.reactive_power_c, self.meter.apparent_power_c, self.meter.power_factor_c
        )

    def test_power_factor_idle_returns_unity(self):
        """PF is defined as 1.0 when there is no power (avoids divide-by-zero)."""
        self.data.system.power = 0.0
        self.data.system.reactive_power = 0.0
        self.meter.update(self.data)
        self.assertEqual(self.meter.power_factor, 1.0)
        self.assertEqual(self.meter.apparent_power, 0.0)


if __name__ == "__main__":
    unittest.main()
