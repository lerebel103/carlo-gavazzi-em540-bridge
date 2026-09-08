import math
import unittest
from unittest.mock import MagicMock

from app.fronius.ts65a_data import RunningAverage, Ts65aMeterData


class TestRunningAverage(unittest.TestCase):
    def test_initial_mean_is_zero(self):
        avg = RunningAverage(3)
        self.assertEqual(avg.mean, 0.0)

    def test_add_and_mean(self):
        avg = RunningAverage(3)
        avg.add(10)
        self.assertEqual(avg.mean, 10.0)
        avg.add(20)
        self.assertEqual(avg.mean, 15.0)
        avg.add(30)
        self.assertEqual(avg.mean, 20.0)

    def test_max_points_limit(self):
        avg = RunningAverage(2)
        avg.add(5)
        avg.add(15)
        avg.add(25)  # Should remove 5
        self.assertEqual(avg.mean, (15 + 25) / 2)

    def test_reset(self):
        avg = RunningAverage(2)
        avg.add(1)
        avg.add(2)
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


class TestTs65aMeterData(unittest.TestCase):
    def setUp(self):
        self.stats = MagicMock()
        self.stats.check_power_over_feed_in_limit.return_value = False
        self.logger = MagicMock()
        self.data = MockData()
        self.meter = Ts65aMeterData(3, -1000, self.logger, self.stats)

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
        # After reset, running averages should be reset to only the latest value
        self.assertEqual(len(self.meter._power.values), 1)
        self.assertEqual(self.meter._power.values[0], -1001)

    # Add a unit test to ensure that power is negative when the system power factor is negative
    def test_power_factor_sign_matches_system_power_mean(self):
        # Feed several negative values
        for _ in range(5):
            self.data.system.power = -500
            self.data.system.power_factor = -0.8
            self.meter.update(self.data)
        self.assertLess(self.meter.power, 0)
        self.assertLess(self.meter.power_factor, 0)

        # Feed several positive values
        for _ in range(5):
            self.data.system.power = 500
            self.data.system.power_factor = 0.8
            self.meter.update(self.data)
        self.assertGreater(self.meter.power, 0)
        self.assertGreater(self.meter.power_factor, 0)

    # Do the same for a, b, and c phases
    def test_phase_power_factor_sign_matches_phase_power_mean(self):
        # Feed several negative values for phase A
        for _ in range(5):
            self.data.phases[0].power = -200
            self.data.phases[0].power_factor = -0.9
            self.meter.update(self.data)
        self.assertLess(self.meter.power_a, 0)
        self.assertLess(self.meter.power_factor_a, 0)

        # Feed several positive values for phase A
        for _ in range(5):
            self.data.phases[0].power = 200
            self.data.phases[0].power_factor = 0.9
            self.meter.update(self.data)
        self.assertGreater(self.meter.power_a, 0)
        self.assertGreater(self.meter.power_factor_a, 0)

        # Repeat for phase B
        for _ in range(5):
            self.data.phases[1].power = -300
            self.data.phases[1].power_factor = -0.85
            self.meter.update(self.data)
        self.assertLess(self.meter.power_b, 0)
        self.assertLess(self.meter.power_factor_b, 0)

        for _ in range(5):
            self.data.phases[1].power = 300
            self.data.phases[1].power_factor = 0.85
            self.meter.update(self.data)
        self.assertGreater(self.meter.power_b, 0)
        self.assertGreater(self.meter.power_factor_b, 0)

        # Repeat for phase C
        for _ in range(5):
            self.data.phases[2].power = -400
            self.data.phases[2].power_factor = -0.75
            self.meter.update(self.data)
        self.assertLess(self.meter.power_c, 0)
        self.assertLess(self.meter.power_factor_c, 0)

        for _ in range(5):
            self.data.phases[2].power = 400
            self.data.phases[2].power_factor = 0.75
            self.meter.update(self.data)
        self.assertGreater(self.meter.power_c, 0)
        self.assertGreater(self.meter.power_factor_c, 0)

    def test_reconfigure_updates_max_points_and_grid_feed_in_limit(self):
        self.meter.update(self.data)
        self.meter.update(self.data)

        self.meter.reconfigure(1, -2500)
        self.data.system.power = 321
        self.meter.update(self.data)

        self.assertEqual(self.meter.stats.grid_feed_in_hard_limit, -2500)
        self.assertEqual(self.meter._power.max_points, 1)
        self.assertEqual(len(self.meter._power.values), 1)

    def test_power_triangle_consistent_under_rotating_load(self):
        """S and PF must satisfy the power triangle on the smoothed output.

        Regression test for the independent-averaging bug: when the real/reactive
        split rotates across the averaging window, smoothing S and PF in their own
        windows made S >> sqrt(P^2 + Q^2) and PF implausibly low. Deriving S and PF
        from the smoothed P/Q must keep S == sqrt(P^2 + Q^2) and PF == P / S for the
        system and every phase, regardless of how the load moves within the window.
        """
        # A window (max_points=3) where each frame has the SAME apparent magnitude
        # but a rotating P/Q split — the worst case for independent averaging.
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
