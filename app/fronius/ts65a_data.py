import collections
import math

from app.fronius.ts65a_slave_stats import Ts65aSlaveStats


class RunningAverage:
    __slots__ = ("max_points", "values")

    def __init__(self, max_points):
        self.max_points = max_points
        self.values = collections.deque(maxlen=max_points)

    def add(self, value):
        self.values.append(value)

    @property
    def mean(self):
        n = len(self.values)
        if n == 0:
            return 0.0
        return sum(self.values) / n

    def reset(self):
        self.values.clear()

    def set_max_points(self, max_points):
        if max_points == self.max_points:
            return
        self.max_points = max_points
        self.values = collections.deque(self.values, maxlen=max_points)


def _derive_power_factor(power, reactive_power):
    """Derive power factor from (smoothed) real and reactive power.

    PF = P / S where S = sqrt(P^2 + Q^2). Deriving PF from the same P and Q used
    for the apparent-power calculation keeps the served frame internally
    consistent (PF, P, Q and S always satisfy the power triangle). The sign of
    the result follows the sign of real power naturally, so no separate sign
    handling is needed. Returns 1.0 when there is no apparent power (idle) to
    avoid division by zero.
    """
    apparent = math.hypot(power, reactive_power)
    if apparent == 0.0:
        return 1.0
    return power / apparent


class Ts65aMeterData:
    """Class to hold TS65A meter data with running averages.

    The reason for running averages is to provide smoother control of feed in set points,
    particularly with equipment that have pulsating power requirements (PID driven heat elments, some A/Cs like Actron)
    """

    def __init__(self, max_points, grid_feed_in_hard_limit, logger, stats: Ts65aSlaveStats):
        self.stats = stats
        self.stats.grid_feed_in_hard_limit = grid_feed_in_hard_limit
        self.logger = logger

        self._current_an = RunningAverage(max_points)
        self._current_a = RunningAverage(max_points)
        self._current_b = RunningAverage(max_points)
        self._current_c = RunningAverage(max_points)
        self._voltage_ln = RunningAverage(max_points)
        self._voltage_ln_a = RunningAverage(max_points)
        self._voltage_ln_b = RunningAverage(max_points)
        self._voltage_ln_c = RunningAverage(max_points)
        self._voltage_ll = RunningAverage(max_points)
        self._voltage_ll_a = RunningAverage(max_points)
        self._voltage_ll_b = RunningAverage(max_points)
        self._voltage_ll_c = RunningAverage(max_points)
        self._frequency = RunningAverage(max_points)
        self._power = RunningAverage(max_points)
        self._power_a = RunningAverage(max_points)
        self._power_b = RunningAverage(max_points)
        self._power_c = RunningAverage(max_points)
        self._apparent_power = RunningAverage(max_points)
        self._apparent_power_a = RunningAverage(max_points)
        self._apparent_power_b = RunningAverage(max_points)
        self._apparent_power_c = RunningAverage(max_points)
        self._reactive_power = RunningAverage(max_points)
        self._reactive_power_a = RunningAverage(max_points)
        self._reactive_power_b = RunningAverage(max_points)
        self._reactive_power_c = RunningAverage(max_points)
        # NOTE: apparent power (S) and power factor (PF) are intentionally NOT
        # smoothed with their own running averages. They are derived from the
        # smoothed real/reactive power (see the apparent_power / power_factor
        # properties) so the served power triangle stays internally consistent.

        # we don't do running average for kWh, just keep adding the latest value
        self._wh_neg_total = 0
        self._wh_neg_a = 0
        self._wh_neg_b = 0
        self._wh_neg_c = 0
        self._wh_plus_total = 0
        self._wh_plus_l1 = 0
        self._wh_plus_l2 = 0
        self._wh_plus_l3 = 0
        self._vah_neg_total = 0
        self._vah_neg_a = 0
        self._vah_neg_b = 0
        self._vah_neg_c = 0
        self._vah_plus_total = 0
        self._vah_plus_a = 0
        self._vah_plus_b = 0
        self._vah_plus_c = 0

    @property
    def current_an(self):
        return self._current_an.mean

    @property
    def current_a(self):
        return self._current_a.mean

    @property
    def current_b(self):
        return self._current_b.mean

    @property
    def current_c(self):
        return self._current_c.mean

    @property
    def voltage_ln(self):
        return self._voltage_ln.mean

    @property
    def voltage_ln_a(self):
        return self._voltage_ln_a.mean

    @property
    def voltage_ln_b(self):
        return self._voltage_ln_b.mean

    @property
    def voltage_ln_c(self):
        return self._voltage_ln_c.mean

    @property
    def voltage_ll(self):
        return self._voltage_ll.mean

    @property
    def voltage_ll_a(self):
        return self._voltage_ll_a.mean

    @property
    def voltage_ll_b(self):
        return self._voltage_ll_b.mean

    @property
    def voltage_ll_c(self):
        return self._voltage_ll_c.mean

    @property
    def frequency(self):
        return self._frequency.mean

    @property
    def power(self):
        return self._power.mean

    @property
    def power_a(self):
        return self._power_a.mean

    @property
    def power_b(self):
        return self._power_b.mean

    @property
    def power_c(self):
        return self._power_c.mean

    # Apparent power and power factor are DERIVED from the smoothed real (P) and
    # reactive (Q) power rather than smoothed independently. Averaging S and PF
    # in their own windows de-correlates them from P and Q (the mean of a
    # magnitude is not the magnitude of the means), which produced a physically
    # impossible power triangle (S far larger than sqrt(P^2 + Q^2), and an
    # implausibly low PF). Deriving them here guarantees, on every served frame:
    #   S  = sqrt(mean(P)^2 + mean(Q)^2)
    #   PF = mean(P) / S            (sign follows real-power direction naturally)
    # Note this is the *fundamental* apparent power; on real hardware S may differ
    # slightly from the meter's measured S under high harmonic distortion, but it
    # is internally coherent, which is what downstream SunSpec consumers expect.
    @property
    def apparent_power(self):
        return math.hypot(self._power.mean, self._reactive_power.mean)

    @property
    def apparent_power_a(self):
        return math.hypot(self._power_a.mean, self._reactive_power_a.mean)

    @property
    def apparent_power_b(self):
        return math.hypot(self._power_b.mean, self._reactive_power_b.mean)

    @property
    def apparent_power_c(self):
        return math.hypot(self._power_c.mean, self._reactive_power_c.mean)

    @property
    def reactive_power(self):
        return self._reactive_power.mean

    @property
    def reactive_power_a(self):
        return self._reactive_power_a.mean

    @property
    def reactive_power_b(self):
        return self._reactive_power_b.mean

    @property
    def reactive_power_c(self):
        return self._reactive_power_c.mean

    @property
    def power_factor(self):
        return _derive_power_factor(self._power.mean, self._reactive_power.mean)

    @property
    def power_factor_a(self):
        return _derive_power_factor(self._power_a.mean, self._reactive_power_a.mean)

    @property
    def power_factor_b(self):
        return _derive_power_factor(self._power_b.mean, self._reactive_power_b.mean)

    @property
    def power_factor_c(self):
        return _derive_power_factor(self._power_c.mean, self._reactive_power_c.mean)

    @property
    def wh_neg_total(self):
        return self._wh_neg_total

    @property
    def wh_neg_a(self):
        return self._wh_neg_a

    @property
    def wh_neg_b(self):
        return self._wh_neg_b

    @property
    def wh_neg_c(self):
        return self._wh_neg_c

    @property
    def wh_plus_total(self):
        return self._wh_plus_total

    @property
    def wh_plus_l1(self):
        return self._wh_plus_l1

    @property
    def wh_plus_l2(self):
        return self._wh_plus_l2

    @property
    def wh_plus_l3(self):
        return self._wh_plus_l3

    @property
    def vah_neg_total(self):
        return self._vah_neg_total

    @property
    def vah_neg_a(self):
        return self._vah_neg_a

    @property
    def vah_neg_b(self):
        return self._vah_neg_b

    @property
    def vah_neg_c(self):
        return self._vah_neg_c

    @property
    def vah_plus_total(self):
        return self._vah_plus_total

    @property
    def vah_plus_a(self):
        return self._vah_plus_a

    @property
    def vah_plus_b(self):
        return self._vah_plus_b

    @property
    def vah_plus_c(self):
        return self._vah_plus_c

    def update(self, data):
        # if we are over the feedback hard_limit, reset all running averages to current values and update stats
        if self.stats.check_power_over_feed_in_limit(data):
            self.logger.debug(f"Power over the feed in limit reached: {self.power}W")
            self._reset_means()

        # Update all running averages with new data
        self._current_an.add(data.system.An)
        self._current_a.add(data.phases[0].current)
        self._current_b.add(data.phases[1].current)
        self._current_c.add(data.phases[2].current)
        self._voltage_ln.add(data.system.line_neutral_voltage)
        self._voltage_ln_a.add(data.phases[0].line_neutral_voltage)
        self._voltage_ln_b.add(data.phases[1].line_neutral_voltage)
        self._voltage_ln_c.add(data.phases[2].line_neutral_voltage)
        self._voltage_ll.add(data.system.line_line_voltage)
        self._voltage_ll_a.add(data.phases[0].line_line_voltage)
        self._voltage_ll_b.add(data.phases[1].line_line_voltage)
        self._voltage_ll_c.add(data.phases[2].line_line_voltage)
        self._frequency.add(data.system.frequency)
        self._power.add(data.system.power)
        self._power_a.add(data.phases[0].power)
        self._power_b.add(data.phases[1].power)
        self._power_c.add(data.phases[2].power)
        self._reactive_power.add(data.system.reactive_power)
        self._reactive_power_a.add(data.phases[0].reactive_power)
        self._reactive_power_b.add(data.phases[1].reactive_power)
        self._reactive_power_c.add(data.phases[2].reactive_power)
        # Apparent power and power factor are derived from the smoothed P/Q above
        # (see the apparent_power / power_factor properties); the meter's own S
        # and PF registers are intentionally not smoothed independently.

        # And now all fixed values
        # export / import energy in Wh
        self._wh_neg_total = data.other_energies.kwh_neg_total * 1000.0  # convert to Wh
        self._wh_neg_a = 0  # Not available from em540
        self._wh_neg_b = 0  # Not available from em540
        self._wh_neg_c = 0  # Not available from em540
        self._wh_plus_total = data.other_energies.kwh_plus_total * 1000.0  # convert to Wh
        self._wh_plus_l1 = data.other_energies.kwh_plus_l1 * 1000.0  # convert to Wh
        self._wh_plus_l2 = data.other_energies.kwh_plus_l2 * 1000.0  # convert to Wh
        self._wh_plus_l3 = data.other_energies.kwh_plus_l3 * 1000.0  # convert to Wh

        # export / import energy in VAh
        # EM540 provides only total apparent energy (kvah_total), not directional import/export.
        # Intentionally zero to avoid ambiguity in TS65A semantics.
        self._vah_neg_total = 0
        self._vah_neg_a = 0
        self._vah_neg_b = 0
        self._vah_neg_c = 0
        self._vah_plus_total = 0
        self._vah_plus_a = 0
        self._vah_plus_b = 0
        self._vah_plus_c = 0

    def reconfigure(self, max_points, grid_feed_in_hard_limit):
        self.stats.grid_feed_in_hard_limit = grid_feed_in_hard_limit

        for attr_name in (
            "_current_an",
            "_current_a",
            "_current_b",
            "_current_c",
            "_voltage_ln",
            "_voltage_ln_a",
            "_voltage_ln_b",
            "_voltage_ln_c",
            "_voltage_ll",
            "_voltage_ll_a",
            "_voltage_ll_b",
            "_voltage_ll_c",
            "_frequency",
            "_power",
            "_power_a",
            "_power_b",
            "_power_c",
            "_reactive_power",
            "_reactive_power_a",
            "_reactive_power_b",
            "_reactive_power_c",
        ):
            getattr(self, attr_name).set_max_points(max_points)

    def _reset_means(self):
        self.logger.debug("Resetting running averages due to power over feed in limit")

        # Reset all running averages to current values
        self._current_an.reset()
        self._current_a.reset()
        self._current_b.reset()
        self._current_c.reset()
        self._voltage_ln.reset()
        self._voltage_ln_a.reset()
        self._voltage_ln_b.reset()
        self._voltage_ln_c.reset()
        self._voltage_ll.reset()
        self._voltage_ll_a.reset()
        self._voltage_ll_b.reset()
        self._voltage_ll_c.reset()
        self._frequency.reset()
        self._power.reset()
        self._power_a.reset()
        self._power_b.reset()
        self._power_c.reset()
        self._reactive_power.reset()
        self._reactive_power_a.reset()
        self._reactive_power_b.reset()
        self._reactive_power_c.reset()
