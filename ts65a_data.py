import collections

from ts65a_slave_stats import Ts65aSlaveStats


class RunningAverage:
    __slots__ = ('max_points', 'values')

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
        self._power_factor = RunningAverage(max_points)
        self._power_factor_a = RunningAverage(max_points)
        self._power_factor_b = RunningAverage(max_points)
        self._power_factor_c = RunningAverage(max_points)

        # we don't do running average for kWh, just keep adding the latest value
        self._kwh_neg_total = 0
        self._kwh_neg_a = 0
        self._kwh_neg_b = 0
        self._kwh_neg_c = 0
        self._kwh_plus_total = 0
        self._kwh_plus_l1 = 0
        self._kwh_plus_l2 = 0
        self._kwh_plus_l3 = 0

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

    @property
    def apparent_power(self):
        return self._apparent_power.mean

    @property
    def apparent_power_a(self):
        return self._apparent_power_a.mean

    @property
    def apparent_power_b(self):
        return self._apparent_power_b.mean

    @property
    def apparent_power_c(self):
        return self._apparent_power_c.mean

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
        return self._power_factor.mean

    @property
    def power_factor_a(self):
        return self._power_factor_a.mean

    @property
    def power_factor_b(self):
        return self._power_factor_b.mean

    @property
    def power_factor_c(self):
        return self._power_factor_c.mean

    @property
    def kwh_neg_total(self):
        return self._kwh_neg_total

    @property
    def kwh_neg_a(self):
        return self._kwh_neg_a

    @property
    def kwh_neg_b(self):
        return self._kwh_neg_b

    @property
    def kwh_neg_c(self):
        return self._kwh_neg_c

    @property
    def kwh_plus_total(self):
        return self._kwh_plus_total

    @property
    def kwh_plus_l1(self):
        return self._kwh_plus_l1

    @property
    def kwh_plus_l2(self):
        return self._kwh_plus_l2

    @property
    def kwh_plus_l3(self):
        return self._kwh_plus_l3

    def update(self, data):
        # if we are over the feedback hard_limit, reset all running averages to current values and update stats
        if self.stats.check_power_over_feed_in_limit(data):
            self.logger.warn(f'Power over the feed in limit reached: {self.power}W')
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
        self._apparent_power.add(data.system.apparent_power)
        self._apparent_power_a.add(data.phases[0].apparent_power)
        self._apparent_power_b.add(data.phases[1].apparent_power)
        self._apparent_power_c.add(data.phases[2].apparent_power)
        self._reactive_power.add(data.system.reactive_power)
        self._reactive_power_a.add(data.phases[0].reactive_power)
        self._reactive_power_b.add(data.phases[1].reactive_power)
        self._reactive_power_c.add(data.phases[2].reactive_power)
        self._power_factor.add(data.system.power_factor)
        self._power_factor_a.add(data.phases[0].power_factor)
        self._power_factor_b.add(data.phases[1].power_factor)
        self._power_factor_c.add(data.phases[2].power_factor)

        # And now all fixed values
        self._kwh_neg_total = data.other_energies.kwh_neg_total
        self._kwh_neg_a = 0
        self._kwh_neg_b = 0
        self._kwh_neg_c = 0
        self._kwh_plus_total = data.other_energies.kwh_plus_total
        self._kwh_plus_l1 = data.other_energies.kwh_plus_l1
        self._kwh_plus_l2 = data.other_energies.kwh_plus_l2
        self._kwh_plus_l3 = data.other_energies.kwh_plus_l3

    def _reset_means(self):
        self.logger.warn("Resetting running averages due to power over feed in limit")

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
        self._apparent_power.reset()
        self._apparent_power_a.reset()
        self._apparent_power_b.reset()
        self._apparent_power_c.reset()
        self._reactive_power.reset()
        self._reactive_power_a.reset()
        self._reactive_power_b.reset()
        self._reactive_power_c.reset()
        self._power_factor.reset()
        self._power_factor_a.reset()
        self._power_factor_b.reset()
        self._power_factor_c.reset()

