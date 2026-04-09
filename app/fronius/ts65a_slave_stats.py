from typing import Callable

from app.carlo_gavazzi.meter_data import MeterData


class Ts65aSlaveStats:
    def __init__(self):
        # Limit in watts, negative value for feed-in limit
        self.grid_feed_in_hard_limit: float = 0
        # Internal state to track when we went over the limit
        self._over_limit_start_time = None

        self.tcp_client_count: int = 0
        self.tcp_client_disconnect_count: int = 0

        self.power_over_feed_in_limit_count: int = 0
        self.power_over_feed_limit_max_duration_sec: float = 0.0

        self._listeners: list[Callable[["Ts65aSlaveStats"], None]] = []

    def changed(self):
        for listener in self._listeners:
            listener(self)

    def add_listener(self, listener: Callable[["Ts65aSlaveStats"], None]):
        self._listeners.append(listener)

    def _over_feed_in_limit(self, timestamp: float):
        """Record when we go over the feed-in limit"""
        if self._over_limit_start_time is None:
            # Record the first time we go over the limit as an event
            self.power_over_feed_in_limit_count += 1
            self._over_limit_start_time = timestamp
            self.changed()

    def _reset_over_limit_timer(self, timestamp: float):
        """Reset the over limit timer, and return True if we were previously over the limit"""
        if self._over_limit_start_time is not None:
            # We were already over the limit, so accumulate the duration
            duration = timestamp - self._over_limit_start_time
            self.power_over_feed_limit_max_duration_sec = max(
                duration, self.power_over_feed_limit_max_duration_sec
            )
            # Reset the timer
            self._over_limit_start_time = None
            self.changed()

    def check_power_over_feed_in_limit(self, data: MeterData) -> bool:
        """Check if the given power is over the feed-in limit, and update stats accordingly.
        Return True if we are currently over the limit."""
        is_over_limit = False

        power = data.system.power
        if power < self.grid_feed_in_hard_limit:
            self._over_feed_in_limit(data.timestamp)
            is_over_limit = True
        else:
            self._reset_over_limit_timer(data.timestamp)

        return is_over_limit
