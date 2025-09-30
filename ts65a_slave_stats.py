import time
from typing import Callable


class Ts65aSlaveStats:
    def __init__(self):
        # Limit in watts, negative value for feed-in limit
        self.grid_feed_in_hard_limit: float = 0
        # Internal state to track when we went over the limit
        self._last_over_limit_time = None

        self.tcp_client_count: int = 0
        self.tcp_client_disconnect_count: int = 0

        self.power_over_feed_in_limit_count: int = 0
        self.power_over_feed_limit_max_duration_sec: float = 0.0

        self._listeners: list[Callable[['Ts65aSlaveStats'], None]] = []

    def changed(self):
        for listener in self._listeners:
            listener(self)

    def add_listener(self, listener: Callable[['Ts65aSlaveStats'], None]):
        self._listeners.append(listener)

    def _over_feed_in_limit(self):
        """ Record when we go over the feed-in limit, and how long we stay over it """
        now = time.time()
        if self._last_over_limit_time is not None:
            # We were already over the limit, so accumulate the duration
            duration = now - self._last_over_limit_time
            self.power_over_feed_limit_max_duration_sec = max(duration,
                                                              self.power_over_feed_limit_max_duration_sec)
        else:
            # Record the first time we go over the limit as an event
            self.power_over_feed_in_limit_count += 1

        self._last_over_limit_time = now


    def _reset_over_limit_timer(self) -> bool:
        """ Reset the over limit timer, and return True if we were previously over the limit """
        if self._last_over_limit_time is not None:
            self._last_over_limit_time = None
            return True
        return False

    def check_power_over_feed_in_limit(self, power: float) -> bool:
        """ Check if the given power is over the feed-in limit, and update stats accordingly.
        Return True if we are currently over the limit."""
        is_over_limit = False
        has_changed = False

        if power < self.grid_feed_in_hard_limit:
            self._over_feed_in_limit()
            is_over_limit = True
        else:
            self._reset_over_limit_timer()

        # notify stats listeners if anything changed
        if has_changed:
            self.changed()

        return is_over_limit


