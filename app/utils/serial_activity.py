import threading


class SerialActivityTracker:
    """Infers downstream serial-client presence from request activity.

    A physical Modbus/RTU serial line has no transport-level connect/disconnect
    events: the port is always open and pymodbus's trace_connect only fires on
    port open/teardown, not when a downstream client (e.g. a Fronius inverter)
    starts or stops polling. The only reliable signal that a client is present
    is recent request traffic.

    This tracker records the timestamp of the last serial request (a cheap,
    lock-guarded write on the server thread) and, when evaluated at the
    diagnostics cadence, derives:

      - ``active``: whether a request arrived within ``idle_timeout`` seconds.
      - ``connect_count`` / ``disconnect_count``: incremented on the rising
        (idle -> active) and falling (active -> idle) edges of ``active``, so
        they count observed client sessions over time.

    Edge detection is intentionally done at evaluation time rather than on the
    hot request path, keeping the request path to a single timestamp write.
    Timestamps use a monotonic clock supplied by the caller so evaluation is
    deterministic in tests.
    """

    def __init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()
        self._last_request_monotonic: float | None = None
        self.active: bool = False
        self.connect_count: int = 0
        self.disconnect_count: int = 0

    def record_request(self, now: float) -> None:
        """Record that a serial request was observed at monotonic time ``now``."""
        with self._lock:
            self._last_request_monotonic = now

    def evaluate(self, idle_timeout: float, now: float) -> None:
        """Recompute ``active`` and update edge counters.

        ``active`` is True when a request was seen within ``idle_timeout``
        seconds of ``now``. A False->True transition increments connect_count;
        a True->False transition increments disconnect_count.
        """
        with self._lock:
            last = self._last_request_monotonic
            is_active = last is not None and (now - last) <= idle_timeout

            if is_active and not self.active:
                self.connect_count += 1
            elif not is_active and self.active:
                self.disconnect_count += 1

            self.active = is_active
