class SerialActivityTracker:
    """Infers downstream serial-client presence from request activity.

    A physical Modbus/RTU serial line has no transport-level connect/disconnect
    events: the port is always open and pymodbus's trace_connect only fires on
    port open/teardown, not when a downstream client (e.g. a Fronius inverter)
    starts or stops polling. The only reliable signal that a client is present
    is recent request traffic.

    This tracker records the timestamp of the last serial request and, when
    evaluated at the diagnostics cadence, derives:

      - ``active``: whether a request arrived within ``idle_timeout`` seconds.
      - ``connect_count`` / ``disconnect_count``: incremented on the rising
        (idle -> active) and falling (active -> idle) edges of ``active``, so
        they count observed client sessions over time.

    Concurrency (lock-free, matching the master's snapshot model): ``record_request``
    is called from the serial PDU trace hook on the server thread and must not
    block that hot path, so it only publishes a single timestamp (an atomic
    reference assignment under CPython's GIL). ``evaluate`` is the sole writer of
    ``active`` and the counters and runs at the diagnostics cadence; the MQTT
    publish thread reads ``active``/counters without a lock (atomic bool/int
    reads). No lock is held on the request path, so serial request processing is
    never blocked by diagnostics evaluation. Timestamps use a monotonic clock
    supplied by the caller so evaluation is deterministic in tests.
    """

    def __init__(self) -> None:
        self._last_request_monotonic: float | None = None
        self.active: bool = False
        self.connect_count: int = 0
        self.disconnect_count: int = 0

    def record_request(self, now: float) -> None:
        """Record that a serial request was observed at monotonic time ``now``.

        Lock-free hot path: a single atomic timestamp publication. See the class
        docstring for the concurrency model.
        """
        self._last_request_monotonic = now

    def evaluate(self, idle_timeout: float, now: float) -> None:
        """Recompute ``active`` and update edge counters.

        ``active`` is True when a request was seen within ``idle_timeout``
        seconds of ``now``. A False->True transition increments connect_count;
        a True->False transition increments disconnect_count. Sole writer of
        ``active``/counters; does not block ``record_request``.
        """
        last = self._last_request_monotonic
        is_active = last is not None and (now - last) <= idle_timeout

        if is_active and not self.active:
            self.connect_count += 1
        elif not is_active and self.active:
            self.disconnect_count += 1

        self.active = is_active
