from app.utils.serial_activity import SerialActivityTracker


def test_starts_inactive_with_zero_counters():
    t = SerialActivityTracker()
    assert t.active is False
    assert t.connect_count == 0
    assert t.disconnect_count == 0


def test_request_within_window_marks_active_and_counts_connect():
    t = SerialActivityTracker()
    t.record_request(now=100.0)
    t.evaluate(idle_timeout=5.0, now=101.0)

    assert t.active is True
    assert t.connect_count == 1
    assert t.disconnect_count == 0


def test_no_request_keeps_inactive_without_counting():
    t = SerialActivityTracker()
    t.evaluate(idle_timeout=5.0, now=100.0)
    assert t.active is False
    assert t.connect_count == 0


def test_idle_beyond_window_marks_disconnect_once():
    t = SerialActivityTracker()
    t.record_request(now=100.0)
    t.evaluate(idle_timeout=5.0, now=101.0)  # active
    assert t.active is True

    # No further requests; evaluate past the idle window.
    t.evaluate(idle_timeout=5.0, now=110.0)
    assert t.active is False
    assert t.connect_count == 1
    assert t.disconnect_count == 1

    # Staying idle does not re-count the disconnect.
    t.evaluate(idle_timeout=5.0, now=120.0)
    assert t.disconnect_count == 1


def test_reconnect_after_idle_counts_second_connect():
    t = SerialActivityTracker()
    t.record_request(now=100.0)
    t.evaluate(idle_timeout=5.0, now=101.0)
    t.evaluate(idle_timeout=5.0, now=110.0)  # disconnect
    assert t.connect_count == 1
    assert t.disconnect_count == 1

    # Client returns.
    t.record_request(now=200.0)
    t.evaluate(idle_timeout=5.0, now=200.5)
    assert t.active is True
    assert t.connect_count == 2
    assert t.disconnect_count == 1


def test_continuous_activity_does_not_re_count_connect():
    t = SerialActivityTracker()
    for tick in range(10):
        base = 100.0 + tick
        t.record_request(now=base)
        t.evaluate(idle_timeout=5.0, now=base + 0.1)

    assert t.active is True
    assert t.connect_count == 1
    assert t.disconnect_count == 0


def test_boundary_at_exactly_idle_timeout_is_active():
    t = SerialActivityTracker()
    t.record_request(now=100.0)
    # now - last == idle_timeout exactly -> still active (<=).
    t.evaluate(idle_timeout=5.0, now=105.0)
    assert t.active is True
