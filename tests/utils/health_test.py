from app.utils.health import HealthWatchdog, write_health_heartbeat


def test_write_health_heartbeat_writes_integer_epoch(tmp_path):
    health_file = tmp_path / "em540_health"
    write_health_heartbeat(1_700_000_123.9, str(health_file))
    # Written as truncated integer epoch seconds, matching the shell probe.
    assert health_file.read_text() == "1700000123"


def test_write_health_heartbeat_overwrites_previous_value(tmp_path):
    health_file = tmp_path / "em540_health"
    write_health_heartbeat(1_700_000_000.0, str(health_file))
    write_health_heartbeat(1_700_000_050.0, str(health_file))
    assert health_file.read_text() == "1700000050"


def test_write_health_heartbeat_zero_before_any_frame(tmp_path):
    health_file = tmp_path / "em540_health"
    write_health_heartbeat(0.0, str(health_file))
    # Zero reads as maximally stale, which the healthcheck treats as unhealthy.
    assert health_file.read_text() == "0"


def test_write_health_heartbeat_swallows_write_errors():
    # Unwritable path (missing parent dir) must not raise — best-effort by design.
    write_health_heartbeat(1_700_000_000.0, "/nonexistent-dir-xyz/em540_health")


def test_watchdog_run_once_writes_heartbeat_and_does_not_exit_when_fresh(tmp_path):
    health_file = tmp_path / "em540_health"
    exited = []
    wd = HealthWatchdog(
        read_last_frame_monotonic=lambda: 100.0,
        read_last_frame_wall_clock=lambda: 1_700_000_200.0,
        should_exit=lambda last_mono, now_mono: False,
        poll_interval_s=1.0,
        on_stale=lambda: exited.append(True),
        heartbeat_path=str(health_file),
        monotonic=lambda: 105.0,
    )
    wd.run_once()

    assert health_file.read_text() == "1700000200"
    assert exited == []


def test_watchdog_run_once_invokes_on_stale_when_should_exit_true(tmp_path):
    health_file = tmp_path / "em540_health"
    seen = {}
    exited = []
    wd = HealthWatchdog(
        read_last_frame_monotonic=lambda: 10.0,
        read_last_frame_wall_clock=lambda: 1_700_000_000.0,
        should_exit=lambda last_mono, now_mono: seen.update(last=last_mono, now=now_mono) or True,
        poll_interval_s=1.0,
        on_stale=lambda: exited.append(True),
        heartbeat_path=str(health_file),
        monotonic=lambda: 999.0,
    )
    wd.run_once()

    # should_exit received the monotonic frame time and monotonic "now".
    assert seen == {"last": 10.0, "now": 999.0}
    # Heartbeat still written (observability), and on_stale fired.
    assert health_file.read_text() == "1700000000"
    assert exited == [True]


def test_watchdog_still_writes_heartbeat_when_wall_clock_reader_zero(tmp_path):
    # Pre-first-frame: wall-clock reader returns 0 -> heartbeat is "0" (stale).
    health_file = tmp_path / "em540_health"
    wd = HealthWatchdog(
        read_last_frame_monotonic=lambda: 0.0,
        read_last_frame_wall_clock=lambda: 0.0,
        should_exit=lambda last_mono, now_mono: False,
        poll_interval_s=1.0,
        on_stale=lambda: None,
        heartbeat_path=str(health_file),
        monotonic=lambda: 5.0,
    )
    wd.run_once()
    assert health_file.read_text() == "0"
