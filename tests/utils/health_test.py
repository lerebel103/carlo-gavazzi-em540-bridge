from app.utils.health import write_health_heartbeat


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
