import json
import socket
import time

import pytest

from app.health import HealthMonitor, evaluate_health, target_rate_hz


def _write_health_file(path, *, acq_rate_hz, target_rate_hz, wall_ts=None):
    payload = {
        "wall_ts": time.time() if wall_ts is None else wall_ts,
        "monotonic_ts": 123.0,
        "acq_rate_hz": acq_rate_hz,
        "target_rate_hz": target_rate_hz,
        "update_interval": (1.0 / target_rate_hz) if target_rate_hz else 0.0,
    }
    path.write_text(json.dumps(payload))
    return payload


@pytest.fixture
def listening_port():
    """A real listening TCP socket so the liveness probe passes."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    yield port
    srv.close()


def _evaluate(path, port, **overrides):
    kwargs = dict(
        file_path=str(path),
        tcp_host="127.0.0.1",
        tcp_port=port,
        rate_margin=0.9,
        unpaced_min_rate_hz=10.0,
        max_file_age_s=5.0,
    )
    kwargs.update(overrides)
    return evaluate_health(**kwargs)


# -- target_rate_hz -----------------------------------------------------------


def test_target_rate_hz_paced():
    assert target_rate_hz(0.1) == pytest.approx(10.0)
    assert target_rate_hz(0.5) == pytest.approx(2.0)


def test_target_rate_hz_unpaced_is_zero():
    assert target_rate_hz(0.0) == 0.0
    assert target_rate_hz(None) == 0.0


# -- evaluate_health: rate on target ------------------------------------------


def test_healthy_when_rate_on_target(tmp_path, listening_port):
    f = tmp_path / "health.json"
    _write_health_file(f, acq_rate_hz=9.6, target_rate_hz=10.0)  # 96% of target
    result = _evaluate(f, listening_port)
    assert result.healthy, result.reason


def test_unhealthy_when_rate_just_below_margin(tmp_path, listening_port):
    f = tmp_path / "health.json"
    _write_health_file(f, acq_rate_hz=8.9, target_rate_hz=10.0)  # 89% < 90%
    result = _evaluate(f, listening_port)
    assert not result.healthy
    assert "rate too low" in result.reason


def test_exactly_at_margin_is_healthy(tmp_path, listening_port):
    f = tmp_path / "health.json"
    _write_health_file(f, acq_rate_hz=9.0, target_rate_hz=10.0)  # exactly 90%
    assert _evaluate(f, listening_port).healthy


# -- evaluate_health: unpaced floor -------------------------------------------


def test_unpaced_healthy_above_floor(tmp_path, listening_port):
    f = tmp_path / "health.json"
    _write_health_file(f, acq_rate_hz=12.0, target_rate_hz=0.0)
    assert _evaluate(f, listening_port).healthy


def test_unpaced_unhealthy_at_or_below_floor(tmp_path, listening_port):
    f = tmp_path / "health.json"
    _write_health_file(f, acq_rate_hz=10.0, target_rate_hz=0.0)  # not > 10
    result = _evaluate(f, listening_port)
    assert not result.healthy
    assert "unpaced" in result.reason


# -- evaluate_health: file freshness / presence -------------------------------


def test_unhealthy_when_file_stale(tmp_path, listening_port):
    f = tmp_path / "health.json"
    _write_health_file(f, acq_rate_hz=10.0, target_rate_hz=10.0, wall_ts=time.time() - 100)
    result = _evaluate(f, listening_port)
    assert not result.healthy
    assert "stale" in result.reason


def test_unhealthy_when_file_missing(tmp_path, listening_port):
    result = _evaluate(tmp_path / "does_not_exist.json", listening_port)
    assert not result.healthy
    assert "not found" in result.reason


# -- evaluate_health: TCP probe -----------------------------------------------


def test_unhealthy_when_tcp_probe_fails(tmp_path):
    f = tmp_path / "health.json"
    _write_health_file(f, acq_rate_hz=10.0, target_rate_hz=10.0)
    # Port 1 is not listening; probe should fail before rate is considered.
    result = _evaluate(f, 1)
    assert not result.healthy
    assert "TCP probe" in result.reason


# -- HealthMonitor.write_once -------------------------------------------------


def test_monitor_writes_rate_from_sequence_delta(tmp_path):
    f = tmp_path / "health.json"
    seq = {"v": 0}
    clock = {"t": 1000.0}
    monitor = HealthMonitor(
        read_sequence=lambda: seq["v"],
        update_interval=0.1,
        file_path=str(f),
        write_interval=1.0,
        rate_window=5.0,
        clock=lambda: clock["t"],
        wall_clock=lambda: 42.0,
    )

    # First sample seeds the window.
    monitor.write_once()
    # 10 frames over 1s -> 10 Hz.
    seq["v"] = 10
    clock["t"] = 1001.0
    payload = monitor.write_once()

    assert payload["acq_rate_hz"] == pytest.approx(10.0)
    assert payload["target_rate_hz"] == pytest.approx(10.0)
    assert payload["wall_ts"] == 42.0

    on_disk = json.loads(f.read_text())
    assert on_disk["acq_rate_hz"] == pytest.approx(10.0)
