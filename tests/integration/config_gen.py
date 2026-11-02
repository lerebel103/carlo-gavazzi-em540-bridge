"""Service configuration generation for integration testing."""

from __future__ import annotations

from pathlib import Path

import yaml


def generate_config(
    path: Path,
    upstream_port: str,
    em540_tcp_port: int,
    em540_rtu_port: int,
    ts65a_tcp_port: int,
    em540_serial_port: str | None = None,
    ts65a_serial_port: str | None = None,
    update_interval: float = 0.1,
    serial_pacing_enabled: bool = True,
    serial_pacing_max_wait_s: float = 0.12,
    serial_timeout: float = 0.5,
    stale_frame_detection_enabled: bool = False,
) -> None:
    """Generate a complete service configuration file for testing.

    Args:
        path: Path where the config YAML should be written.
        upstream_port: Serial port path for the upstream EM540 master.
        em540_tcp_port: TCP port for EM540 slave.
        em540_rtu_port: RTU-over-TCP port for EM540 slave.
        ts65a_tcp_port: TCP port for TS65A slave.
        update_interval: Upstream master read interval (also the downstream
            publish cadence) in seconds.
        serial_pacing_enabled: Whether serial frame-latching is enabled on both
            downstream serial bridges.
        serial_pacing_max_wait_s: The serial frame-latching cap (seconds). Must be
            strictly less than serial_timeout when pacing is enabled; <= 0 disables
            latching (duplicate frames possible).
        serial_timeout: Downstream serial adapter client-response timeout.
        stale_frame_detection_enabled: Whether the upstream stale/duplicate-frame
            detector is armed. Defaults to False for the integration harness: a
            test that holds the simulated upstream frame constant (e.g. during
            convergence) would otherwise be (correctly) flagged as a stuck meter
            and fail closed. Tests that advance the frame every tick can pass True
            to additionally assert the detector stays dormant under live traffic.
    """
    data = {
        "em540_master": {
            "mode": "serial",
            "baudrate": 9600,
            "parity": "N",
            "bytesize": 8,
            "stopbits": 1,
            "handle_local_echo": False,
            "serial_port": upstream_port,
            "port": 8899,
            "slave_id": 1,
            "update_interval": update_interval,
            "timeout": 0.3,
            "retries": 0,
            "stale_frame_detection": {"enabled": stale_frame_detection_enabled},
            "log_level": "WARNING",
        },
        "em540_slave": {
            "host": "127.0.0.1",
            "rtu_port": em540_rtu_port,
            "tcp_port": em540_tcp_port,
            "slave_id": 1,
            "update_timeout": 2.0,
            "log_level": "WARNING",
            "serial": {
                "enabled": em540_serial_port is not None,
                "port": em540_serial_port or "/dev/ttyUSB1",
                "timeout": serial_timeout,
                "pacing_enabled": serial_pacing_enabled,
                "pacing_max_wait_s": serial_pacing_max_wait_s,
            },
        },
        "ts65a_slave": {
            "host": "127.0.0.1",
            "port": ts65a_tcp_port,
            "slave_id": 1,
            "update_timeout": 2.0,
            "grid_feed_in_hard_limit": -5000,
            "smoothing_window_seconds": 2.5,
            "log_level": "WARNING",
            "serial": {
                "enabled": ts65a_serial_port is not None,
                "port": ts65a_serial_port or "/dev/ttyUSB2",
                "timeout": serial_timeout,
                "pacing_enabled": serial_pacing_enabled,
                "pacing_max_wait_s": serial_pacing_max_wait_s,
            },
        },
        "mqtt": {
            "enabled": False,
            "ha_topic_prefix": "",
            "host": "localhost",
            "port": 1883,
            "username": "",
            "password": "",
            "update_interval": 1.0,
            "log_level": "WARNING",
        },
        "pymodbus": {"log_level": "WARNING"},
        "root": {"log_level": "WARNING"},
    }
    path.write_text(yaml.safe_dump(data, sort_keys=False))
