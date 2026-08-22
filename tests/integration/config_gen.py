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
) -> None:
    """Generate a complete service configuration file for testing.

    Args:
        path: Path where the config YAML should be written.
        upstream_port: Serial port path for the upstream EM540 master.
        em540_tcp_port: TCP port for EM540 slave.
        em540_rtu_port: RTU-over-TCP port for EM540 slave.
        ts65a_tcp_port: TCP port for TS65A slave.
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
            "update_interval": 0.1,
            "timeout": 0.3,
            "retries": 0,
            "log_level": "WARNING",
        },
        "em540_slave": {
            "host": "127.0.0.1",
            "rtu_port": em540_rtu_port,
            "tcp_port": em540_tcp_port,
            "slave_id": 1,
            "update_timeout": 2.0,
            "log_level": "WARNING",
            "serial": {"enabled": False},
        },
        "ts65a_slave": {
            "host": "127.0.0.1",
            "port": ts65a_tcp_port,
            "slave_id": 1,
            "update_timeout": 2.0,
            "grid_feed_in_hard_limit": -5000,
            "smoothing_num_points": 20,
            "log_level": "WARNING",
            "serial": {"enabled": False},
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
