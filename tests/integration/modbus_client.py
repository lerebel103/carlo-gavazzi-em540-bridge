"""Modbus client utilities and network connection helpers."""

from __future__ import annotations

import socket
import time


def wait_for_port(port: int, timeout: float = 10.0) -> None:
    """Wait for a TCP port to become available."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            try:
                sock.connect(("127.0.0.1", port))
            except OSError:
                time.sleep(0.05)
                continue
            return
    raise TimeoutError(f"port {port} was not ready within {timeout}s")


def connect_modbus_client(client, timeout: float = 10.0) -> None:
    """Connect a Modbus client with retries."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if client.connect():
            return
        time.sleep(0.05)
    raise TimeoutError(f"failed to connect client {client}")


def read_holding_registers(client, address: int, count: int, device_id: int = 1) -> list[int]:
    """Read holding registers from a Modbus device."""
    result = client.read_holding_registers(address, count=count, device_id=device_id)
    assert not result.isError(), f"read failed at {hex(address)} count={count}: {result}"
    assert len(result.registers) == count, f"expected {count} registers at {hex(address)}, got {len(result.registers)}"
    return list(result.registers)


def find_free_port() -> int:
    """Find an available TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
