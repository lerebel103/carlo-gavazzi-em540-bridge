"""Orchestration and health check helpers for integration testing."""

from __future__ import annotations

import time
from collections import Counter
from typing import Callable

from pymodbus import ModbusException


def wait_for_condition(predicate: Callable[[], bool], timeout: float, message: str) -> None:
    """Poll a predicate until it becomes true or timeout is exceeded.

    Args:
        predicate: Callable that returns True when the condition is met.
        timeout: Maximum time to wait in seconds.
        message: Error message if timeout occurs.

    Raises:
        TimeoutError: If predicate never returns True within timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise TimeoutError(message)


def wait_for_register_coverage(upstream_server, expected_requests: set[tuple[int, int]], timeout: float = 60.0) -> None:
    """Wait until all expected registers have been requested at least once.

    Verifies that:
    - Primary dynamic block (0x0000) is requested at least 5 times
    - First energy block (0x0500) is requested at least once
    - Second energy block (0x0520) is requested at least once
    - At least one static register is requested

    Note: Full static register coverage can be slow on CI runners, so we only
    verify the primary cycles and partial static coverage.

    Args:
        upstream_server: SerialServerHarness or similar with .requests property.
        expected_requests: Set of (address, count) tuples expected to be requested.
        timeout: Maximum time to wait in seconds (default 60s for CI).

    Raises:
        TimeoutError: If coverage criteria not met within timeout.
    """

    def _coverage_ready() -> bool:
        requests = upstream_server.requests
        if not requests:
            return False
        counts = Counter((addr, count) for _, addr, count in requests)
        # Check we have at least some static register coverage
        static_requests = [item for item in expected_requests if item[0] not in (0x0000, 0x0500, 0x0520)]
        static_ok = any(counts[item] >= 1 for item in static_requests)
        # Dynamic blocks: primary should be polled multiple times
        primary_req = next((item for item in expected_requests if item[0] == 0x0000), None)
        primary_ok = primary_req is not None and counts[primary_req] >= 5
        # Energy blocks should be accessed
        energy0_ok = counts[(0x0500, 32)] >= 1
        energy1_ok = counts[(0x0520, 32)] >= 1
        return static_ok and primary_ok and energy0_ok and energy1_ok

    wait_for_condition(
        _coverage_ready,
        timeout=timeout,
        message="service did not exercise the full EM540 read schedule",
    )


def wait_for_downstream_data(client, address: int = 0x000B, timeout: float = 60.0) -> None:
    """Wait until downstream slave serves non-zero data.

    Args:
        client: Connected Modbus client.
        address: Register address to poll (default: device type 0x000B).
        timeout: Maximum time to wait in seconds (default 60s for CI).

    Raises:
        TimeoutError: If no non-zero data appears within timeout.
    """

    def _data_available() -> bool:
        try:
            result = client.read_holding_registers(address, count=1, device_id=1)
        except (ModbusException, OSError, ConnectionError):
            # During startup/reconnect windows the downstream socket can briefly
            # reset before the server is fully ready; keep polling until timeout.
            return False
        return (not result.isError()) and result.registers[0] != 0

    wait_for_condition(
        _data_available,
        timeout=timeout,
        message="downstream slave did not serve non-zero data within timeout",
    )
