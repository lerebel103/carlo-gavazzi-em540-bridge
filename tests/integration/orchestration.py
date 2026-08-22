"""Orchestration and health check helpers for integration testing."""

from __future__ import annotations

import time
from collections import Counter
from typing import Callable


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


def wait_for_register_coverage(upstream_server, expected_requests: set[tuple[int, int]], timeout: float = 30.0) -> None:
    """Wait until all expected registers have been requested at least once.

    Verifies that:
    - All static registers are requested at least once
    - Primary dynamic block (0x0000) is requested at least 10 times
    - First energy block (0x0500) is requested at least 2 times
    - Second energy block (0x0520) is requested at least once

    Args:
        upstream_server: SerialServerHarness or similar with .requests property.
        expected_requests: Set of (address, count) tuples expected to be requested.
        timeout: Maximum time to wait in seconds.

    Raises:
        TimeoutError: If coverage criteria not met within timeout.
    """

    def _coverage_ready() -> bool:
        requests = upstream_server.requests
        counts = Counter((addr, count) for _, addr, count in requests)
        # Static registers: all must be requested at least once
        static_ok = all(counts[item] >= 1 for item in expected_requests if item[0] not in (0x0000, 0x0500, 0x0520))
        # Dynamic blocks: stricter requirements to exercise the full polling cycle
        primary_req = next((item for item in expected_requests if item[0] == 0x0000), None)
        primary_ok = primary_req is not None and counts[primary_req] >= 10
        energy0_ok = counts[(0x0500, 32)] >= 2
        energy1_ok = counts[(0x0520, 32)] >= 1
        return static_ok and primary_ok and energy0_ok and energy1_ok

    wait_for_condition(
        _coverage_ready,
        timeout=timeout,
        message="service did not exercise the full EM540 read schedule",
    )


def wait_for_downstream_data(client, address: int = 0x000B) -> None:
    """Wait until downstream slave serves non-zero data.

    Args:
        client: Connected Modbus client.
        address: Register address to poll (default: device type 0x000B).

    Raises:
        TimeoutError: If no non-zero data appears within timeout.
    """

    def _data_available() -> bool:
        result = client.read_holding_registers(address, count=1, device_id=1)
        return (not result.isError()) and result.registers[0] != 0

    wait_for_condition(
        _data_available,
        timeout=30.0,
        message="downstream slave did not serve non-zero data within timeout",
    )
