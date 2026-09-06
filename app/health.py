"""Health monitoring for the EM540/TS65A bridge.

Two cooperating pieces:

- ``HealthMonitor``: a daemon thread inside the running service that periodically
  writes a small JSON heartbeat file with the measured acquisition rate and the
  configured target. It runs OFF the tick loop (its own thread) so it never
  interferes with the 10 Hz upstream read path.

- ``evaluate_health`` / ``main``: the container healthcheck. It performs a TCP
  liveness probe against a downstream server port and validates the heartbeat
  file (freshness + acquisition rate within margin). Invoked as
  ``python -m app.healthcheck`` from the Docker HEALTHCHECK.

A service is healthy when:
  1. the downstream server port accepts a TCP connection, AND
  2. the heartbeat file is fresh, AND
  3. the acquisition rate is on target:
       - configured update_interval > 0  -> rate >= margin * (1 / update_interval)
       - configured update_interval == 0 -> rate >  unpaced_min_rate_hz
"""

from __future__ import annotations

import json
import logging
import socket
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

logger = logging.getLogger("health")


def target_rate_hz(update_interval: float) -> float:
    """Configured target acquisition rate in Hz, or 0.0 for unpaced (max-rate)."""
    if update_interval and update_interval > 0:
        return 1.0 / update_interval
    return 0.0


class HealthMonitor:
    """Periodically writes a heartbeat file with the measured acquisition rate.

    ``read_sequence`` returns a monotonically increasing count of successfully
    published frames (e.g. ``Em540Master.data_sequence``); the monitor derives
    the rate from its delta over ``write_interval``. All timing uses a monotonic
    clock. The writer runs on its own daemon thread — never on the tick loop.
    """

    def __init__(
        self,
        read_sequence: Callable[[], int],
        update_interval: float,
        file_path: str,
        write_interval: float = 1.0,
        rate_window: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self._read_sequence = read_sequence
        self._update_interval = update_interval
        self._file_path = file_path
        self._write_interval = max(0.05, write_interval)
        self._rate_window = max(self._write_interval, rate_window)
        self._clock = clock
        self._wall_clock = wall_clock

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # Rolling samples of (monotonic_time, sequence) used to average the rate
        # over rate_window, smoothing transient dips.
        self._samples: list[tuple[float, int]] = []

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, daemon=True, name="health-monitor")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=1.0)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.write_once()
            except Exception:
                logger.debug("Health file write failed", exc_info=True)
            self._stop.wait(self._write_interval)

    def _current_rate_hz(self, now: float, sequence: int) -> float:
        """Average rate over the window from the rolling samples."""
        self._samples.append((now, sequence))
        # Drop samples older than the window (keep at least the oldest needed).
        cutoff = now - self._rate_window
        while len(self._samples) > 2 and self._samples[0][0] < cutoff:
            self._samples.pop(0)

        oldest_t, oldest_seq = self._samples[0]
        elapsed = now - oldest_t
        if elapsed <= 0:
            return 0.0
        return (sequence - oldest_seq) / elapsed

    def write_once(self) -> dict:
        """Compute the current rate and write the heartbeat file. Returns the payload."""
        now = self._clock()
        sequence = self._read_sequence()
        rate = self._current_rate_hz(now, sequence)

        payload = {
            "wall_ts": self._wall_clock(),
            "monotonic_ts": now,
            "acq_rate_hz": rate,
            "target_rate_hz": target_rate_hz(self._update_interval),
            "update_interval": self._update_interval,
        }
        # Atomic-ish write: write to a temp file then rename so a reader never
        # sees a partially written file.
        tmp_path = f"{self._file_path}.tmp"
        with open(tmp_path, "w") as f:
            json.dump(payload, f)
        import os

        os.replace(tmp_path, self._file_path)
        return payload


@dataclass
class HealthResult:
    healthy: bool
    reason: str


def _tcp_probe(host: str, port: int, timeout: float) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def evaluate_health(
    *,
    file_path: str,
    tcp_host: str,
    tcp_port: int,
    rate_margin: float,
    unpaced_min_rate_hz: float,
    max_file_age_s: float,
    tcp_timeout: float = 3.0,
    now: Optional[float] = None,
) -> HealthResult:
    """Return the health verdict. Pure/testable: no process exit here."""
    # 1. Downstream server liveness.
    if not _tcp_probe(tcp_host, tcp_port, tcp_timeout):
        return HealthResult(False, f"TCP probe to {tcp_host}:{tcp_port} failed")

    # 2. Heartbeat file present and parseable.
    try:
        with open(file_path) as f:
            payload = json.load(f)
    except FileNotFoundError:
        return HealthResult(False, f"health file {file_path} not found")
    except (OSError, ValueError) as exc:
        return HealthResult(False, f"health file unreadable: {exc}")

    # 3. Freshness (wall clock, since the reader is a separate process).
    wall_ts = payload.get("wall_ts")
    if not isinstance(wall_ts, (int, float)):
        return HealthResult(False, "health file missing wall_ts")
    current = time.time() if now is None else now
    age = current - wall_ts
    if age > max_file_age_s:
        return HealthResult(False, f"health file stale ({age:.1f}s > {max_file_age_s:.1f}s)")

    # 4. Acquisition rate on target.
    rate = payload.get("acq_rate_hz")
    target = payload.get("target_rate_hz")
    if not isinstance(rate, (int, float)) or not isinstance(target, (int, float)):
        return HealthResult(False, "health file missing rate fields")

    if target > 0:
        required = rate_margin * target
        if rate < required:
            return HealthResult(
                False,
                f"acquisition rate too low: {rate:.2f} Hz < {required:.2f} Hz "
                f"({rate_margin:.0%} of {target:.2f} Hz target)",
            )
    else:
        # Unpaced/max-rate: require the floor.
        if rate <= unpaced_min_rate_hz:
            return HealthResult(
                False,
                f"acquisition rate too low (unpaced): {rate:.2f} Hz <= {unpaced_min_rate_hz:.2f} Hz",
            )

    return HealthResult(True, f"healthy (rate={rate:.2f} Hz)")
