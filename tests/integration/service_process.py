"""Service process lifecycle management for integration testing."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


class ServiceProcess:
    """Manages the lifecycle of the bridge service as a subprocess.

    Handles starting, stopping, and health checks.
    """

    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self._process: subprocess.Popen[str] | None = None

    def start(self) -> None:
        """Start the service process with the given config."""
        self._process = subprocess.Popen(
            [sys.executable, "-m", "app", "--config", str(self.config_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=str(Path(__file__).resolve().parents[2]),
        )

    def stop(self) -> None:
        """Stop the service process gracefully, then forcefully if needed."""
        if self._process is None:
            return
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5.0)

    def assert_running(self) -> None:
        """Assert that the service is still running."""
        if self._process is None:
            raise AssertionError("service was not started")
        code = self._process.poll()
        if code is not None:
            raise AssertionError(f"service exited unexpectedly with code {code}")
