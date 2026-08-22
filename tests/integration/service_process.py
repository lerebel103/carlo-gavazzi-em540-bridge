"""Service process lifecycle management for integration testing."""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path


class ServiceProcess:
    """Manages the lifecycle of the bridge service as a subprocess.

    Handles starting, stopping, and health checks.
    """

    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self._process: subprocess.Popen[str] | None = None
        self._stdout_log: tempfile.NamedTemporaryFile | None = None
        self._stderr_log: tempfile.NamedTemporaryFile | None = None

    def start(self) -> None:
        """Start the service process with the given config."""
        self._stdout_log = tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", delete=False)
        self._stderr_log = tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", delete=False)
        self._process = subprocess.Popen(
            [sys.executable, "-m", "app", "--config", str(self.config_path)],
            stdout=self._stdout_log,
            stderr=self._stderr_log,
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
        if self._stdout_log is not None:
            self._stdout_log.close()
            self._stdout_log = None
        if self._stderr_log is not None:
            self._stderr_log.close()
            self._stderr_log = None

    def _read_log_excerpt(self, handle: tempfile.NamedTemporaryFile | None, limit: int = 2000) -> str:
        if handle is None:
            return ""
        path = Path(handle.name)
        if not path.exists():
            return ""
        text = path.read_text(encoding="utf-8", errors="replace")
        return text[-limit:]

    def diagnostics(self) -> str:
        """Return stderr/stdout excerpts for debugging integration failures."""
        stdout_excerpt = self._read_log_excerpt(self._stdout_log)
        stderr_excerpt = self._read_log_excerpt(self._stderr_log)
        return f"\n--- service stdout (tail) ---\n{stdout_excerpt}\n--- service stderr (tail) ---\n{stderr_excerpt}\n"

    def assert_running(self) -> None:
        """Assert that the service is still running."""
        if self._process is None:
            raise AssertionError("service was not started")
        code = self._process.poll()
        if code is not None:
            raise AssertionError(f"service exited unexpectedly with code {code}\n{self.diagnostics()}")
