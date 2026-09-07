"""Upstream-freshness heartbeat file shared by the health watchdog and the
Docker healthcheck.

The bridge writes the integer epoch seconds of the last successful upstream
frame to a small file on tmpfs. A shell-based Docker healthcheck reads that file
and reports the container unhealthy once the value is stale (or missing/zero).

There is a single writer: the dedicated ``HealthWatchdog`` daemon thread (see
below), started from ``app.main``. It runs off the asyncio event loop reserved
for the tick path and is independent of the optional MQTT/Home Assistant
integration, so the healthcheck reflects real acquisition liveness in every
supported configuration. No other component writes this file.
"""

import logging
import os
import threading
import time
from typing import Callable

logger = logging.getLogger(__name__)

# Freshness heartbeat file consumed by the Docker healthcheck. Lives on tmpfs
# (/dev/shm) so repeated writes never touch the SD card on a Pi. Kept as a
# module-level constant so it is trivial to retarget without threading it
# through config.
HEALTH_HEARTBEAT_FILE: str = "/dev/shm/em540_health"


def write_health_heartbeat(last_frame_wall_clock: float, path: str = HEALTH_HEARTBEAT_FILE) -> None:
    """Write the last-frame epoch seconds to the heartbeat file.

    The value is the last observed *frame* wall-clock time (not "now"), so a
    wedged acquisition loop or dead upstream goes stale even while the writer
    keeps running. The write is atomic (temp file + ``os.replace``) so the probe
    never reads a half-written value, and best-effort: health monitoring must
    never perturb the bridge, so all errors are swallowed. A persistently
    failing write simply leaves the file stale, which the healthcheck treats as
    unhealthy — the safe direction.
    """
    try:
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w") as health_file:
            health_file.write(str(int(last_frame_wall_clock)))
        os.replace(tmp_path, path)
    except Exception:
        logger.debug("Failed to write health heartbeat to %s", path, exc_info=True)


class HealthWatchdog:
    """Dedicated daemon thread that monitors upstream-frame freshness.

    Runs entirely off the asyncio event loop reserved for the tick path. Using a
    plain ``threading.Event.wait()`` timer (no asyncio) means the watchdog stays
    schedulable even if the acquisition event loop wedges synchronously — which
    is exactly the failure it must detect and cannot detect from within that
    loop. On each poll it:

      1. refreshes the observability heartbeat file with the last frame's
         wall-clock epoch seconds (for the shell-based Docker healthcheck), and
      2. evaluates a caller-supplied staleness decision (in monotonic time) and,
         when it fires, invokes ``on_stale`` (normally ``lambda: os._exit(1)``)
         so Docker's restart policy recovers a fresh process.

    All inputs are injected callables so the thread is unit-testable without real
    time, real files, or actually terminating the interpreter.
    """

    def __init__(
        self,
        *,
        read_last_frame_monotonic: Callable[[], float],
        read_last_frame_wall_clock: Callable[[], float],
        should_exit: Callable[[float, float], bool],
        poll_interval_s: float,
        on_stale: Callable[[], None],
        heartbeat_path: str = HEALTH_HEARTBEAT_FILE,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._read_last_frame_monotonic = read_last_frame_monotonic
        self._read_last_frame_wall_clock = read_last_frame_wall_clock
        self._should_exit = should_exit
        self._poll_interval_s = poll_interval_s
        self._on_stale = on_stale
        self._heartbeat_path = heartbeat_path
        self._monotonic = monotonic
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def run_once(self) -> None:
        """Perform a single heartbeat write + staleness evaluation.

        Separated from the loop so it can be unit-tested directly. The heartbeat
        write is best-effort; the staleness decision uses monotonic time.
        """
        write_health_heartbeat(self._read_last_frame_wall_clock(), self._heartbeat_path)
        if self._should_exit(self._read_last_frame_monotonic(), self._monotonic()):
            self._on_stale()

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception:
                # The watchdog must never die on a transient error; that would
                # silently remove the recovery mechanism. Log and keep polling.
                logger.debug("Health watchdog poll raised; continuing", exc_info=True)
            self._stop_event.wait(self._poll_interval_s)

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="em540-health-watchdog")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
