"""Upstream-freshness heartbeat file shared by the health watchdog and the
Docker healthcheck.

The bridge writes the integer epoch seconds of the last successful upstream
frame to a small file on tmpfs. A shell-based Docker healthcheck reads that file
and reports the container unhealthy once the value is stale (or missing/zero).

The writer is deliberately independent of MQTT/Home Assistant: it is invoked
from the always-on supervisor loop in ``app.main`` so the healthcheck reflects
real acquisition liveness even when the optional MQTT integration is disabled.
The Home Assistant diagnostics path also refreshes it opportunistically on its
own cadence, but that path is not required for the file to exist.
"""

import logging
import os

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
