"""Allow running the package with ``python -m app``.

Profiling: Send SIGUSR1 to start profiling, SIGUSR2 to stop and dump results.
    docker exec <container> kill -USR1 1   # start profiling
    docker exec <container> kill -USR2 1   # stop and print results to stdout
"""

import asyncio
import cProfile
import io
import pstats
import signal
import sys

from app.main import main

_profiler: cProfile.Profile | None = None


def _start_profiling(signum, frame):
    """Start CPU profiling on SIGUSR1."""
    global _profiler
    if _profiler is not None:
        print("[profiler] Already running, ignoring.", flush=True)
        return
    _profiler = cProfile.Profile()
    _profiler.enable()
    print("[profiler] Started. Send SIGUSR2 to stop and dump results.", flush=True)


def _stop_profiling(signum, frame):
    """Stop profiling on SIGUSR2 and dump results to stdout."""
    global _profiler
    if _profiler is None:
        print("[profiler] Not running, ignoring.", flush=True)
        return
    _profiler.disable()
    print("[profiler] Stopped. Results:", flush=True)

    s = io.StringIO()
    ps = pstats.Stats(_profiler, stream=s).sort_stats("tottime")
    ps.print_stats(50)
    print(s.getvalue(), flush=True)

    _profiler = None


signal.signal(signal.SIGUSR1, _start_profiling)
signal.signal(signal.SIGUSR2, _stop_profiling)

asyncio.run(main())
