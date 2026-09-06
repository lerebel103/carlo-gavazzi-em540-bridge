#!/usr/bin/env python3
import argparse
import asyncio
import logging
import math
import os
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass

from pymodbus import pymodbus_apply_logging_config

from app.carlo_gavazzi.em540_master import Em540Master
from app.carlo_gavazzi.em540_slave_bridge import Em540Slave
from app.config import ConfigError, ConfigManager
from app.fronius.ts65a_slave_bridge import Ts65aSlaveBridge
from app.home_assistant.ha_bridge import HABridge
from app.utils.health import write_health_heartbeat
from app.version import version_for_display

logger = logging.getLogger()
config_manager = None
_MIN_PACED_INTERVAL_S = 0.001

# Log line format. asctime uses local time (time.localtime), which honours the
# TZ environment variable (set via docker-compose), so timestamps are in the
# configured local zone. %(msecs) appends milliseconds to the date/time.
_LOG_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s"
_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


@dataclass(frozen=True)
class _TickSignal:
    sequence: int
    deadline_mono: float
    ready_at_mono: float


def _health_watchdog_should_exit(
    *,
    last_frame_wall_clock: float,
    now_wall_clock: float,
    process_start_wall_clock: float,
    max_stale_s: float,
    grace_period_s: float,
) -> bool:
    """Decide whether the upstream-freshness watchdog should self-exit.

    Returns True when the process has been running past its startup grace
    period yet the most recent successful upstream frame is older than
    ``max_stale_s``. This is the "wedged / dead upstream that internal recovery
    can't fix" condition; the caller responds by exiting so Docker's restart
    policy recovers a fresh process.

    Pure and side-effect free so it can be unit-tested without terminating the
    interpreter. The freshness input is the last successful frame's wall-clock
    time (``MeterData.timestamp``), which only advances on a real acquisition,
    so a stalled tick loop or dead upstream both let it go stale.

    - ``max_stale_s <= 0`` disables the watchdog (always returns False).
    - Before the first frame (``last_frame_wall_clock <= 0``) staleness is
      measured from process start, so a meter that never produces data still
      triggers recovery once past the grace period.
    - The grace period suppresses exits during boot/first-connect/reconnect
      backoff, mirroring the Docker healthcheck ``start_period``.
    """
    if max_stale_s <= 0.0:
        return False

    # Still inside the startup grace window: never exit yet.
    if now_wall_clock - process_start_wall_clock < grace_period_s:
        return False

    # Anchor freshness to the first frame if one has landed, otherwise to
    # process start (so "never produced a frame" is caught too).
    reference = last_frame_wall_clock if last_frame_wall_clock > 0.0 else process_start_wall_clock
    return (now_wall_clock - reference) > max_stale_s


class _PymodbusReconnectWarningFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.name == "pymodbus.logging" and record.levelno == logging.WARNING:
            # Check the raw msg without eagerly formatting (avoids % formatting overhead)
            msg = record.msg if isinstance(record.msg, str) else str(record.msg)
            if msg.startswith("Failed to connect"):
                return False
        return True


@contextmanager
def _suppress_pymodbus_reconnect_warning():
    reconnect_warning_filter = _PymodbusReconnectWarningFilter()
    pymodbus_logger = logging.getLogger("pymodbus.logging")
    pymodbus_logger.addFilter(reconnect_warning_filter)
    try:
        yield
    finally:
        pymodbus_logger.removeFilter(reconnect_warning_filter)


def parse_args():
    parser = argparse.ArgumentParser(description="EM540 Modbus bridge")
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file",
    )
    return parser.parse_args()


async def process_loop(state):
    # ``state`` is the already-validated config loaded in main(). We reuse it
    # rather than reloading here: a second load() would re-run the serial-device
    # reachability probe (opening/closing every configured port again) and any
    # ConfigError it raised would bypass main()'s fail-fast startup handler.
    pymodbus_apply_logging_config(state.pymodbus_log_level)

    em540_master = Em540Master(state.em540_master)
    em540_slave = Em540Slave(state.em540_slave, em540_master.data.frame)
    ts65a_slave = Ts65aSlaveBridge(state.ts65a_slave)
    mqtt_bridge = None

    em540_master.add_listener(em540_slave)
    em540_master.add_listener(ts65a_slave)

    if state.mqtt.enabled:
        mqtt_bridge = HABridge(state.mqtt, state=state, config_manager=config_manager)
        em540_master.add_listener(mqtt_bridge)
        em540_master.add_stats_listener(mqtt_bridge.on_em540_master_stats)
        mqtt_bridge.set_daily_extrema_source(em540_master.daily_extrema)
        em540_slave.add_stats_listener(mqtt_bridge.on_em540_slave_stats)
        ts65a_slave.add_stats_listener(mqtt_bridge.on_ts65a_slave_stats)
        try:
            mqtt_bridge.connect()
        except Exception:
            logger.exception("Failed to initialize MQTT bridge connection")

    config_manager.start_flush_loop()
    await em540_slave.start()
    await ts65a_slave.start()

    reconnect_backoff = float(state.em540_master.update_interval)
    reconnect_backoff = reconnect_backoff if reconnect_backoff > 0.0 else 0.1
    max_reconnect_backoff = 5.0
    next_connect_attempt_time = 0.0
    stop_event = asyncio.Event()
    tick_queue: asyncio.Queue[_TickSignal | None] = asyncio.Queue(maxsize=1)

    def _current_interval() -> float:
        interval_s = float(state.em540_master.update_interval)
        if interval_s <= 0.0:
            return 0.0
        # Prevent pathological scheduler behavior for tiny positive intervals.
        return max(_MIN_PACED_INTERVAL_S, interval_s)

    def _aligned_start_deadline(interval_s: float) -> float:
        wall_now = time.time()
        mono_now = time.perf_counter()
        next_wall_deadline = math.floor(wall_now / interval_s) * interval_s + interval_s
        return mono_now + (next_wall_deadline - wall_now)

    def _clear_tick_queue() -> None:
        while True:
            try:
                tick_queue.get_nowait()
            except asyncio.QueueEmpty:
                return

    async def _notify_tick(signal: _TickSignal) -> None:
        if tick_queue.full():
            try:
                tick_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            tick_queue.put_nowait(signal)
        except asyncio.QueueFull:
            # Another producer pass won the race; skipping is acceptable because
            # worker semantics are latest-tick-wins under overload.
            pass

    async def _attempt_connect(now: float, interval_s: float) -> tuple[bool, float]:
        nonlocal reconnect_backoff, next_connect_attempt_time

        if now < next_connect_attempt_time:
            return False, max(0.0, next_connect_attempt_time - now)

        # Suppress the "Failed to connect" WARNING from pymodbus.logging to avoid reconnect log spam.
        with _suppress_pymodbus_reconnect_warning():
            await em540_master.connect()

        if em540_master.connected:
            reconnect_backoff = interval_s if interval_s > 0.0 else 0.1
            next_connect_attempt_time = 0.0
            return True, 0.0
        else:
            next_connect_attempt_time = time.perf_counter() + reconnect_backoff
            retry_base = interval_s if interval_s > 0.0 else 0.1
            reconnect_backoff = min(max(retry_base, reconnect_backoff * 2), max_reconnect_backoff)
            return True, reconnect_backoff

    async def _acquire_cycle(tick_signal: _TickSignal | None) -> bool:
        interval_s = _current_interval()
        now = time.perf_counter()

        if not em540_master.connected:
            attempted_connect, wait_for_next_connect_s = await _attempt_connect(now, interval_s)
            if not attempted_connect:
                # In unpaced mode, avoid a disconnected hot loop while reconnect
                # attempts are intentionally rate-limited by backoff.
                if interval_s <= 0.0:
                    await asyncio.sleep(min(wait_for_next_connect_s, 0.05))
                return False

        if tick_signal is None:
            return await em540_master.acquire_data(tick_interval_s=interval_s)

        return await em540_master.acquire_data(
            tick_deadline_mono=tick_signal.deadline_mono,
            tick_interval_s=interval_s,
        )

    async def _paced_scheduler() -> None:
        sequence = 0
        interval_s = _current_interval()
        if interval_s <= 0.0:
            return
        next_deadline = _aligned_start_deadline(interval_s)

        try:
            while not stop_event.is_set():
                sleep_for = next_deadline - time.perf_counter()
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)

                if stop_event.is_set():
                    return

                ready_at = time.perf_counter()
                sequence += 1
                await _notify_tick(_TickSignal(sequence, next_deadline, ready_at))

                interval_s = _current_interval()
                if interval_s <= 0.0:
                    return

                next_deadline += interval_s
                now = time.perf_counter()
                while next_deadline <= now:
                    next_deadline += interval_s
        finally:
            if tick_queue.full():
                try:
                    tick_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                tick_queue.put_nowait(None)
            except asyncio.QueueFull:
                pass

    async def _paced_worker() -> None:
        try:
            while not stop_event.is_set():
                if em540_master.has_fatal_error:
                    stop_event.set()
                    return

                # Live transition: leave paced worker as soon as interval is disabled.
                if _current_interval() <= 0.0:
                    return

                try:
                    signal = await asyncio.wait_for(tick_queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue

                if signal is None or stop_event.is_set():
                    return

                await _acquire_cycle(signal)
        finally:
            if tick_queue.full():
                try:
                    tick_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                tick_queue.put_nowait(None)
            except asyncio.QueueFull:
                pass

    async def _unpaced_worker() -> None:
        try:
            while not stop_event.is_set():
                if em540_master.has_fatal_error:
                    stop_event.set()
                    return

                # Live transition: switch to paced mode once interval is enabled.
                if _current_interval() > 0.0:
                    return

                await _acquire_cycle(None)
        finally:
            pass

    # Upstream-freshness watchdog. Docker does not restart a container merely
    # because its healthcheck reports "unhealthy" (that only reacts to the
    # container exiting), so the freshness signal that drives automatic recovery
    # must be an application-side self-exit. We evaluate it here in the
    # supervisor loop — off the pinned tick core and independent of MQTT — so a
    # wedged acquisition loop or dead upstream that internal recovery cannot fix
    # results in os._exit(1), letting `restart: unless-stopped` recover a fresh
    # process. Reading em540_master.data.timestamp is a lock-free atomic float
    # read and adds no work to the 10Hz path.
    health_max_stale_s = float(state.em540_master.health_max_stale_s)
    # Grace mirrors the compose healthcheck start_period so boot/first-connect/
    # reconnect-backoff never self-kills. Bounded below by the stale threshold so
    # a very small configured threshold still gets a sane startup window.
    health_grace_period_s = max(health_max_stale_s, 45.0)
    # Cap the supervisor's wait so a fully-wedged tick loop (no task ever
    # completing) is still re-evaluated for staleness on a bounded cadence.
    health_poll_interval_s = 5.0
    process_start_wall_clock = time.time()

    def _check_health_watchdog() -> None:
        # Refresh the observability heartbeat file on every check, independent of
        # MQTT, so the Docker healthcheck reflects freshness even when the MQTT
        # integration is disabled. Best-effort; never raises.
        write_health_heartbeat(em540_master.data.timestamp)

        if health_max_stale_s <= 0.0:
            return
        if _health_watchdog_should_exit(
            last_frame_wall_clock=em540_master.data.timestamp,
            now_wall_clock=time.time(),
            process_start_wall_clock=process_start_wall_clock,
            max_stale_s=health_max_stale_s,
            grace_period_s=health_grace_period_s,
        ):
            logger.critical(
                "No fresh upstream frame for over %.0fs; internal recovery appears wedged. "
                "Exiting so the container is restarted.",
                health_max_stale_s,
            )
            os._exit(1)

    async def _supervise(tasks: set[asyncio.Task]) -> None:
        """Wait for any task to finish, polling the freshness watchdog meanwhile.

        The bounded ``asyncio.wait`` timeout guarantees ``_check_health_watchdog()``
        runs on a fixed cadence even when a worker never completes (a fully-wedged
        acquisition path), so the process can still self-exit. Applies to BOTH the
        paced (scheduler + worker) and unpaced (single worker) modes so neither
        can bypass the watchdog. Running tasks are only torn down once one of them
        actually finishes; exceptions are re-raised to the caller.
        """
        pending: set[asyncio.Task] = set(tasks)
        try:
            while True:
                done, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=health_poll_interval_s,
                )
                if done:
                    break
                # Timed out with everything still running: re-evaluate the
                # watchdog without disrupting the tick loop, then keep waiting.
                _check_health_watchdog()
                if stop_event.is_set():
                    break
        finally:
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            exc = task.exception()
            if exc is not None:
                raise exc

    try:
        while not stop_event.is_set():
            if em540_master.has_fatal_error:
                stop_event.set()
                break

            _check_health_watchdog()

            if _current_interval() > 0.0:
                _clear_tick_queue()
                scheduler_task = asyncio.create_task(_paced_scheduler(), name="em540-tick-scheduler")
                worker_task = asyncio.create_task(_paced_worker(), name="em540-acquisition-worker")
                await _supervise({scheduler_task, worker_task})
            else:
                # Unpaced mode also runs under supervision so a blocked
                # _acquire_cycle() cannot starve the watchdog (the single worker
                # is the only task, but the bounded poll still fires the check).
                unpaced_task = asyncio.create_task(_unpaced_worker(), name="em540-unpaced-worker")
                await _supervise({unpaced_task})
    finally:
        stop_event.set()
        _clear_tick_queue()
        try:
            tick_queue.put_nowait(None)
        except asyncio.QueueFull:
            pass
        em540_master.stop_listeners()
        em540_slave.stop()
        ts65a_slave.stop()
        if mqtt_bridge is not None:
            mqtt_bridge.stop()
        await em540_master.disconnect()
        config_manager.stop()


async def main():
    global config_manager

    # Real-time GC tuning (intentional, retained optimisation):
    # The tick loop targets 10Hz, so cyclic-GC pauses translate directly into tick
    # jitter/overruns. Note that refcounting already frees the bulk of per-tick garbage
    # (acyclic temporaries) immediately, independent of these thresholds; the cyclic
    # collector only reclaims reference cycles. Raising the gen-0 threshold from the
    # default 700 to 5000 makes young-generation collections less frequent (fewer, but
    # slightly larger, sweeps) rather than relocating them out of the tick — a collection
    # fires when the allocation watermark is crossed, which is typically mid-tick during
    # parsing. gen-1/gen-2 thresholds stay at defaults so cyclic garbage is still
    # reclaimed and memory stays bounded. We deliberately do NOT disable GC entirely,
    # which would risk unbounded growth under connection churn. An occasional
    # gen-2 pause (a few ms) is acceptable; it may cause a single tick overrun
    # but the scheduler absorbs it on the next tick.
    import gc

    gc.set_threshold(5000, 10, 10)

    args = parse_args()
    config_manager = ConfigManager(args.config)
    try:
        state = config_manager.load()
    except ConfigError as exc:
        # Fail hard and fast on invalid configuration (e.g. a misconfigured or
        # unreachable serial device) rather than starting a service that can
        # never do useful work. logging isn't configured yet at this point, so
        # fall back to basicConfig for a visible message. force=True guarantees
        # the fallback handler is installed even if some handler was already
        # configured (e.g. under an embedding launcher), so the critical message
        # is never silently suppressed.
        logging.basicConfig(force=True, format=_LOG_FORMAT, datefmt=_LOG_DATEFMT)
        logger.critical("Invalid configuration, refusing to start: %s", exc)
        sys.exit(1)
    logging.basicConfig(level=state.root_log_level, format=_LOG_FORMAT, datefmt=_LOG_DATEFMT)
    logger.info("Starting EM540 Energy Meter Bridge (%s)", version_for_display())
    await process_loop(state)
