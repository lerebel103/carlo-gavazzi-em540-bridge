#!/usr/bin/env python3
import argparse
import asyncio
import logging
import math
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
from app.version import version_for_display

logger = logging.getLogger()
config_manager = None
_MIN_PACED_INTERVAL_S = 0.001


@dataclass(frozen=True)
class _TickSignal:
    sequence: int
    deadline_mono: float
    ready_at_mono: float


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


async def process_loop():
    state = config_manager.load()
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

    try:
        while not stop_event.is_set():
            if em540_master.has_fatal_error:
                stop_event.set()
                break

            if _current_interval() > 0.0:
                _clear_tick_queue()
                scheduler_task = asyncio.create_task(_paced_scheduler(), name="em540-tick-scheduler")
                worker_task = asyncio.create_task(_paced_worker(), name="em540-acquisition-worker")
                done, pending = await asyncio.wait(
                    {scheduler_task, worker_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                for task in done:
                    exc = task.exception()
                    if exc is not None:
                        raise exc
            else:
                await _unpaced_worker()
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
        # never do useful work. logging isn't configured yet at this point,
        # so fall back to basicConfig defaults for a visible message.
        logging.basicConfig()
        logger.critical("Invalid configuration, refusing to start: %s", exc)
        sys.exit(1)
    logging.basicConfig(level=state.root_log_level)
    logger.info("Starting EM540 Energy Meter Bridge (%s)", version_for_display())
    await process_loop()
