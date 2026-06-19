#!/usr/bin/env python3
import argparse
import asyncio
import logging
import time
from contextlib import contextmanager

from pymodbus import pymodbus_apply_logging_config

from app.carlo_gavazzi.em540_master import Em540Master
from app.carlo_gavazzi.em540_slave_bridge import Em540Slave
from app.config import ConfigManager
from app.fronius.ts65a_slave_bridge import Ts65aSlaveBridge
from app.home_assistant.ha_bridge import HABridge
from app.version import version_for_display

logger = logging.getLogger()
config_manager = None


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

    initial_interval = state.em540_master.update_interval
    start_time = time.perf_counter()
    next_call_time = start_time + initial_interval
    reconnect_backoff = initial_interval
    max_reconnect_backoff = 5.0
    next_connect_attempt_time = 0.0

    try:
        while True:
            if em540_master.has_fatal_error:
                logger.critical("A listener thread encountered unrecoverable errors, initiating shutdown.")
                break

            read_interval = max(0.001, float(state.em540_master.update_interval))
            current_time = time.perf_counter()
            if current_time >= next_call_time:
                # If we are late, skip missed ticks instead of executing catch-up bursts.
                lag = current_time - next_call_time
                if lag >= read_interval:
                    skipped_ticks = int(lag // read_interval)
                    next_call_time += (skipped_ticks + 1) * read_interval
                else:
                    next_call_time += read_interval

                if not em540_master.connected:
                    if current_time >= next_connect_attempt_time:
                        # Suppress the "Failed to connect" WARNING from pymodbus.logging to avoid reconnect log spam.
                        with _suppress_pymodbus_reconnect_warning():
                            await em540_master.connect()

                        if em540_master.connected:
                            reconnect_backoff = read_interval
                            next_connect_attempt_time = 0.0
                        else:
                            next_connect_attempt_time = time.perf_counter() + reconnect_backoff
                            reconnect_backoff = min(reconnect_backoff * 2, max_reconnect_backoff)
                await em540_master.acquire_data()

            sleep_for = max(0, next_call_time - time.perf_counter() - 0.0001)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
    finally:
        em540_master.stop_listeners()
        em540_slave.stop()
        ts65a_slave.stop()
        if mqtt_bridge is not None:
            mqtt_bridge.stop()
        await em540_master.disconnect()
        config_manager.stop()


async def main():
    global config_manager

    # Tune GC for real-time performance: raise the gen-0 threshold so collections
    # happen less frequently (roughly every few seconds instead of multiple times
    # per second). This reduces tick-loop pauses from gen-0 sweeps while still
    # collecting reference cycles that would otherwise leak memory.
    # Default is (700, 10, 10); we raise gen-0 to 5000 so collections batch up
    # during idle periods between tick bursts.
    import gc

    gc.set_threshold(5000, 10, 10)

    args = parse_args()
    config_manager = ConfigManager(args.config)
    state = config_manager.load()
    logging.basicConfig(level=state.root_log_level)
    logger.info("Starting EM540 Energy Meter Bridge (%s)", version_for_display())
    await process_loop()
