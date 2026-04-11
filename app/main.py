#!/usr/bin/env python3
import argparse
import asyncio
import logging
import time

from pymodbus import pymodbus_apply_logging_config

from app.carlo_gavazzi.em540_master import Em540Master
from app.carlo_gavazzi.em540_slave_bridge import Em540Slave
from app.config import ConfigManager
from app.fronius.ts65a_slave_bridge import Ts65aSlaveBridge
from app.home_assistant.ha_bridge import HABridge
from app.version import version_for_display

logger = logging.getLogger()
config_manager = None


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
        if mqtt_bridge is not None:
            mqtt_bridge.stop()
        await em540_master.disconnect()
        config_manager.stop()


async def main():
    global config_manager
    args = parse_args()
    config_manager = ConfigManager(args.config)
    state = config_manager.load()
    logging.basicConfig(level=state.root_log_level)
    logger.info("Starting EM540 Energy Meter Bridge (%s)", version_for_display())
    await process_loop()
