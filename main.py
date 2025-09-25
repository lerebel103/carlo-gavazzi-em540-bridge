#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import asyncio

from pyconfigparser import configparser
from pymodbus import pymodbus_apply_logging_config

from Config import load_config
from Em540_master import Em540Master
import logging

from Em540_slave_bridge import Em540Slave
from TS65A_slave_bridge import Ts65aSlaveBridge

logger = logging.getLogger()


def parse_args():
    parser = argparse.ArgumentParser(description='EM540 Modbus bridge')

    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to configuration file')

    return parser.parse_args()


async def process_loop():
    conf = configparser.get_config()
    pymodbus_apply_logging_config(conf.pymodbus.logging)

    # Create our master and slave instances
    em540_master = Em540Master(conf.em540_master)
    em540_slave = Em540Slave(conf.em540_slave, em540_master.data.frame)
    ts65a_slave = Ts65aSlaveBridge(conf.ts65a_slave)

    # register listeners on master to receive data updates
    em540_master.add_listener(em540_slave)
    em540_master.add_listener(ts65a_slave)

    # Start all
    await em540_slave.start()
    await ts65a_slave.start()

    from datetime import datetime
    timeline = datetime.now().timestamp()
    read_interval = conf.em540_master.update_interval
    while True:
        # Ensure em540 client is connected
        if not em540_master.connected:
            await em540_master.connect()

        # Now we can read data
        await em540_master.read_data()

        timeline += read_interval
        delta = timeline - datetime.now().timestamp()
        if delta < - 0.2:
            logger.warning(f"Falling behind schedule by {delta:.2f} seconds")
        if delta > 0:
            await asyncio.sleep(delta)
        else:
            timeline = datetime.now().timestamp()


async def main():
    logger.info("Starting EM540 Energy Meter Bridge")
    await process_loop()


if __name__ == '__main__':
    args = parse_args()
    load_config(args.config)

    # set up logger with default level of DEBUG and log to console and time
    logging.basicConfig(level=configparser.get_config().root.logging)
    asyncio.run(main())
