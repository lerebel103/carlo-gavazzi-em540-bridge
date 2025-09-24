#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import asyncio

from pymodbus import pymodbus_apply_logging_config

from Em540_master import Em540Master
import logging

from Em540_slave_bridge import Em540Slave
from TS65A_slave_bridge import Ts65aSlaveBridge

logger = logging.getLogger()

read_interval = 0.1  # seconds
bridge_timeout = 1  # seconds


def parse_args():
    parser = argparse.ArgumentParser(description='EM540 Modbus bridge')

    parser.add_argument('--source-host', type=str, default='192.168.102.240',
                        help='Default EM540 modbus host to bind to')
    parser.add_argument('--source-port', type=int, default=8899,
                        help='Default modbus EM540 port to bind to')
    parser.add_argument('--target-host', type=str, default='0.0.0.0',
                        help='Host on which a TS-65A is emulated')
    parser.add_argument('--em540-port', type=int, default=5002,
                        help='Port on which a EM540 is bridged')
    parser.add_argument('--ts65a-port', type=int, default=5003,
                        help='Port on which a TS-65-A is bridged')

    return parser.parse_args()


async def process_loop(args):
    pymodbus_apply_logging_config("INFO")

    # Create our master and slave instances
    em540_master = Em540Master(args.source_host, args.source_port)
    em540_slave = Em540Slave(args.target_host, args.em540_port, bridge_timeout, em540_master.data.frame)
    ts65a_slave = Ts65aSlaveBridge(args.target_host, args.ts65a_port, bridge_timeout)

    # register listeners on master to receive data updates
    em540_master.add_listener(em540_slave)
    em540_master.add_listener(ts65a_slave)

    # Start all
    await em540_slave.start()
    await ts65a_slave.start()

    from datetime import datetime
    timeline = datetime.now().timestamp()
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


async def main(args):
    logger.info("Starting EM540 to Fronius TS-65A converter")
    await process_loop(args)


if __name__ == '__main__':
    args = parse_args()

    # set up logger with default level of DEBUG and log to console and time
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main(args))
