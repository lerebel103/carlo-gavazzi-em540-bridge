#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import asyncio

from pymodbus import pymodbus_apply_logging_config

from Em540_master import Em540Master
import logging

from Em540_slave import Em540Slave

logger = logging.getLogger()

read_interval = 0.1  # seconds

def parse_args():
    parser = argparse.ArgumentParser(description='EM540 to Fronius TS-65A converter')
    parser.add_argument('--source-host', type=str, default='192.168.102.240', help='Default EM540 modbus host to bind to')
    parser.add_argument('--source-port', type=int, default=8899, help='Default modbus EM540 port to bind to')
    parser.add_argument('--target-host', type=str, default='0.0.0.0', help='Host on which a TS-65A is emulated')
    parser.add_argument('--target-port', type=int, default=5002, help='Port on which a TS-65A is emulated')
    return parser.parse_args()

async def process_loop(source_host, source_port, target_host, target_port):
    pymodbus_apply_logging_config("INFO")

    em540_master = Em540Master(source_host, source_port)
    em540_slave = Em540Slave(target_host, target_port, em540_master.data)
    await em540_slave.start()

    from datetime import datetime
    timeline = datetime.now().timestamp()
    while True:
        # Ensure em540 client is connected
        if not em540_master.connected:
          await em540_master.connect()


        # Now we can read data
        if await em540_master.read_data():
            await em540_slave.data_ready()
        else:
            em540_slave.data_failed()

        timeline += read_interval
        delta = timeline - datetime.now().timestamp()
        if delta < - 0.2:
            logger.warning(f"Falling behind schedule by {delta:.2f} seconds")
        if delta > 0:
            await asyncio.sleep(delta)
        else:
            timeline = datetime.now().timestamp()


async def main(source_host, source_port, target_host, target_port):
    logger.info("Starting EM540 to Fronius TS-65A converter")

    await process_loop(source_host, source_port, target_host, target_port)

    # await em540.connect()
    # fronius = FroniusTS65A(target_host, target_port)
    # await fronius.connect()
    pass

if __name__ == '__main__':
    args = parse_args()
    # set up logger with default level of DEBUG and log to console and time
    logging.basicConfig(level=logging.INFO)


    asyncio.run(main(args.source_host, args.source_port, args.target_host, args.target_port))
