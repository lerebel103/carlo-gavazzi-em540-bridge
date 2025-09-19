#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import asyncio
from em540_client import Em540Client
import logging

logger = logging.getLogger()

read_interval = 0.5  # seconds

def parse_args():
    parser = argparse.ArgumentParser(description='EM540 to Fronius TS-65A converter')
    parser.add_argument('--source-host', type=str, default='127.0.0.1', help='Default EM540 modbus host to bind to')
    parser.add_argument('--source-port', type=int, default=503, help='Default modbus EM540 port to bind to')
    parser.add_argument('--target-host', type=str, default='127.0.0.1', help='Host on which a TS-65A is emulated')
    parser.add_argument('--target-port', type=int, default=502, help='Port on which a TS-65A is emulated')
    return parser.parse_args()

async def process_loop(source_host, source_port, target_host, target_port):
    em540 = Em540Client(source_host, source_port)

    while True:
        # Ensure em540 client is connected
         if not em540.connected:
             await em540.connect()

         # Now we can read data
         await em540.read_data()

         await asyncio.sleep(read_interval)


async def main(source_host, source_port, target_host, target_port):
    logger.info("Starting EM540 to Fronius TS-65A converter")

    await process_loop(source_host, source_port, target_host, target_port)

    # await em540.connect()
    # fronius = FroniusTS65A(target_host, target_port)
    # await fronius.connect()
    pass

if __name__ == '__main__':
    args = parse_args()
    # set up logger with default level of DEBUG and log to console
    logging.basicConfig(level=logging.DEBUG)


    asyncio.run(main(args.source_host, args.source_port, args.target_host, args.target_port))
