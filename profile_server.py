"""Steady-state server profile: isolated downstream serving under realistic client load.

This test profiles ONLY the downstream Modbus server serving clients —
no upstream master, no listener workers, no cross-thread scheduling.
It isolates the pymodbus server's request handling CPU cost.

Run on target hardware:
    python -m app  # (stop the main service first)
    python profile_server.py

Or standalone (no app dependencies needed beyond pymodbus):
    pip install pymodbus==3.13.1
    python profile_server.py

Output: prints top-40 functions by self-time, plus a summary.
Results are also saved to profile_results.txt for easy collection.
"""

import asyncio
import cProfile
import pstats
import io
import sys
import time

from pymodbus.simulator.simdevice import SimDevice
from pymodbus.simulator.simdata import SimData, DataType
from pymodbus.server import ModbusTcpServer
from pymodbus.client import AsyncModbusTcpClient
from pymodbus import FramerType


# --- Configuration ---
SERVER_PORT = 15400
SLAVE_ID = 1
DURATION_SECONDS = 15
NUM_CLIENTS = 2
CLIENT_HZ = 50  # Each client polls at 50Hz
READS_PER_CYCLE = 3  # 3 block reads per poll cycle
REGISTERS_PER_READ = 20  # 20 registers per block read
# Total: 2 clients × 50Hz × 3 reads = 300 reads/s


def build_device():
    """Build a SimDevice with 570 registers matching the EM540 layout."""
    # Contiguous block 0x0000-0x0160 (353 registers — the compatibility range)
    entries = [SimData(i, values=[1000 + (i % 100)], datatype=DataType.UINT16) for i in range(0x161)]
    # Energy block at 0x0500 (64 registers)
    entries += [SimData(0x0500 + i, values=[2000 + i], datatype=DataType.UINT16) for i in range(64)]
    # Scattered static registers
    for addr in [0x0302, 0x1002, 0x1010, 0x5000, 0x5008]:
        entries.append(SimData(addr, values=[42], datatype=DataType.UINT16))
    return SimDevice(SLAVE_ID, simdata=entries)


async def client_loop(client_id: int, stop_event: asyncio.Event, stats: dict):
    """Simulate a single client polling at CLIENT_HZ with READS_PER_CYCLE block reads."""
    client = AsyncModbusTcpClient("127.0.0.1", port=SERVER_PORT)
    await client.connect()

    interval = 1.0 / CLIENT_HZ
    cycles = 0
    errors = 0

    while not stop_event.is_set():
        start = time.perf_counter()
        for block in range(READS_PER_CYCLE):
            addr = block * REGISTERS_PER_READ
            result = await client.read_holding_registers(addr, count=REGISTERS_PER_READ, device_id=SLAVE_ID)
            if result.isError():
                errors += 1
        cycles += 1
        elapsed = time.perf_counter() - start
        sleep_time = max(0, interval - elapsed)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

    client.close()
    stats[client_id] = {"cycles": cycles, "errors": errors, "hz": cycles / DURATION_SECONDS}


async def run_profiled():
    """Run the benchmark."""
    device = build_device()
    server = ModbusTcpServer(
        context=device,
        framer=FramerType.SOCKET,
        address=("127.0.0.1", SERVER_PORT),
    )
    await server.serve_forever(background=True)

    stop_event = asyncio.Event()
    stats: dict = {}

    client_tasks = [
        asyncio.create_task(client_loop(i, stop_event, stats))
        for i in range(NUM_CLIENTS)
    ]

    await asyncio.sleep(DURATION_SECONDS)
    stop_event.set()
    await asyncio.gather(*client_tasks)
    await server.shutdown()
    return stats


def main():
    output_lines = []

    def log(msg=""):
        print(msg)
        output_lines.append(msg)

    log(f"=== pymodbus Server Profile ===")
    log(f"Config: {NUM_CLIENTS} clients × {CLIENT_HZ}Hz × {READS_PER_CYCLE}×{REGISTERS_PER_READ} regs")
    log(f"Duration: {DURATION_SECONDS}s")
    log(f"Expected: ~{NUM_CLIENTS * CLIENT_HZ * READS_PER_CYCLE} reads/s")
    log()

    pr = cProfile.Profile()
    pr.enable()
    t0 = time.perf_counter()

    stats = asyncio.run(run_profiled())

    wall_time = time.perf_counter() - t0
    pr.disable()

    log(f"Wall time: {wall_time:.2f}s")
    log()
    for cid, s in sorted(stats.items()):
        log(f"  Client {cid}: {s['cycles']} cycles ({s['hz']:.1f} Hz), {s['errors']} errors")
    total_reads = sum(s["cycles"] * READS_PER_CYCLE for s in stats.values())
    log(f"  Total reads served: {total_reads} ({total_reads/DURATION_SECONDS:.0f}/s)")
    log()

    # Profile output
    s_buf = io.StringIO()
    ps = pstats.Stats(pr, stream=s_buf).sort_stats("tottime")
    ps.print_stats(40)
    profile_text = s_buf.getvalue()

    log("=== Top 40 by self time ===")
    log(profile_text)

    # Save to file
    with open("profile_results.txt", "w") as f:
        f.write("\n".join(output_lines))

    log(f"\nResults saved to profile_results.txt")


if __name__ == "__main__":
    main()
