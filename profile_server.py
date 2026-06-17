"""Steady-state server profile: clean reads only, no error paths.

Ensures:
- All client reads hit valid register ranges (zero exceptions)
- Profiler only starts AFTER clients are connected and flowing
- No trace_pdu callback (eliminates circuit breaker path)
- pymodbus logging at CRITICAL (no log formatting)

Run on target:
    docker run --rm <image> python profile_server.py
"""

import asyncio
import cProfile
import pstats
import io
import logging
import time

from pymodbus.simulator.simdevice import SimDevice
from pymodbus.simulator.simdata import SimData, DataType
from pymodbus.server import ModbusTcpServer
from pymodbus.client import AsyncModbusTcpClient
from pymodbus import FramerType, pymodbus_apply_logging_config


# --- Configuration ---
SERVER_PORT = 15400
SLAVE_ID = 1
WARMUP_SECONDS = 3  # Let connections stabilize before profiling
PROFILE_SECONDS = 15  # Profile only this window
NUM_CLIENTS = 2
CLIENT_HZ = 50
READS_PER_CYCLE = 3
REGISTERS_PER_READ = 20
# Clients read addresses 0-19, 20-39, 40-59 — all within the valid contiguous block


def build_device():
    """Build a SimDevice with a contiguous valid block covering all client read ranges."""
    # Single contiguous block: 0-199 (way more than clients will read)
    # No gaps, no invalid addresses, no exceptions possible
    entries = [SimData(i, values=[1000 + i], datatype=DataType.UINT16) for i in range(200)]
    return SimDevice(SLAVE_ID, simdata=entries)


async def client_loop(client_id: int, stop_event: asyncio.Event, stats: dict):
    """Client reads 3 contiguous blocks of 20 registers at CLIENT_HZ."""
    client = AsyncModbusTcpClient("127.0.0.1", port=SERVER_PORT)
    await client.connect()

    interval = 1.0 / CLIENT_HZ
    cycles = 0
    errors = 0

    while not stop_event.is_set():
        start = time.perf_counter()
        for block in range(READS_PER_CYCLE):
            addr = block * REGISTERS_PER_READ  # 0, 20, 40 — all valid
            result = await client.read_holding_registers(addr, count=REGISTERS_PER_READ, device_id=SLAVE_ID)
            if result.isError():
                errors += 1
        cycles += 1
        elapsed = time.perf_counter() - start
        sleep_time = max(0, interval - elapsed)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

    client.close()
    stats[client_id] = {"cycles": cycles, "errors": errors}


async def run_profiled():
    """Run with profiling only during steady state."""
    # Suppress all pymodbus logging
    pymodbus_apply_logging_config("CRITICAL")
    logging.getLogger().setLevel(logging.CRITICAL)

    device = build_device()

    # NO trace_pdu, NO trace_packet — pure datastore serving
    server = ModbusTcpServer(
        context=device,
        framer=FramerType.SOCKET,
        address=("127.0.0.1", SERVER_PORT),
    )
    await server.serve_forever(background=True)

    stop_event = asyncio.Event()
    stats: dict = {}

    # Start clients
    client_tasks = [
        asyncio.create_task(client_loop(i, stop_event, stats))
        for i in range(NUM_CLIENTS)
    ]

    # --- WARMUP: let connections establish and stabilize ---
    print(f"Warming up for {WARMUP_SECONDS}s...")
    await asyncio.sleep(WARMUP_SECONDS)

    # --- PROFILE: only measure steady state ---
    print(f"Profiling steady state for {PROFILE_SECONDS}s...")
    print(f"  {NUM_CLIENTS} clients × {CLIENT_HZ}Hz × {READS_PER_CYCLE}×{REGISTERS_PER_READ} regs")
    print(f"  Expected: ~{NUM_CLIENTS * CLIENT_HZ * READS_PER_CYCLE} reads/s")
    print(f"  All reads hit valid contiguous registers (zero exceptions)")
    print()

    pr = cProfile.Profile()
    pr.enable()
    t0 = time.perf_counter()

    await asyncio.sleep(PROFILE_SECONDS)

    wall_time = time.perf_counter() - t0
    pr.disable()

    # Stop clients
    stop_event.set()
    await asyncio.gather(*client_tasks)
    await server.shutdown()

    return pr, wall_time, stats


def main():
    pr, wall_time, stats = asyncio.run(run_profiled())

    output_lines = []

    def log(msg=""):
        print(msg)
        output_lines.append(msg)

    log("=== Steady-State Server Profile ===")
    log(f"Profile window: {wall_time:.2f}s (after {WARMUP_SECONDS}s warmup)")
    log()

    total_cycles = 0
    total_errors = 0
    for cid, s in sorted(stats.items()):
        # Subtract warmup cycles (approximate)
        profile_cycles = max(0, s["cycles"] - WARMUP_SECONDS * CLIENT_HZ)
        log(f"  Client {cid}: ~{profile_cycles} profiled cycles ({profile_cycles/PROFILE_SECONDS:.1f} Hz), {s['errors']} errors")
        total_cycles += profile_cycles
        total_errors += s["errors"]

    total_reads = total_cycles * READS_PER_CYCLE
    log(f"  Total profiled reads: ~{total_reads} ({total_reads/PROFILE_SECONDS:.0f}/s)")
    log(f"  Errors during profile: {total_errors}")
    log()

    if total_errors > 0:
        log("WARNING: Errors detected — profile may include exception paths!")
        log()

    # Profile output sorted by self time
    s_buf = io.StringIO()
    ps = pstats.Stats(pr, stream=s_buf).sort_stats("tottime")
    ps.print_stats(40)
    profile_text = s_buf.getvalue()

    log("=== Top 40 by self time ===")
    log(profile_text)

    # Save
    with open("profile_results.txt", "w") as f:
        f.write("\n".join(output_lines))
    log("Results saved to profile_results.txt")


if __name__ == "__main__":
    main()
