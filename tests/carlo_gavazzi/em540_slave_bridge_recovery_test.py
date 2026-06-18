"""Integration test: prove whether the Em540Slave can recover after an upstream
outage during which downstream clients flood connections.

This test exercises the REAL server threads, REAL TCP connections, and REAL
circuit breaker logic — no mocks of the data pipeline.

Scenario:
1. Start Em540Slave with real server threads (dedicated event loop)
2. Continuously feed data at simulated 10Hz (background thread)
3. Verify downstream reads succeed while data is flowing
4. Simulate upstream outage: stop feeding → circuit opens
5. Flood the server with downstream TCP connections (client retries)
6. Resume feeding data (upstream recovery)
7. Assert: circuit closes AND downstream reads succeed again

If the listener/server thread is blocked/deadlocked by connection flood,
step 7 will fail — proving the silent thread failure hypothesis.
"""

import asyncio
import socket
import struct
import threading
import time
import unittest
from types import SimpleNamespace

from pymodbus import FramerType
from pymodbus.client import AsyncModbusTcpClient

from app.carlo_gavazzi.em540_data import Em540Frame
from app.carlo_gavazzi.em540_slave_bridge import Em540Slave
from app.carlo_gavazzi.meter_data import MeterData


def _make_config(tcp_port: int, rtu_port: int):
    """Create a real-shaped config namespace for Em540Slave."""
    return SimpleNamespace(
        host="127.0.0.1",
        rtu_port=rtu_port,
        tcp_port=tcp_port,
        slave_id=1,
        update_timeout=0.5,  # Match production config
        log_level="DEBUG",
    )


def _make_meter_data(frame: Em540Frame) -> MeterData:
    """Create a MeterData snapshot with valid values safe for remap."""
    data = MeterData()
    data.frame = frame

    # Put small safe values in the dynamic primary block (INT32 pairs)
    primary = frame.dynamic_reg_map[0x0000]
    primary.values = [0] * len(primary.values)
    for i in range(0, min(len(primary.values), 48), 2):
        primary.values[i] = 1000  # low word
        primary.values[i + 1] = 0  # high word

    # Energy block — zeros
    energy = frame.dynamic_reg_map[0x0500]
    energy.values = [0] * len(energy.values)

    # Trigger static register change so _static_synced becomes True
    first_static_addr = next(iter(frame.static_reg_map))
    frame.static_reg_map[first_static_addr].values = [1744]  # EM540 device type

    frame.remap_registers()
    data._timestamp = time.time()
    return data


async def _create_and_start_slave(tcp_port: int, rtu_port: int) -> tuple["Em540Slave", Em540Frame]:
    """Create and start Em540Slave inside a running event loop."""
    frame = Em540Frame()
    config = _make_config(tcp_port=tcp_port, rtu_port=rtu_port)
    slave = Em540Slave(config, frame)
    await slave.start()
    return slave, frame


def _get_free_port() -> int:
    """Get an available TCP port from the OS."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 3.0):
    """Wait until a TCP port is accepting connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.1)
        try:
            s.connect(("127.0.0.1", port))
        except ConnectionRefusedError, OSError:
            pass
        else:
            s.close()
            return
        finally:
            s.close()
        time.sleep(0.05)
    raise TimeoutError(f"Port {port} not ready within {timeout}s")


def _flood_connections(host: str, port: int, count: int, timeout: float = 1.0) -> list[socket.socket]:
    """Open connections and send Modbus requests, keeping sockets open.

    Simulates downstream clients retrying during an outage — each opens a new
    TCP connection and sends a read request. Does NOT wait for responses
    to keep the flood fast.
    """
    sockets = []
    for i in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.settimeout(timeout)
            s.connect((host, port))

            # Send a Modbus TCP read request (function code 3)
            transaction_id = i + 1
            request = struct.pack(
                ">HHHBBHH",
                transaction_id,
                0,  # protocol
                6,  # length
                1,  # unit_id
                3,  # function: read holding registers
                0,  # address
                8,  # count
            )
            s.sendall(request)

            # Don't wait for response — just keep the socket open
            # (simulates a client that opens, sends, and hangs)
            s.setblocking(False)
            sockets.append(s)
        except ConnectionRefusedError, OSError:
            s.close()
            break
    return sockets


class _DataFeeder:
    """Background thread that continuously calls new_data() at ~20Hz,
    faster than the 0.5s bridge timeout to keep the circuit closed.
    """

    def __init__(self, slave: Em540Slave, meter_data: MeterData):
        self._slave = slave
        self._meter_data = meter_data
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="test-data-feeder")
        self._error: BaseException | None = None

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=3.0)
        if self._thread.is_alive():
            raise RuntimeError("Data feeder thread did not stop within 3s — likely deadlocked")

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    @property
    def error(self) -> BaseException | None:
        return self._error

    def _run(self):
        loop = asyncio.new_event_loop()
        try:
            while not self._stop.is_set():
                self._meter_data._timestamp = time.time()
                loop.run_until_complete(self._slave.new_data(self._meter_data))
                self._stop.wait(0.05)  # ~20Hz
        except Exception as e:
            self._error = e
        finally:
            loop.close()


class TestEm540SlaveRecoveryAfterOutage(unittest.TestCase):
    """Integration test: proves whether Em540Slave recovers after outage + connection flood."""

    def setUp(self):
        self.slave: Em540Slave | None = None
        self.flood_sockets: list[socket.socket] = []
        self.feeder: _DataFeeder | None = None

    def tearDown(self):
        if self.feeder is not None:
            self.feeder.stop()
        for s in self.flood_sockets:
            try:
                s.close()
            except Exception:
                pass
        if self.slave is not None:
            self.slave.stop()
        time.sleep(0.2)

    def test_basic_read_with_continuous_feed(self):
        """Sanity check: downstream read succeeds when data is continuously fed."""
        asyncio.run(self._async_test_basic_read())

    def test_recovery_after_outage_with_connection_flood(self):
        """Prove: Em540Slave recovers from outage + connection flood.

        This is the exact scenario from the production failure. Prior to the fix,
        200 connections would starve the server loop and _flush_writes() (which used
        run_coroutine_threadsafe) would block forever, preventing recovery.

        With direct lock-based register writes, the listener thread no longer depends
        on the server loop for writes, so recovery succeeds regardless of connection count.
        """
        asyncio.run(self._async_test_recovery_with_flood())

    def test_recovery_without_connection_flood_as_control(self):
        """Control: recovery works without flood (proves flood is the trigger)."""
        asyncio.run(self._async_test_recovery_without_flood())

    # --- Test implementations ---

    async def _async_test_basic_read(self):
        tcp_port = _get_free_port()
        rtu_port = _get_free_port()
        slave, frame = await _create_and_start_slave(tcp_port, rtu_port)
        self.slave = slave
        _wait_for_port(tcp_port)

        # Start continuous data feed
        meter_data = _make_meter_data(frame)
        self.feeder = _DataFeeder(slave, meter_data)
        self.feeder.start()

        # Wait for feeder to close the circuit (poll with timeout)
        deadline = time.time() + 3.0
        while slave._pdu_helper.circuit_open and time.time() < deadline:
            await asyncio.sleep(0.05)

        self.assertFalse(slave._pdu_helper.circuit_open, "Circuit should be closed with active feed")
        self.assertIsNone(self.feeder.error, f"Feeder crashed: {self.feeder.error}")

        # Read using pymodbus client
        client = AsyncModbusTcpClient(host="127.0.0.1", port=tcp_port, framer=FramerType.SOCKET, timeout=3.0)
        try:
            await client.connect()
            self.assertTrue(client.connected, "Could not connect to downstream server")
            result = await client.read_holding_registers(0, count=2, device_id=1)
            self.assertFalse(result.isError(), f"Read failed while data is flowing: {result}")
        finally:
            client.close()

    async def _async_test_recovery_with_flood(self):
        tcp_port = _get_free_port()
        rtu_port = _get_free_port()
        slave, frame = await _create_and_start_slave(tcp_port, rtu_port)
        self.slave = slave
        _wait_for_port(tcp_port)

        # --- Phase 1: System running normally ---
        meter_data = _make_meter_data(frame)
        self.feeder = _DataFeeder(slave, meter_data)
        self.feeder.start()

        # Wait for feeder to close the circuit (poll with timeout)
        deadline = time.time() + 3.0
        while slave._pdu_helper.circuit_open and time.time() < deadline:
            await asyncio.sleep(0.05)

        self.assertFalse(slave._pdu_helper.circuit_open, "Circuit should be closed initially")

        # Verify downstream read works
        client = AsyncModbusTcpClient(host="127.0.0.1", port=tcp_port, framer=FramerType.SOCKET, timeout=3.0)
        try:
            await client.connect()
            result = await client.read_holding_registers(0, count=2, device_id=1)
            self.assertFalse(result.isError(), f"Initial read should succeed: {result}")
        finally:
            client.close()

        # --- Phase 2: Upstream outage ---
        self.feeder.stop()
        self.feeder = None

        # Simulate what the master does during outage: call read_failed()
        await slave.read_failed()

        # Wait for circuit to open (update_timeout=0.5s)
        await asyncio.sleep(0.2)
        self.assertTrue(slave._pdu_helper.circuit_open, "Circuit should open after feed stops")

        # --- Phase 3: Connection flood during outage ---
        self.flood_sockets = _flood_connections("127.0.0.1", tcp_port, count=200)
        actual_flood = len(self.flood_sockets)
        self.assertGreater(actual_flood, 50, f"Only opened {actual_flood} flood connections, need >50")

        # Let the server process all those connections
        await asyncio.sleep(0.5)

        # --- Phase 4: Upstream recovers (resume feed) ---
        meter_data_2 = _make_meter_data(frame)
        self.feeder = _DataFeeder(slave, meter_data_2)
        self.feeder.start()

        # Wait for circuit to close — if the feeder thread is blocked by the
        # overwhelmed server loop, this will time out.
        deadline = time.time() + 10.0
        while slave._pdu_helper.circuit_open and time.time() < deadline:
            await asyncio.sleep(0.1)

        # --- Phase 5: Verify recovery ---
        self.assertTrue(self.feeder.is_alive, "Data feeder thread died — new_data() raised an exception or deadlocked")
        self.assertIsNone(self.feeder.error, f"Data feeder crashed with: {self.feeder.error}")
        self.assertFalse(
            slave._pdu_helper.circuit_open,
            "Circuit breaker did NOT close after recovery — listener/server thread "
            "is deadlocked or starved by the connection flood!",
        )

        # Downstream read should succeed
        client = AsyncModbusTcpClient(host="127.0.0.1", port=tcp_port, framer=FramerType.SOCKET, timeout=3.0)
        try:
            await client.connect()
            self.assertTrue(client.connected, "Could not connect after recovery")
            result = await client.read_holding_registers(0, count=2, device_id=1)
            self.assertFalse(
                result.isError(),
                f"Downstream read FAILED after recovery (got {result}) — "
                "system cannot recover from outage + connection flood!",
            )
        finally:
            client.close()

    async def _async_test_recovery_without_flood(self):
        tcp_port = _get_free_port()
        rtu_port = _get_free_port()
        slave, frame = await _create_and_start_slave(tcp_port, rtu_port)
        self.slave = slave
        _wait_for_port(tcp_port)

        # Phase 1: System running
        meter_data = _make_meter_data(frame)
        self.feeder = _DataFeeder(slave, meter_data)
        self.feeder.start()

        deadline = time.time() + 3.0
        while slave._pdu_helper.circuit_open and time.time() < deadline:
            await asyncio.sleep(0.05)
        self.assertFalse(slave._pdu_helper.circuit_open)

        # Phase 2: Outage (no flood)
        self.feeder.stop()
        self.feeder = None
        await slave.read_failed()
        await asyncio.sleep(0.2)
        self.assertTrue(slave._pdu_helper.circuit_open)

        # Phase 3: Recovery without flood
        meter_data_2 = _make_meter_data(frame)
        self.feeder = _DataFeeder(slave, meter_data_2)
        self.feeder.start()

        deadline = time.time() + 5.0
        while slave._pdu_helper.circuit_open and time.time() < deadline:
            await asyncio.sleep(0.1)

        # Phase 4: Verify
        self.assertFalse(slave._pdu_helper.circuit_open, "Circuit should close after recovery (no flood)")

        client = AsyncModbusTcpClient(host="127.0.0.1", port=tcp_port, framer=FramerType.SOCKET, timeout=3.0)
        try:
            await client.connect()
            result = await client.read_holding_registers(0, count=2, device_id=1)
            self.assertFalse(result.isError(), f"Read should succeed after recovery (no flood): {result}")
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
