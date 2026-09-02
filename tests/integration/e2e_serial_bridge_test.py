"""End-to-end integration test for serial and TCP Modbus bridges.

This test exercises:
- Upstream EM540 master reading from a virtual serial device
- Downstream EM540 slave serving data over TCP and RTU-over-TCP
- Downstream TS65A slave serving Fronius-compatible data over TCP
- Full register coverage including dynamic and static blocks
- Concurrent TCP and RTU clients validating the same data
"""

from __future__ import annotations

import logging
import tempfile
import time
from pathlib import Path

import pytest
from pymodbus import FramerType
from pymodbus.client import ModbusSerialClient, ModbusTcpClient
from pymodbus.exceptions import ModbusException

from app.carlo_gavazzi.meter_data import MeterData
from app.fronius.ts65a_data import Ts65aMeterData
from app.fronius.ts65a_slave_stats import Ts65aSlaveStats

from .config_gen import generate_config
from .modbus_client import (
    connect_modbus_client,
    find_free_port,
    read_holding_registers,
    wait_for_port,
)
from .orchestration import (
    wait_for_condition,
    wait_for_downstream_data,
)
from .serial_helpers import ModbusRtuServer, SerialCable
from .service_process import ServiceProcess
from .test_data import (
    decode_ts65a_dynamic_values,
    encode_ts65a_dynamic_values,
    expected_em540_blocks,
    make_em540_source_frame,
)

logger = logging.getLogger("integration")


class Em540UpstreamSimulator:
    """Simulates a real EM540 meter over a virtual serial port.

    This server receives Modbus RTU requests and serves a register map
    that reflects the state of a simulated meter.
    """

    def __init__(self, frame_seed: int = 100) -> None:
        self.server = ModbusRtuServer("upstream-em540", slave_id=1)
        self.frame = make_em540_source_frame(seed=frame_seed)
        self._update_registers()

    def _update_registers(self) -> None:
        """Refresh the server's register map from the current frame.

        Models real EM540 behaviour: the instantaneous-variables block
        (0x0000..) is a contiguous run of INT32/INT16 measurements, and a
        block scan returns those raw measurement words. Some static/identity
        registers (e.g. 0x000B "Carlo Gavazzi identification code") share an
        address that falls *inside* that dynamic block — on real hardware that
        register reads as the (zero) high word of the V L3-L1 INT32 during a
        block scan, and the identity code is only returned via a separate
        single-word read. So static registers must NOT overwrite addresses
        already occupied by a dynamic register; doing so previously injected the
        device-type code (1744) into the V L3-L1 high word and corrupted the
        parsed line-to-line voltage — a value a real meter never emits.
        """
        values: dict[int, int] = {}

        # Lay down dynamic registers first and record every address they occupy.
        dynamic_addrs: set[int] = set()
        for addr, reg in self.frame.dynamic_reg_map.items():
            for i, value in enumerate(reg.values):
                values[addr + i] = int(value)
                dynamic_addrs.add(addr + i)

        # Add static registers only at addresses NOT already served by the
        # dynamic block, mirroring how a real meter lays out its register space.
        # Static registers that overlap a dynamic address are exposed instead as
        # single-word-read overrides below (see set_single_word_overrides), so a
        # standalone read of e.g. 0x000B returns the identification code while a
        # block scan returns the (zero) V L3-L1 high word.
        overlap_overrides: dict[int, int] = {}
        for addr, reg in self.frame.static_reg_map.items():
            for i, value in enumerate(reg.values):
                if (addr + i) in dynamic_addrs:
                    # Only single-register static fields can be represented as a
                    # single-word override without ambiguity.
                    if len(reg.values) == 1:
                        overlap_overrides[addr + i] = int(value)
                    continue
                values[addr + i] = int(value)

        self.server.set_registers(values)
        self.server.set_single_word_overrides(overlap_overrides)

    def start(self) -> None:
        """Start the simulator server."""
        self._update_registers()
        self.server.start()

    def stop(self) -> None:
        """Stop the simulator server."""
        self.server.stop()

    def publish(self, frame) -> None:
        """Update to a new frame and refresh registers."""
        self.frame = frame
        self._update_registers()

    @property
    def port(self) -> str:
        """Get the virtual serial port path."""
        return self.server.port

    @property
    def requests(self) -> list[tuple[float, int, int]]:
        """Get all requests received by the server."""
        return self.server.requests

    @property
    def connect_events(self) -> list[bool]:
        """Get all connection state changes."""
        return self.server.connect_events


class DownstreamClients:
    """Manages Modbus clients connecting to downstream slaves.

    Provides a unified interface for TCP and RTU clients targeting
    both EM540 and TS65A slaves.
    """

    def __init__(
        self,
        em540_tcp_port: int,
        em540_rtu_port: int,
        ts65a_tcp_port: int,
        em540_serial_port: str,
        ts65a_serial_port: str,
    ) -> None:
        self.em540_tcp = ModbusTcpClient(host="127.0.0.1", port=em540_tcp_port, framer=FramerType.SOCKET, timeout=2.0)
        self.em540_rtu = ModbusTcpClient(host="127.0.0.1", port=em540_rtu_port, framer=FramerType.RTU, timeout=2.0)
        self.ts65a_tcp = ModbusTcpClient(host="127.0.0.1", port=ts65a_tcp_port, framer=FramerType.SOCKET, timeout=2.0)
        self.em540_serial = ModbusSerialClient(
            port=em540_serial_port,
            framer=FramerType.RTU,
            baudrate=9600,
            parity="N",
            bytesize=8,
            stopbits=1,
            timeout=1.0,
            retries=1,
            handle_local_echo=False,
        )
        self.ts65a_serial = ModbusSerialClient(
            port=ts65a_serial_port,
            framer=FramerType.RTU,
            baudrate=9600,
            parity="N",
            bytesize=8,
            stopbits=1,
            timeout=1.0,
            retries=1,
            handle_local_echo=False,
        )

    def connect_all(self) -> None:
        """Connect all clients."""
        for client in [self.em540_tcp, self.em540_rtu, self.ts65a_tcp, self.em540_serial, self.ts65a_serial]:
            connect_modbus_client(client)

    def disconnect_all(self) -> None:
        """Disconnect all clients."""
        for client in [self.em540_tcp, self.em540_rtu, self.ts65a_tcp, self.em540_serial, self.ts65a_serial]:
            client.close()


class Em540Validator:
    """Validates EM540 register data from upstream and downstream.

    Reads expected values from the upstream frame, compares them against
    what downstream clients observe, and validates both TCP and RTU paths.
    """

    def __init__(self, upstream_frame) -> None:
        self.upstream_frame = upstream_frame
        self.expected_blocks = expected_em540_blocks(upstream_frame)

    def validate_tcp_client(self, client) -> None:
        """Validate EM540 TCP client against expected data."""
        reads = self._read_all_blocks(client)
        self._assert_all_blocks_match(reads, "TCP")

    def validate_rtu_client(self, client) -> None:
        """Validate EM540 RTU client against expected data."""
        reads = self._read_all_blocks(client)
        self._assert_all_blocks_match(reads, "RTU")

    def validate_serial_client(self, client) -> None:
        """Validate EM540 serial client against the full expected register set.

        The serial slave shares the same register context as the TCP/RTU slaves,
        so it must serve identical data. All seven blocks are verified (same as
        TCP/RTU) rather than a representative subset. Serial transport over PTY
        can occasionally timeout under CI load, so callers retry via
        wait_for_condition; a ModbusException here propagates (rather than being
        swallowed) so a permanently unresponsive serial path fails the test
        instead of being treated as a pass.
        """
        reads = self._read_all_blocks(client)
        self._assert_all_blocks_match(reads, "SERIAL")

    def _read_all_blocks(self, client) -> list[list[int]]:
        """Read all expected register blocks from a client."""
        return [
            self._read_stable_holding_registers(client, 0x000B, len(self.expected_blocks["static_000b"])),
            self._read_stable_holding_registers(client, 0x0000, len(self.expected_blocks["primary"])),
            self._read_stable_holding_registers(client, 0x0500, len(self.expected_blocks["energy"])),
            self._read_stable_holding_registers(client, 0x0033, len(self.expected_blocks["remapped_0033"])),
            self._read_stable_holding_registers(client, 0x0110, len(self.expected_blocks["remapped_0110"])),
            self._read_stable_holding_registers(client, 0x0034, len(self.expected_blocks["remapped_0034"])),
            self._read_stable_holding_registers(client, 0x0112, len(self.expected_blocks["remapped_0112"])),
        ]

    def _read_stable_holding_registers(self, client, address: int, count: int) -> list[int]:
        """Read a block twice and return it only if the values are stable.

        The service updates downstream register buffers continuously. A single read
        may land while a new snapshot is being published, so the test waits for two
        identical reads before treating the data as representative.
        """
        deadline = time.monotonic() + 10.0
        last_read: list[int] | None = None

        while time.monotonic() < deadline:
            current_read = read_holding_registers(client, address, count)
            if last_read == current_read:
                return current_read
            last_read = current_read
            time.sleep(0.02)

        raise TimeoutError(f"register block {hex(address)} did not stabilize within timeout")

    def _assert_all_blocks_match(self, reads: list[list[int]], label: str) -> None:
        """Assert that all blocks match expected values."""
        block_names = [
            "static_000b",
            "primary",
            "energy",
            "remapped_0033",
            "remapped_0110",
            "remapped_0034",
            "remapped_0112",
        ]
        for read, expected_key in zip(reads, block_names):
            expected = self.expected_blocks[expected_key]
            assert read == expected, f"{label} mismatch at {expected_key}: got {read[:5]}... expected {expected[:5]}..."


class Ts65aValidator:
    """Validates TS65A register data from downstream TCP client.

    Validates the transformation of EM540 data to Fronius TS65A format,
    including smoothing and derived fields.
    """

    def __init__(self, upstream_frame) -> None:
        self.upstream_frame = upstream_frame

    def compute_expected_values(self) -> list[float]:
        """Compute expected TS65A values from the upstream frame.

        Returns:
            List of all 45 expected dynamic float values, matching the TS65A
            register layout served downstream.
        """
        meter_data = MeterData()
        meter_data.frame = self.upstream_frame
        meter_data.update_from_frame()
        ts65a_model = Ts65aMeterData(
            20,  # smoothing_num_points
            -5000,  # grid_feed_in_hard_limit
            logging.getLogger("integration-ts65a"),  # logger
            Ts65aSlaveStats(),  # stats
        )
        ts65a_model.update(meter_data)
        return decode_ts65a_dynamic_values(encode_ts65a_dynamic_values(ts65a_model))

    def validate_tcp_client(self, client) -> None:
        """Validate TS65A TCP client against expected data."""
        # Read the dynamic values register block (45 floats = 90 registers).
        ts65a_dynamic = self._read_stable_holding_registers(client, 40071, 90)
        ts65a_decoded = decode_ts65a_dynamic_values(ts65a_dynamic)
        expected_decoded = self.compute_expected_values()

        # Verify all 45 decoded floats. The current/voltage/power/etc. fields are
        # fed through bounded moving averages, but for a fixed upstream frame those
        # averages converge to the instantaneous value; the energy (Wh) and VAh
        # fields are assigned directly (no smoothing). _read_stable_holding_registers
        # waits for two identical reads, so we only compare a converged snapshot.
        assert ts65a_decoded == pytest.approx(expected_decoded, rel=1e-3, abs=1e-3), (
            f"TS65A TCP dynamic values mismatch: got {ts65a_decoded[:3]}... expected {expected_decoded[:3]}..."
        )

    def validate_signature_registers(self, client) -> None:
        """Validate TS65A signature registers (device ID)."""
        signature = self._read_stable_holding_registers(client, 40000, 2)
        assert signature == [21365, 28243], f"TS65A signature mismatch: {signature}"

    def validate_serial_client(self, client) -> None:
        """Validate TS65A serial client against the full expected value set.

        The serial slave shares the same register context as the TCP slave, so it
        must serve identical data. Signature registers plus all 45 dynamic floats
        are verified (same as TCP). A ModbusException here propagates (rather than
        being swallowed) so a permanently unresponsive serial path fails the test
        instead of being treated as a pass; transient PTY timeouts are handled by
        the caller's retry loop via wait_for_condition.
        """
        self.validate_signature_registers(client)
        ts65a_dynamic = self._read_stable_holding_registers(client, 40071, 90)
        decoded = decode_ts65a_dynamic_values(ts65a_dynamic)
        expected_decoded = self.compute_expected_values()
        assert decoded == pytest.approx(expected_decoded, rel=1e-3, abs=1e-3), (
            f"TS65A serial dynamic values mismatch: got {decoded[:3]} expected {expected_decoded[:3]}"
        )

    def _read_stable_holding_registers(self, client, address: int, count: int) -> list[int]:
        """Read a block twice and return it only if the values are stable."""
        deadline = time.monotonic() + 10.0
        last_read: list[int] | None = None

        while time.monotonic() < deadline:
            current_read = read_holding_registers(client, address, count)
            if last_read == current_read:
                return current_read
            last_read = current_read
            time.sleep(0.02)

        raise TimeoutError(f"register block {hex(address)} did not stabilize within timeout")


@pytest.mark.integration
def test_end_to_end_serial_and_tcp_clients_observe_expected_data() -> None:
    """End-to-end test: upstream serial EM540 → downstream TCP/RTU slaves.

    Exercises the full data path:
    1. Upstream virtual EM540 meter starts over serial (PTY)
    2. Service reads upstream via Modbus RTU, transforms, stores
    3. Downstream EM540 slaves serve over TCP and RTU
    4. Downstream TS65A slave serves over TCP
    5. TCP and RTU clients validate identical data
    6. On upstream outage, downstream paths return Modbus exceptions (not stale/zero payloads)
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Allocate free ports for downstream slaves
        em540_tcp_port = find_free_port()
        em540_rtu_port = find_free_port()
        ts65a_tcp_port = find_free_port()

        em540_serial = SerialCable("downstream-em540")
        ts65a_serial = SerialCable("downstream-ts65a")

        # Set up upstream simulator
        upstream = Em540UpstreamSimulator(frame_seed=100)
        upstream.start()

        try:
            # Generate service config
            config_path = tmp_path / "config.yaml"
            generate_config(
                config_path,
                upstream.port,
                em540_tcp_port,
                em540_rtu_port,
                ts65a_tcp_port,
                em540_serial_port=em540_serial.left_path,
                ts65a_serial_port=ts65a_serial.left_path,
            )

            # Start service
            service = ServiceProcess(config_path)
            service.start()

            try:
                # Wait for downstream ports to be ready
                wait_for_port(em540_tcp_port, timeout=20.0)
                wait_for_port(em540_rtu_port, timeout=20.0)
                wait_for_port(ts65a_tcp_port, timeout=20.0)

                # Connect downstream clients
                clients = DownstreamClients(
                    em540_tcp_port,
                    em540_rtu_port,
                    ts65a_tcp_port,
                    em540_serial.right_path,
                    ts65a_serial.right_path,
                )
                clients.connect_all()

                # Wait for downstream to start serving non-zero dynamic data.
                # Dynamic block readiness is less timing-sensitive than static overlays on CI.
                wait_for_downstream_data(clients.em540_tcp, address=0x0000, timeout=120.0)

                em540_validator = Em540Validator(upstream.frame)
                last_em540_error: str = ""

                def _em540_validated() -> bool:
                    nonlocal last_em540_error
                    try:
                        em540_validator.validate_tcp_client(clients.em540_tcp)
                        em540_validator.validate_rtu_client(clients.em540_rtu)
                        em540_validator.validate_serial_client(clients.em540_serial)
                    except (AssertionError, ModbusException) as exc:
                        last_em540_error = str(exc)
                        return False
                    return True

                wait_for_condition(
                    _em540_validated,
                    timeout=120.0,
                    message=(
                        "downstream EM540 slaves did not converge to expected values within timeout"
                        f"\nLast EM540 validation error: {last_em540_error}"
                        f"{service.diagnostics()}"
                    ),
                )

                ts65a_validator = Ts65aValidator(upstream.frame)
                last_ts65a_error: str = ""

                def _ts65a_validated() -> bool:
                    nonlocal last_ts65a_error
                    try:
                        ts65a_validator.validate_tcp_client(clients.ts65a_tcp)
                        ts65a_validator.validate_signature_registers(clients.ts65a_tcp)
                        ts65a_validator.validate_serial_client(clients.ts65a_serial)
                    except (AssertionError, ModbusException) as exc:
                        last_ts65a_error = str(exc)
                        return False
                    return True

                wait_for_condition(
                    _ts65a_validated,
                    timeout=120.0,
                    message=(
                        "downstream TS65A slave did not converge to expected values within timeout"
                        f"\nLast TS65A validation error: {last_ts65a_error}"
                        f"{service.diagnostics()}"
                    ),
                )

                # Publish a new upstream snapshot with changed dynamic values.
                # Static registers stay the same because the master only refreshes
                # them on reconnect, but downstream consumers should still track
                # the evolving dynamic data.
                updated_frame = make_em540_source_frame(seed=101)
                for addr, reg in upstream.frame.static_reg_map.items():
                    updated_frame.static_reg_map[addr].values = list(reg.values)
                upstream.publish(updated_frame)

                em540_validator = Em540Validator(upstream.frame)
                ts65a_validator = Ts65aValidator(upstream.frame)
                last_em540_error = ""
                last_ts65a_error = ""

                wait_for_condition(
                    _em540_validated,
                    timeout=120.0,
                    message=(
                        "downstream EM540 slaves did not converge to updated values within timeout"
                        f"\nLast EM540 validation error: {last_em540_error}"
                        f"{service.diagnostics()}"
                    ),
                )

                wait_for_condition(
                    _ts65a_validated,
                    timeout=120.0,
                    message=(
                        "downstream TS65A slave did not converge to updated values within timeout"
                        f"\nLast TS65A validation error: {last_ts65a_error}"
                        f"{service.diagnostics()}"
                    ),
                )

                # Simulate upstream outage and verify downstream paths fail closed
                # with Modbus exceptions instead of serving stale/zero payloads.
                upstream.stop()

                def _all_downstream_paths_error() -> bool:
                    def _is_error(client, address: int) -> bool:
                        # A real Modbus exception *response* (circuit breaker firing) means
                        # result.isError() is True without raising. A raised ModbusException
                        # here means the transport itself failed (connection refused/timeout),
                        # which is a different failure mode (e.g. the dead serial path bug)
                        # and must not be conflated with the expected fail-closed behavior.
                        result = client.read_holding_registers(address, count=2, device_id=1)
                        return result.isError()

                    em540_tcp_result = _is_error(clients.em540_tcp, 0x0000)
                    em540_rtu_result = _is_error(clients.em540_rtu, 0x0000)
                    em540_serial_result = _is_error(clients.em540_serial, 0x0000)
                    ts65a_tcp_result = _is_error(clients.ts65a_tcp, 40071)
                    ts65a_serial_result = _is_error(clients.ts65a_serial, 40071)
                    return (
                        em540_tcp_result
                        and em540_rtu_result
                        and em540_serial_result
                        and ts65a_tcp_result
                        and ts65a_serial_result
                    )

                wait_for_condition(
                    _all_downstream_paths_error,
                    timeout=30.0,
                    message=(
                        "downstream paths did not return Modbus exceptions after upstream outage"
                        f"{service.diagnostics()}"
                    ),
                )

                # Verify service is still healthy
                service.assert_running()

                clients.disconnect_all()

            finally:
                service.stop()
        finally:
            upstream.stop()
            em540_serial.close()
            ts65a_serial.close()
