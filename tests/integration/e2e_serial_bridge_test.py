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
from pathlib import Path

import pytest
from pymodbus import FramerType
from pymodbus.client import ModbusTcpClient

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
    wait_for_downstream_data,
    wait_for_register_coverage,
)
from .serial_helpers import ModbusRtuServer
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
        """Refresh the server's register map from the current frame."""
        values: dict[int, int] = {}
        # Dynamic registers laid down first, then static registers overwrite overlaps
        for reg_map in (self.frame.dynamic_reg_map, self.frame.static_reg_map):
            for addr, reg in reg_map.items():
                for i, value in enumerate(reg.values):
                    values[addr + i] = int(value)
        self.server.set_registers(values)

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

    def __init__(self, em540_tcp_port: int, em540_rtu_port: int, ts65a_tcp_port: int) -> None:
        self.em540_tcp = ModbusTcpClient(host="127.0.0.1", port=em540_tcp_port, framer=FramerType.SOCKET, timeout=2.0)
        self.em540_rtu = ModbusTcpClient(host="127.0.0.1", port=em540_rtu_port, framer=FramerType.RTU, timeout=2.0)
        self.ts65a_tcp = ModbusTcpClient(host="127.0.0.1", port=ts65a_tcp_port, framer=FramerType.SOCKET, timeout=2.0)

    def connect_all(self) -> None:
        """Connect all clients."""
        for client in [self.em540_tcp, self.em540_rtu, self.ts65a_tcp]:
            connect_modbus_client(client)

    def disconnect_all(self) -> None:
        """Disconnect all clients."""
        for client in [self.em540_tcp, self.em540_rtu, self.ts65a_tcp]:
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

    def _read_all_blocks(self, client) -> list[list[int]]:
        """Read all expected register blocks from a client."""
        return [
            read_holding_registers(client, 0x000B, len(self.expected_blocks["static_000b"])),
            read_holding_registers(client, 0x0000, len(self.expected_blocks["primary"])),
            read_holding_registers(client, 0x0500, len(self.expected_blocks["energy"])),
            read_holding_registers(client, 0x0033, len(self.expected_blocks["remapped_0033"])),
            read_holding_registers(client, 0x0110, len(self.expected_blocks["remapped_0110"])),
            read_holding_registers(client, 0x0034, len(self.expected_blocks["remapped_0034"])),
            read_holding_registers(client, 0x0112, len(self.expected_blocks["remapped_0112"])),
        ]

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
            List of floats representing the expected dynamic values (first 11 floats only,
            which are the stable current/voltage/frequency measurements before smoothing effects).
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
        # Read the dynamic values register block
        ts65a_dynamic = read_holding_registers(client, 40071, 90)  # 45 floats = 90 registers
        ts65a_decoded = decode_ts65a_dynamic_values(ts65a_dynamic)
        expected_decoded = self.compute_expected_values()

        # Validate only the stable subset (first 11 floats: current, voltage, frequency)
        # Energy/VAh fields are smoothed and less stable
        assert ts65a_decoded[:11] == pytest.approx(expected_decoded[:11], rel=1e-3, abs=1e-3), (
            f"TS65A TCP dynamic values mismatch: got {ts65a_decoded[:3]}... expected {expected_decoded[:3]}..."
        )

    def validate_signature_registers(self, client) -> None:
        """Validate TS65A signature registers (device ID)."""
        signature = read_holding_registers(client, 40000, 2)
        assert signature == [21365, 28243], f"TS65A signature mismatch: {signature}"


@pytest.mark.integration
def test_end_to_end_serial_and_tcp_clients_observe_expected_data() -> None:
    """End-to-end test: upstream serial EM540 → downstream TCP/RTU slaves.

    Exercises the full data path:
    1. Upstream virtual EM540 meter over serial (PTY)
    2. Service reads upstream via Modbus RTU, transforms, stores
    3. Downstream EM540 slaves serve over TCP and RTU
    4. Downstream TS65A slave serves over TCP
    5. TCP and RTU clients validate identical data
    6. All register coverage (static, dynamic, remapped) validated
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Allocate free ports for downstream slaves
        em540_tcp_port = find_free_port()
        em540_rtu_port = find_free_port()
        ts65a_tcp_port = find_free_port()

        # Set up upstream simulator
        upstream = Em540UpstreamSimulator(frame_seed=100)
        upstream.start()

        # Generate service config
        config_path = tmp_path / "config.yaml"
        generate_config(
            config_path,
            upstream.port,
            em540_tcp_port,
            em540_rtu_port,
            ts65a_tcp_port,
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
            clients = DownstreamClients(em540_tcp_port, em540_rtu_port, ts65a_tcp_port)
            clients.connect_all()

            # Verify full register coverage before validating data
            expected_requests = {(addr, len(reg.values)) for addr, reg in upstream.frame.static_reg_map.items()}
            expected_requests.update(
                {
                    (0x0000, len(upstream.frame.dynamic_reg_map[0x0000].values)),
                    (0x0500, 32),
                    (0x0520, 32),
                }
            )
            wait_for_register_coverage(upstream, expected_requests, timeout=60.0)

            # Wait for downstream to start serving non-zero data
            wait_for_downstream_data(clients.em540_tcp, address=0x000B)

            # Validate EM540 data on both TCP and RTU paths
            em540_validator = Em540Validator(upstream.frame)
            em540_validator.validate_tcp_client(clients.em540_tcp)
            em540_validator.validate_rtu_client(clients.em540_rtu)

            # Validate TS65A data on TCP path
            ts65a_validator = Ts65aValidator(upstream.frame)
            ts65a_validator.validate_tcp_client(clients.ts65a_tcp)
            ts65a_validator.validate_signature_registers(clients.ts65a_tcp)

            # Verify service is still healthy
            service.assert_running()

            clients.disconnect_all()

        finally:
            service.stop()
            upstream.stop()
