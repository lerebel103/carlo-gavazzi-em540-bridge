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
import struct
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


# Energy-block offset (in registers, within the 0x0500 block) used to carry a
# per-tick monotonic MARKER through the full path to the serial client. The
# energy block is served to downstream clients verbatim (it is cumulative, not
# smoothed) and is NOT bounds-checked by upstream frame validation, so stamping a
# monotonic counter here survives end-to-end without being rejected or averaged
# away. The counter is encoded as an INT64 (4 registers) exactly as
# make_em540_source_frame encodes energy values (encode_int64_le).
_MARKER_ENERGY_REG_OFFSET = 0  # first INT64 slot of the 0x0500 energy block
_MARKER_BLOCK_ADDRESS = 0x0500
_MARKER_REG_COUNT = 4


def _decode_marker_int64_le(registers: list[int]) -> int:
    """Decode the 4-register INT64 marker (little-endian word order).

    Mirrors test_data.encode_int64_le, which stores [w3, w2, w1, w0].
    """
    w3, w2, w1, w0 = registers[0], registers[1], registers[2], registers[3]
    packed = struct.pack(">4H", w0, w1, w2, w3)
    return struct.unpack(">q", packed)[0]


class Em540UpstreamSimulator:
    """Simulates a real EM540 meter over a virtual serial port.

    This server receives Modbus RTU requests and serves a register map
    that reflects the state of a simulated meter.

    ``publish_next()`` stamps a strictly-monotonic per-tick MARKER into the energy
    block (see _MARKER_* constants) and refreshes the served registers, so a
    downstream reader can prove each fresh serial read observes a NEWER frame
    (Finding 3). The marker is independent of the seed-derived field variation and
    guarantees a distinct, increasing value every tick.
    """

    def __init__(self, frame_seed: int = 100) -> None:
        self.server = ModbusRtuServer("upstream-em540", slave_id=1)
        self.frame = make_em540_source_frame(seed=frame_seed)
        self._marker: int = 0
        self._stamp_marker()
        self._update_registers()

    def _stamp_marker(self) -> None:
        """Write the current monotonic marker into the energy block INT64 slot."""
        from .test_data import encode_int64_le

        energy = self.frame.dynamic_reg_map[_MARKER_BLOCK_ADDRESS].values
        offset = _MARKER_ENERGY_REG_OFFSET
        energy[offset : offset + _MARKER_REG_COUNT] = encode_int64_le(self._marker)
        self.frame.remap_registers()

    def publish_next(self) -> int:
        """Advance to the next monotonic marker and refresh served registers.

        Returns the newly-published marker value. The rest of the frame is held
        constant so the ONLY thing changing is the marker, isolating the proof
        that each fresh serial read tracks a new upstream tick.
        """
        self._marker += 1
        self._stamp_marker()
        self._update_registers()
        return self._marker

    @property
    def marker(self) -> int:
        """The currently-published monotonic marker."""
        return self._marker

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
            # L3-L1 voltage at its remapped location, read independently of the
            # 0x000B device-type overlay to prove the voltage survives it.
            self._read_stable_holding_registers(
                client, 0x013A, len(self.expected_blocks["remapped_013a_l3l1_voltage"])
            ),
            # Device type via a standalone single-register read at 0x000B.
            self._read_stable_holding_registers(client, 0x000B, 1),
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
            "remapped_013a_l3l1_voltage",
            None,  # standalone 0x000B device-type read, asserted separately below
        ]
        for read, expected_key in zip(reads, block_names):
            if expected_key is None:
                continue
            expected = self.expected_blocks[expected_key]
            assert read == expected, f"{label} mismatch at {expected_key}: got {read[:5]}... expected {expected[:5]}..."

        # Honest, non-self-fulfilling checks for the 0x000B collision:
        #  - The L3-L1 voltage (whose INT32 high word shares address 0x000B in the
        #    raw block) reads back its true source value at the remapped location.
        #  - The device type reads back correctly from a standalone 0x000B read.
        l3l1 = reads[7]
        expected_l3l1 = self.expected_blocks["remapped_013a_l3l1_voltage"]
        assert l3l1 == expected_l3l1, (
            f"{label} L3-L1 voltage corrupted by device-type overlay: got {l3l1}, expected {expected_l3l1}"
        )
        device_type = reads[8]
        assert device_type == [self.expected_blocks["device_type_000b"]], (
            f"{label} device type mismatch at standalone 0x000B read: "
            f"got {device_type}, expected [{self.expected_blocks['device_type_000b']}]"
        )


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
            2.5,  # smoothing_window_seconds
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
                        # The expected fail-closed behavior is a real Modbus exception
                        # *response* (circuit breaker firing): result.isError() is True
                        # without raising. A raised ModbusException/OSError here means the
                        # transport itself failed transiently (e.g. a PTY read timeout on
                        # CI) — that must NOT count as the expected response, but it also
                        # must not abort the test, since wait_for_condition does not catch
                        # exceptions. So we treat a raised transport error as "not yet in
                        # the expected state" and return False to keep polling, while never
                        # conflating it with a genuine exception response.
                        try:
                            result = client.read_holding_registers(address, count=2, device_id=1)
                        except (ModbusException, OSError, ConnectionError):
                            return False
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


def _poll_serial_read_latencies(client, address: int, count: int, reads: int) -> list[float]:
    """Issue `reads` back-to-back serial reads and return per-read durations (s).

    The client polls in the tightest possible loop (no inter-read sleep), so any
    non-trivial per-read duration is attributable to the bridge deferring the
    response until a fresh frame lands (frame-latching), not to client pacing.
    A failed/exception read is skipped (not timed) so transient PTY hiccups under
    CI do not pollute the latency sample.
    """
    latencies: list[float] = []
    attempts = 0
    max_attempts = reads * 4
    while len(latencies) < reads and attempts < max_attempts:
        attempts += 1
        start = time.monotonic()
        try:
            result = client.read_holding_registers(address, count=count, device_id=1)
        except (ModbusException, OSError, ConnectionError):
            continue
        elapsed = time.monotonic() - start
        if result.isError():
            continue
        latencies.append(elapsed)
    return latencies


def _poll_serial_markers(client, upstream, *, reads: int, max_attempts: int) -> list[tuple[float, int]]:
    """Poll the serial client in a tight loop, advancing the upstream each attempt.

    Returns a list of ``(latency_s, marker)`` for each SUCCESSFUL marker read. The
    upstream publishes a NEW monotonic marker before every poll attempt, so the
    service has a fresh frame available to latch onto. The marker is decoded from
    the energy block (served verbatim, not smoothed), so the returned value is the
    exact frame the serial read observed.

    This is the heart of the Finding-3 proof: with latching ON each successful
    read must observe a STRICTLY NEWER marker (the latch releases on the next
    fresh frame and never re-serves the previous one), and the number of distinct
    markers tracks the number of upstream ticks consumed.
    """
    samples: list[tuple[float, int]] = []
    attempts = 0
    while len(samples) < reads and attempts < max_attempts:
        attempts += 1
        upstream.publish_next()
        start = time.monotonic()
        try:
            result = client.read_holding_registers(_MARKER_BLOCK_ADDRESS, count=_MARKER_REG_COUNT, device_id=1)
        except (ModbusException, OSError, ConnectionError):
            continue
        elapsed = time.monotonic() - start
        if result.isError():
            continue
        marker = _decode_marker_int64_le(list(result.registers))
        samples.append((elapsed, marker))
    return samples


def _count_duplicate_markers(markers: list[int]) -> int:
    """Count consecutive-duplicate markers (a read that returned the SAME frame
    as the immediately-preceding successful read)."""
    duplicates = 0
    for prev, cur in zip(markers, markers[1:]):
        if cur == prev:
            duplicates += 1
    return duplicates


def _run_marker_latching_scenario(*, pacing_enabled: bool, pacing_max_wait_s: float):
    """Spin up the full service over the PTY harness and collect marker samples
    from the EM540 serial client while a fresh upstream marker is published before
    every poll.

    Returns ``(samples, update_interval, pacing_max_wait_s)`` where ``samples`` is
    the list of ``(latency_s, marker)`` tuples. Shared by the positive test and
    the latching-disabled negative control so the ONLY difference between them is
    ``pacing_enabled`` — making the control a true A/B of the mechanism.
    """
    update_interval = 0.1

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        em540_tcp_port = find_free_port()
        em540_rtu_port = find_free_port()
        ts65a_tcp_port = find_free_port()

        em540_serial = SerialCable("latch-em540")
        ts65a_serial = SerialCable("latch-ts65a")

        upstream = Em540UpstreamSimulator(frame_seed=100)
        upstream.start()

        try:
            config_path = tmp_path / "config.yaml"
            generate_config(
                config_path,
                upstream.port,
                em540_tcp_port,
                em540_rtu_port,
                ts65a_tcp_port,
                em540_serial_port=em540_serial.left_path,
                ts65a_serial_port=ts65a_serial.left_path,
                update_interval=update_interval,
                serial_pacing_enabled=pacing_enabled,
                serial_pacing_max_wait_s=pacing_max_wait_s,
                serial_timeout=0.5,
            )

            service = ServiceProcess(config_path)
            service.start()

            try:
                wait_for_port(em540_tcp_port, timeout=20.0)
                wait_for_port(ts65a_tcp_port, timeout=20.0)

                clients = DownstreamClients(
                    em540_tcp_port,
                    em540_rtu_port,
                    ts65a_tcp_port,
                    em540_serial.right_path,
                    ts65a_serial.right_path,
                )
                clients.connect_all()

                # Wait until the downstream is serving live data (breaker closed),
                # so the latch — not the stale breaker — governs the serial reads.
                wait_for_downstream_data(clients.em540_tcp, address=0x0000, timeout=120.0)

                # Warm up one marker read so the per-serial-line cursor is
                # initialised and the breaker is confirmed closed for this path.
                wait_for_condition(
                    lambda: _poll_serial_markers(clients.em540_serial, upstream, reads=1, max_attempts=10) != [],
                    timeout=30.0,
                    message="EM540 serial path did not serve a live marker read before sampling"
                    + service.diagnostics(),
                )

                samples = _poll_serial_markers(clients.em540_serial, upstream, reads=12, max_attempts=60)

                service.assert_running()
                clients.disconnect_all()
                return samples, update_interval, pacing_max_wait_s
            finally:
                service.stop()
        finally:
            upstream.stop()
            em540_serial.close()
            ts65a_serial.close()


@pytest.mark.integration
def test_serial_frame_latching_serves_one_fresh_frame_per_upstream_tick() -> None:
    """Serial frame-latching: each successful serial read returns a STRICTLY NEWER
    upstream frame — one response per publication, no duplicates at the client's
    raw poll rate.

    Genuine end-to-end over the PTY serial harness. The upstream stamps a
    monotonic MARKER into the energy block and advances it before every poll; the
    serial read decodes the marker from its response, so the test checks the
    CONTENTS each read observed — not just timing.

    Positive assertions (the Finding-3 proof):
      (1) Each successful read returns a marker strictly GREATER than the previous
          successful read's marker (the latch releases on the next fresh frame and
          never re-serves the one already consumed) — ZERO duplicate markers.
      (2) The number of distinct markers equals the number of successful reads
          (one fresh frame consumed per read) and tracks the upstream ticks.
      (3) Secondary bound: worst-case served read latency < 200ms (the real serial
          client READ TIMEOUT) and within the pacing cap plus a transport margin.
    """
    pacing_max_wait_s = 0.12
    samples, _update_interval, cap = _run_marker_latching_scenario(
        pacing_enabled=True, pacing_max_wait_s=pacing_max_wait_s
    )

    assert len(samples) >= 8, f"too few successful marker reads to assess latching (got {len(samples)})"

    latencies = [s[0] for s in samples]
    markers = [s[1] for s in samples]

    # (1) No duplicate markers: with latching ON a read never re-serves the frame
    # the previous read already consumed.
    duplicates = _count_duplicate_markers(markers)
    assert duplicates == 0, f"latching ON must yield NO duplicate markers, got {duplicates} in {markers}"

    # (1b) Strictly increasing: each read observed a newer upstream frame.
    for prev, cur in zip(markers, markers[1:]):
        assert cur > prev, f"each serial read must return a strictly newer marker; saw {prev} -> {cur} in {markers}"

    # (2) Distinct markers == successful reads: one fresh frame consumed per read.
    assert len(set(markers)) == len(markers), f"every successful read must observe a distinct frame: {markers}"

    # (3) Worst-case latency within the real client read deadline + margin.
    worst = max(latencies)
    assert worst < 0.2, f"serial worst-case read {worst * 1000:.1f}ms exceeded 200ms"
    assert worst < cap + 0.08, (
        f"serial worst-case read {worst * 1000:.1f}ms exceeded pacing cap + margin ({(cap + 0.08) * 1000:.0f}ms)"
    )


@pytest.mark.integration
def test_serial_frame_latching_disabled_yields_duplicate_markers_negative_control() -> None:
    """NEGATIVE CONTROL (a) — end-to-end: with latching DISABLED
    (pacing_enabled=False), a tight-loop serial client OUTRUNS the upstream and
    receives DUPLICATE frames (the same marker more than once). This MUST fail if
    the duplicate detector is broken, proving the positive test's no-duplicate
    assertion is meaningful rather than vacuous.

    Identical to the positive scenario except ``pacing_enabled=False``, so the
    only variable is the latch itself.
    """
    samples, _update_interval, _cap = _run_marker_latching_scenario(pacing_enabled=False, pacing_max_wait_s=0.12)

    assert len(samples) >= 8, f"too few successful marker reads for the negative control (got {len(samples)})"

    markers = [s[1] for s in samples]

    # With latching OFF the client reads faster than the upstream publishes, so at
    # least one read returns the SAME marker as the preceding read. If this does
    # NOT happen the mechanism/harness is not actually exercising the race the
    # positive test guards against.
    duplicates = _count_duplicate_markers(markers)
    assert duplicates > 0, (
        "latching DISABLED must produce at least one duplicate marker (client outrunning upstream); "
        f"saw none in {markers} — the no-duplicate proof would be vacuous"
    )


@pytest.mark.integration
def test_serial_frame_latching_wake_suppressed_stalls_or_duplicates_negative_control() -> None:
    """NEGATIVE CONTROL (b) — proxy boundary: if the gate wake never arrives (the
    listener-thread ``call_soon_threadsafe(gate.publish, ...)`` hop is suppressed),
    a latched read falls to the bounded-wait TIMEOUT and serves a STALE/duplicate
    frame instead of a fresh one. This proves the wake is load-bearing.

    This control is exercised at the proxy boundary (deterministic, no flakiness)
    rather than end-to-end because suppressing the wake end-to-end would require a
    production-only test seam in the hot listener path (AGENTS.md forbids adding
    blocking/behavioural hooks there). The positive no-duplicate proof and control
    (a) remain fully end-to-end over the PTY harness; this control closes the
    remaining "what if the wake is lost" gap without perturbing production code.
    """
    import asyncio

    from app.utils.serial_pacing import FrameGate, SerialPacingContext

    class _Core:
        def __init__(self):
            self.sentinel = [111, 222, 333, 444]

        async def async_getValues(self, device_id, func_code, address, count=1):
            return self.sentinel

        async def async_setValues(self, *a):
            return None

        def device_ids(self):
            return [1]

    class _Helper:
        circuit_open = False

    async def scenario():
        core = _Core()
        gate = FrameGate()
        # A fresh frame exists (gen 1) and the line has already consumed it.
        gate.publish(1, (900, 901, 902, 903))
        await asyncio.sleep(0)
        ctx = SerialPacingContext(core, gate, _Helper(), max_wait_s=0.02, reg_start_address=0)
        ctx._last_served_gen = 1

        first = await asyncio.wait_for(ctx.async_getValues(1, 3, 0, 4), timeout=1.0)

        # The wake for the NEXT frame is suppressed: the register state advances but
        # publish() is never called, so no newer generation is announced. The read
        # must time out and re-serve the SAME (now stale) frame — a duplicate.
        second = await asyncio.wait_for(ctx.async_getValues(1, 3, 0, 4), timeout=1.0)
        return first, second

    first, second = asyncio.run(scenario())

    # Without the wake the second read duplicates the first (stale/duplicate),
    # which is exactly the failure the end-to-end wake is designed to prevent.
    assert first == second == [900, 901, 902, 903], (
        f"suppressed wake must cause a stale/duplicate serve, got first={first} second={second}"
    )
