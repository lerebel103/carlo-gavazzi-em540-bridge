import asyncio
import logging
import struct
from threading import Event, Thread
from typing import Callable

from pymodbus import FramerType
from pymodbus.server import ModbusTcpServer
from pymodbus.simulator.simdata import DataType, SimData
from pymodbus.simulator.simdevice import SimDevice

from app.carlo_gavazzi import meter_data
from app.carlo_gavazzi.em540_master import MeterDataListener
from app.fronius.ts65a_data import Ts65aMeterData
from app.fronius.ts65a_slave_stats import Ts65aSlaveStats
from app.utils.pdu_helper import PduHelper

logger = logging.getLogger("ts65a-slave")

# Holding register function code used for async_setValues.
_FC_HOLDING_REGISTER = 3

# Pre-compiled struct for FLOAT32 → 2 registers (big-endian)
_STRUCT_FLOAT32 = struct.Struct(">f")
_STRUCT_2H = struct.Struct(">2H")

# Static register layout for the Fronius Smart Meter TS 65A-3 SunSpec model.
# Addresses are 0-based Modbus protocol addresses (register number - 1).
# Each tuple is (address, values_list).
_TS65A_STATIC_REGISTERS: tuple[tuple[int, list[int]], ...] = (
    (768, [0]),
    (1706, [0]),
    (40000, [21365, 28243]),
    (40002, [1]),
    (40003, [65]),
    # Manufacturer "Fronius"
    (40004, [18034, 28526, 26997, 29440, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    # Device Model "Smart Meter TS 65A-3"
    (40020, [21357, 24946, 29728, 19813, 29797, 29216, 21587, 8246, 13633, 11571, 0, 0, 0, 0, 0, 0]),
    (40036, [15472, 29289, 28001, 29305, 15872, 0, 0, 0]),  # Options N/A
    (40044, [12590, 14592, 0, 0, 0, 0, 0, 0]),  # Software Version N/A
    # Serial Number: 000001
    (40052, [48, 48, 48, 48, 48, 48, 48, 49, 0, 0, 0, 0, 0, 0, 0, 1]),
    (40068, [3]),  # Modbus TCP Address
    (40069, [213]),  # Meter Type
    (40070, [124]),  # Modbus Length
    (40071, [0, 0]),  # Ac Current Total
    (40073, [0, 0]),  # Ac Current Phase A
    (40075, [0, 0]),  # Ac Current Phase B
    (40077, [0, 0]),  # Ac Current Phase C
    (40079, [0, 0]),  # Ac voltage average phase to neutral value
    (40081, [0, 0]),  # Ac voltage phase A to neutral value
    (40083, [0, 0]),  # Ac voltage phase B to neutral value
    (40085, [0, 0]),  # Ac voltage phase C to neutral value
    (40087, [0, 0]),  # Ac voltage average phase to phase value
    (40089, [0, 0]),  # Ac voltage phase ab value
    (40091, [0, 0]),  # Ac voltage phase bc value
    (40093, [0, 0]),  # Ac voltage phase ca value
    (40095, [0, 0]),  # Ac Frequency
    (40097, [0, 0]),  # Ac power value
    (40099, [0, 0]),  # Ac power phase A value
    (40101, [0, 0]),  # Ac power phase B value
    (40103, [0, 0]),  # Ac power phase C value
    (40105, [0, 0]),  # Ac apparent power value (VA)
    (40107, [0, 0]),  # Ac apparent power phase A value (VA)
    (40109, [0, 0]),  # Ac apparent power phase B value (VA)
    (40111, [0, 0]),  # Ac apparent power phase C value (VA)
    (40113, [0, 0]),  # Ac reactive power value (VAr)
    (40115, [0, 0]),  # Ac reactive power phase A value (VAr)
    (40117, [0, 0]),  # Ac reactive power phase B value (VAr)
    (40119, [0, 0]),  # Ac reactive power phase C value (VAr)
    (40121, [0, 0]),  # Ac power factor value
    (40123, [0, 0]),  # Ac power factor phase A value
    (40125, [0, 0]),  # Ac power factor phase B value
    (40127, [0, 0]),  # Ac power factor phase C value
    (40129, [0, 0]),  # Total Watt Hours exported (Wh)
    (40131, [0, 0]),  # Total Watt Hours exported phase A (Wh)
    (40133, [0, 0]),  # Total Watt Hours exported phase B (Wh)
    (40135, [0, 0]),  # Total Watt Hours exported phase C (Wh)
    (40137, [0, 0]),  # Total Watt Hours imported (Wh)
    (40139, [0, 0]),  # Total Watt Hours imported phase A (Wh)
    (40141, [0, 0]),  # Total Watt Hours imported phase B (Wh)
    (40143, [0, 0]),  # Total Watt Hours imported phase C (Wh)
    (40145, [0, 0]),  # Total VA Hours exported (VAh)
    (40147, [0, 0]),  # Total VA Hours exported phase A (VAh)
    (40149, [0, 0]),  # Total VA Hours exported phase B (VAh)
    (40151, [0, 0]),  # Total VA Hours exported phase C (VAh)
    (40153, [0, 0]),  # Total VA Hours imported (VAh)
    (40155, [0, 0]),  # Total VA Hours imported phase A (VAh)
    (40157, [0, 0]),  # Total VA Hours imported phase B (VAh)
    (40159, [0, 0]),  # Total VA Hours imported phase C (VAh)
    (
        40161,
        [
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
            32704,
            0,
        ],
    ),
    (40193, [0, 0]),  # Event
    (40195, [65535, 0]),  # End Block
)


def _build_ts65a_simdata(slave_id: int) -> SimDevice:
    """Build a SimDevice for the TS65A register layout.

    The layout is flattened to individual register entries to avoid address
    overlap issues (e.g. the 50-register scale factor block at 40161 spans
    into the Event/End Block addresses at 40193/40195).
    """
    values: dict[int, int] = {}
    for addr, vals in _TS65A_STATIC_REGISTERS:
        for i, v in enumerate(vals):
            values[addr + i] = v

    simdata = [SimData(addr, values=[val], datatype=DataType.UINT16) for addr, val in sorted(values.items())]
    return SimDevice(slave_id, simdata=simdata)


class Ts65aSlaveBridge(MeterDataListener):
    def __init__(self, config):
        self._config = config
        self.host = config.host
        self.port: int = config.port
        self._slave_id: int = config.slave_id
        self._pdu_helper = PduHelper(logger, lambda: self._config.update_timeout)
        self._stats = Ts65aSlaveStats()
        logger.setLevel(config.log_level)

        self.meter_data = Ts65aMeterData(
            config.smoothing_num_points,
            config.grid_feed_in_hard_limit,
            logger,
            self._stats,
        )

        device = _build_ts65a_simdata(self._slave_id)

        self._server = ModbusTcpServer(
            framer=FramerType.SOCKET,
            context=device,
            address=(self.host, self.port),
            trace_pdu=self._pdu_helper.on_pdu,
            trace_connect=self._trace_connect,
        )
        self._dynamic_start_address: int = 40071
        self._dynamic_register_buffer: list[int] = [0] * (len(self._dynamic_values()) * 2)
        self._server_loop: asyncio.AbstractEventLoop | None = None

    def _trace_connect(self, connect):
        logger.debug("Client connection to TCP server: %s", connect)
        if connect:
            self._stats.tcp_client_count += 1
            logger.info("Downstream TS65A client connected (total: %d).", self._stats.tcp_client_count)
        else:
            self._stats.tcp_client_count -= 1
            self._stats.tcp_client_disconnect_count += 1
            logger.info("Downstream TS65A client disconnected (total: %d).", self._stats.tcp_client_count)
        self._stats.changed()

    def add_stats_listener(self, listener: Callable[["Ts65aSlaveStats"], None]):
        self._stats.add_listener(listener)

    async def start(self):
        """Start the downstream TS65A Modbus server on a dedicated event loop.

        Isolates downstream server I/O from the main event loop where upstream reads run.
        """
        self._server_loop = asyncio.new_event_loop()
        ready = Event()
        startup_error: list[BaseException] = []

        async def _run_server():
            try:
                await self._server.serve_forever(background=True)
            except Exception as e:
                startup_error.append(e)
                return
            finally:
                ready.set()

        def _server_thread():
            asyncio.set_event_loop(self._server_loop)
            self._server_loop.run_until_complete(_run_server())
            self._server_loop.run_forever()

        thread = Thread(target=_server_thread, daemon=True, name="ts65a-slave-server")
        thread.start()

        signalled = await asyncio.to_thread(ready.wait, 5.0)
        if not signalled:
            raise TimeoutError("TS65A downstream server failed to start within 5 seconds")
        if startup_error:
            raise startup_error[0]

    def stop(self):
        """Stop the server and clean up the dedicated event loop."""
        if self._server_loop is not None and self._server_loop.is_running():
            self._server_loop.call_soon_threadsafe(self._server_loop.stop)
        self._server_loop = None

    def _sync_pdu_stats(self) -> None:
        stale_age = self._pdu_helper.stale_age_seconds()
        self._stats.stale_data_age_ms = 0.0 if stale_age is None else stale_age * 1000.0
        self._stats.circuit_breaker_open = self._pdu_helper.circuit_open
        self._stats.circuit_breaker_open_count = self._pdu_helper.circuit_open_count
        self._stats.dropped_stale_request_count = self._pdu_helper.dropped_request_count
        self._stats.changed()

    def _dynamic_values(self) -> tuple[float, ...]:
        return (
            self.meter_data.current_an,
            self.meter_data.current_a,
            self.meter_data.current_b,
            self.meter_data.current_c,
            self.meter_data.voltage_ln,
            self.meter_data.voltage_ln_a,
            self.meter_data.voltage_ln_b,
            self.meter_data.voltage_ln_c,
            self.meter_data.voltage_ll,
            self.meter_data.voltage_ll_a,
            self.meter_data.voltage_ll_b,
            self.meter_data.voltage_ll_c,
            self.meter_data.frequency,
            self.meter_data.power,
            self.meter_data.power_a,
            self.meter_data.power_b,
            self.meter_data.power_c,
            self.meter_data.apparent_power,
            self.meter_data.apparent_power_a,
            self.meter_data.apparent_power_b,
            self.meter_data.apparent_power_c,
            self.meter_data.reactive_power,
            self.meter_data.reactive_power_a,
            self.meter_data.reactive_power_b,
            self.meter_data.reactive_power_c,
            self.meter_data.power_factor,
            self.meter_data.power_factor_a,
            self.meter_data.power_factor_b,
            self.meter_data.power_factor_c,
            self.meter_data.wh_neg_total,
            self.meter_data.wh_neg_a,
            self.meter_data.wh_neg_b,
            self.meter_data.wh_neg_c,
            self.meter_data.wh_plus_total,
            self.meter_data.wh_plus_l1,
            self.meter_data.wh_plus_l2,
            self.meter_data.wh_plus_l3,
            self.meter_data.vah_neg_total,
            self.meter_data.vah_neg_a,
            self.meter_data.vah_neg_b,
            self.meter_data.vah_neg_c,
            self.meter_data.vah_plus_total,
            self.meter_data.vah_plus_a,
            self.meter_data.vah_plus_b,
            self.meter_data.vah_plus_c,
        )

    async def new_data(self, data: meter_data.MeterData):
        registers = self._dynamic_register_buffer
        index = 0

        # Run the data through our smoothing and grid feed-in limiter
        self.meter_data.reconfigure(
            self._config.smoothing_num_points,
            self._config.grid_feed_in_hard_limit,
        )
        self.meter_data.update(data)

        # now update the registers in the Modbus datastore
        values = self._dynamic_values()

        for value in values:
            hi, lo = _STRUCT_2H.unpack(_STRUCT_FLOAT32.pack(value))
            registers[index] = hi
            registers[index + 1] = lo
            index += 2

        coro = self._server.async_setValues(
            self._slave_id, _FC_HOLDING_REGISTER, self._dynamic_start_address, registers
        )
        if self._server_loop is not None and self._server_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, self._server_loop)
            await asyncio.wrap_future(future)
        else:
            await coro

        # Notify the PDU helper that we have new data
        self._pdu_helper.data_received(data.timestamp)
        self._sync_pdu_stats()

    async def read_failed(self):
        self._pdu_helper.upstream_failed()
        self._sync_pdu_stats()
