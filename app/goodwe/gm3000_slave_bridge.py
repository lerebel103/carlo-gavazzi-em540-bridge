import logging
from typing import Callable

from pymodbus import FramerType
from pymodbus.datastore import ModbusDeviceContext, ModbusServerContext, ModbusSparseDataBlock
from pymodbus.server import ModbusTcpServer

from app.carlo_gavazzi import meter_data
from app.carlo_gavazzi.em540_master import MeterDataListener
from app.utils.pdu_helper import PduHelper

logger = logging.getLogger("goodwe-gm3000-slave")

GOODWE_CT_RATIO_REGISTER = 40143
GOODWE_CT_RATIO_VALUE = 3000
GOODWE_DYNAMIC_START_REGISTER = 40306
GOODWE_METER_TYPE_REGISTER = 40512
GOODWE_METER_TYPE_VALUE = 2


class GoodweGm3000SlaveStats:
    def __init__(self):
        self.rtu_client_count: int = 0
        self.rtu_client_disconnect_count: int = 0
        self.tcp_client_count: int = 0
        self.tcp_client_disconnect_count: int = 0

        self.circuit_breaker_open: bool = True
        self.circuit_breaker_open_count: int = 0
        self.stale_data_age_ms: float = 0.0
        self.dropped_stale_request_count: int = 0

        self._listeners: list[Callable[["GoodweGm3000SlaveStats"], None]] = []

    def changed(self):
        for listener in self._listeners:
            listener(self)

    def add_listener(self, listener: Callable[["GoodweGm3000SlaveStats"], None]):
        self._listeners.append(listener)


class GoodweGm3000SlaveBridge(MeterDataListener):
    """Bridge EM540 upstream meter data into a Goodwe GM3000-compatible map."""

    def __init__(self, config):
        self._config = config
        self.host: str = config.host
        self.socket_port: int = config.socket_port
        self.rtu_port: int = config.rtu_port
        self._slave_id: int = config.slave_id
        self._pdu_helper = PduHelper(logger, lambda: self._config.update_timeout)
        self._stats = GoodweGm3000SlaveStats()
        logger.setLevel(config.log_level)

        # Addresses follow the 40xxx holding-register map observed for GM3000.
        datablock = ModbusSparseDataBlock(
            {
                40142: [0],
                GOODWE_CT_RATIO_REGISTER: [GOODWE_CT_RATIO_VALUE],
                GOODWE_METER_TYPE_REGISTER: [GOODWE_METER_TYPE_VALUE],
                GOODWE_DYNAMIC_START_REGISTER: [0] * 17,
            }
        )
        self.datablock = datablock

        context = ModbusDeviceContext(
            di=datablock,
            co=datablock,
            hr=datablock,
            ir=datablock,
        )
        self.context = ModbusServerContext({self._slave_id: context}, single=False)

        self._rtu_server = ModbusTcpServer(
            framer=FramerType.RTU,
            context=self.context,
            address=(self.host, self.rtu_port),
            trace_pdu=self._pdu_helper.on_pdu,
            trace_connect=self._trace_rtu_connect,
        )
        self._tcp_server = ModbusTcpServer(
            framer=FramerType.SOCKET,
            context=self.context,
            address=(self.host, self.socket_port),
            trace_pdu=self._pdu_helper.on_pdu,
            trace_connect=self._trace_tcp_connect,
        )

    def _trace_rtu_connect(self, connect: bool):
        logger.info(f"Client connection to RTU server: {connect}")
        if connect:
            self._stats.rtu_client_count += 1
        else:
            self._stats.rtu_client_count -= 1
            self._stats.rtu_client_disconnect_count += 1
        self._stats.changed()

    def _trace_tcp_connect(self, connect: bool):
        logger.info(f"Client connection to TCP server: {connect}")
        if connect:
            self._stats.tcp_client_count += 1
        else:
            self._stats.tcp_client_count -= 1
            self._stats.tcp_client_disconnect_count += 1
        self._stats.changed()

    def add_stats_listener(self, listener: Callable[[GoodweGm3000SlaveStats], None]):
        self._stats.add_listener(listener)

    async def start(self):
        await self._rtu_server.serve_forever(background=True)
        await self._tcp_server.serve_forever(background=True)

    def stop(self):
        pass

    def _sync_pdu_stats(self) -> None:
        stale_age = self._pdu_helper.stale_age_seconds()
        self._stats.stale_data_age_ms = 0.0 if stale_age is None else stale_age * 1000.0
        self._stats.circuit_breaker_open = self._pdu_helper.circuit_open
        self._stats.circuit_breaker_open_count = self._pdu_helper.circuit_open_count
        self._stats.dropped_stale_request_count = self._pdu_helper.dropped_request_count
        self._stats.changed()

    @staticmethod
    def _to_u16_scaled(value: float, scale: float) -> int:
        scaled = int(round(value * scale))
        return 0 if scaled < 0 else 65535 if scaled > 65535 else scaled

    @staticmethod
    def _to_i16(value: float) -> int:
        i16 = int(round(value))
        return -32768 if i16 < -32768 else 32767 if i16 > 32767 else i16

    @staticmethod
    def _sign_extended_words(value: int) -> tuple[int, int]:
        low = value & 0xFFFF
        high = 0xFFFF if value < 0 else 0x0000
        return high, low

    async def new_data(self, data: meter_data.MeterData):
        ia = self._to_u16_scaled(data.phases[0].current, 100.0)
        ib = self._to_u16_scaled(data.phases[1].current, 100.0)
        ic = self._to_u16_scaled(data.phases[2].current, 100.0)

        # EM540 reports export/feed-in as negative power; the Goodwe map expects
        # export as positive and import as negative in the low-word register.
        pa = self._to_i16(-data.phases[0].power)
        pb = self._to_i16(-data.phases[1].power)
        pc = self._to_i16(-data.phases[2].power)
        total_power = self._to_i16(pa + pb + pc)

        dynamic_block = [
            self._to_u16_scaled(data.phases[0].line_neutral_voltage, 10.0),
            self._to_u16_scaled(data.phases[1].line_neutral_voltage, 10.0),
            self._to_u16_scaled(data.phases[2].line_neutral_voltage, 10.0),
            0x0000,
            ia,
            0x0000,
            ib,
            0x0000,
            ic,
            *self._sign_extended_words(pa),
            *self._sign_extended_words(pb),
            *self._sign_extended_words(pc),
            *self._sign_extended_words(total_power),
        ]

        self.datablock.setValues(GOODWE_DYNAMIC_START_REGISTER, dynamic_block)

        self._pdu_helper.data_received(data.timestamp)
        self._sync_pdu_stats()

    async def read_failed(self) -> None:
        self._pdu_helper.upstream_failed()
        self._sync_pdu_stats()
