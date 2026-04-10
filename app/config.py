"""Application configuration dataclasses and constants.

Defines the typed AppState tree and ConfigManager that replaces the old
pyconfigparser-based config.  ConfigManager loads YAML into these
dataclasses and handles validation and persistence.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------


class ConfigError(Exception):
    """Raised when configuration loading or validation fails."""


# ---------------------------------------------------------------------------
# Nested config dataclasses — defaults match config-default.yaml
# ---------------------------------------------------------------------------


@dataclass
class Em540MasterConfig:
    mode: str = "serial"
    baudrate: int = 9600
    parity: str = "N"
    bytesize: int = 8
    stopbits: int = 1
    serial_port: str = "/dev/ttyUSB0"
    host: str = "192.168.102.240"
    port: int = 8899
    slave_id: int = 1
    update_interval: float = 0.1
    timeout: float = 0.08
    retries: int = 0
    log_level: str = "INFO"


@dataclass
class Em540SlaveConfig:
    host: str = "0.0.0.0"
    rtu_port: int = 5002
    tcp_port: int = 5001
    slave_id: int = 1
    update_timeout: float = 0.5
    log_level: str = "INFO"


@dataclass
class Ts65aSlaveConfig:
    host: str = "0.0.0.0"
    port: int = 5003
    slave_id: int = 1
    update_timeout: float = 0.5
    grid_feed_in_hard_limit: float = -5000.0
    smoothing_num_points: int = 20
    log_level: str = "INFO"


@dataclass
class MqttConfig:
    enabled: bool = True
    ha_topic_prefix: str = ""
    host: str = "localhost"
    port: int = 1883
    username: str = ""
    password: str = ""
    update_interval: float = 1.0
    log_level: str = "INFO"


# ---------------------------------------------------------------------------
# Top-level application state
# ---------------------------------------------------------------------------


@dataclass
class AppState:
    em540_master: Em540MasterConfig = field(default_factory=Em540MasterConfig)
    em540_slave: Em540SlaveConfig = field(default_factory=Em540SlaveConfig)
    ts65a_slave: Ts65aSlaveConfig = field(default_factory=Ts65aSlaveConfig)
    mqtt: MqttConfig = field(default_factory=MqttConfig)
    pymodbus_log_level: str = "INFO"
    root_log_level: str = "INFO"


# ---------------------------------------------------------------------------
# Fields that are written back to YAML when changed at runtime
# ---------------------------------------------------------------------------

PERSISTED_FIELDS: set[str] = {
    "ts65a_slave.grid_feed_in_hard_limit",
    "ts65a_slave.smoothing_num_points",
    "mqtt.update_interval",
    "em540_master.update_interval",
    "em540_master.retries",
    "em540_master.timeout",
    "em540_slave.update_timeout",
    "ts65a_slave.update_timeout",
}

# ---------------------------------------------------------------------------
# Required top-level YAML sections — load() raises ConfigError if missing
# ---------------------------------------------------------------------------

REQUIRED_SECTIONS: tuple[str, ...] = (
    "em540_master",
    "em540_slave",
    "ts65a_slave",
    "mqtt",
)


# ---------------------------------------------------------------------------
# ConfigManager
# ---------------------------------------------------------------------------

# Mapping from YAML section name to AppState attribute for special cases
# where the section name doesn't directly match a nested dataclass field.
_SECTION_FIELD_MAP: dict[str, str] = {
    "pymodbus": "pymodbus_log_level",
    "root": "root_log_level",
}


class ConfigManager:
    """Load config from YAML into AppState, with validation and persistence."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._state: AppState | None = None
        self._dirty: bool = False
        self._last_dirty: float = 0.0
        self._stop_event = threading.Event()
        self._flush_thread: threading.Thread | None = None

    def load(self) -> AppState:
        """Read YAML file, validate required sections, return populated AppState."""
        path = Path(self._path)

        # --- read file ---
        try:
            raw = path.read_text()
        except FileNotFoundError:
            raise ConfigError(f"Config file not found: {self._path}")
        except OSError as exc:
            raise ConfigError(f"Cannot read config file: {exc}")

        # --- parse YAML ---
        try:
            data = yaml.safe_load(raw)
        except yaml.YAMLError as exc:
            raise ConfigError(f"Invalid YAML in {self._path}: {exc}")

        if not isinstance(data, dict):
            raise ConfigError(f"Expected a YAML mapping in {self._path}")

        # --- validate required sections ---
        for section in REQUIRED_SECTIONS:
            if section not in data:
                raise ConfigError(f"Missing required config section: {section}")

        # --- populate AppState ---
        state = AppState()

        for section_name, section_data in data.items():
            # Special-case sections that map to a single top-level field
            if section_name in _SECTION_FIELD_MAP:
                if isinstance(section_data, dict):
                    value = section_data.get("log_level", "INFO")
                else:
                    value = "INFO"
                setattr(state, _SECTION_FIELD_MAP[section_name], value)
                continue

            # Regular nested-dataclass sections
            if not isinstance(section_data, dict):
                continue

            sub_config = getattr(state, section_name, None)
            if sub_config is None:
                continue

            for key, value in section_data.items():
                if hasattr(sub_config, key):
                    setattr(sub_config, key, value)

        self._validate(state)
        self._state = state
        return state

    # -- validation helpers --------------------------------------------------

    _VALID_MODES = ("tcp", "serial")
    _VALID_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

    def _validate(self, state: AppState) -> None:
        """Validate populated AppState; raise ConfigError on first failure."""
        # 1. mode
        if state.em540_master.mode not in self._VALID_MODES:
            raise ConfigError(f"em540_master.mode must be one of {self._VALID_MODES}, got '{state.em540_master.mode}'")

        # 2. ports  (0 < port < 65535)
        port_fields = [
            ("em540_master.port", state.em540_master.port),
            ("em540_slave.rtu_port", state.em540_slave.rtu_port),
            ("em540_slave.tcp_port", state.em540_slave.tcp_port),
            ("ts65a_slave.port", state.ts65a_slave.port),
            ("mqtt.port", state.mqtt.port),
        ]
        for name, value in port_fields:
            if not (0 < value < 65535):
                raise ConfigError(f"{name} must satisfy 0 < port < 65535, got {value}")

        # 3. slave_id  (0 < slave_id < 256)
        slave_id_fields = [
            ("em540_master.slave_id", state.em540_master.slave_id),
            ("em540_slave.slave_id", state.em540_slave.slave_id),
            ("ts65a_slave.slave_id", state.ts65a_slave.slave_id),
        ]
        for name, value in slave_id_fields:
            if not (0 < value < 256):
                raise ConfigError(f"{name} must satisfy 0 < slave_id < 256, got {value}")

        # 4. log_level
        log_level_fields = [
            ("em540_master.log_level", state.em540_master.log_level),
            ("em540_slave.log_level", state.em540_slave.log_level),
            ("ts65a_slave.log_level", state.ts65a_slave.log_level),
            ("mqtt.log_level", state.mqtt.log_level),
            ("pymodbus_log_level", state.pymodbus_log_level),
            ("root_log_level", state.root_log_level),
        ]
        for name, value in log_level_fields:
            if value not in self._VALID_LOG_LEVELS:
                raise ConfigError(f"{name} must be one of {self._VALID_LOG_LEVELS}, got '{value}'")

        # 5. grid_feed_in_hard_limit  (<= 0)
        if state.ts65a_slave.grid_feed_in_hard_limit > 0:
            raise ConfigError(
                f"ts65a_slave.grid_feed_in_hard_limit must be <= 0, got {state.ts65a_slave.grid_feed_in_hard_limit}"
            )

        # 6. smoothing_num_points  (1 <= value <= 600)
        if not (1 <= state.ts65a_slave.smoothing_num_points <= 600):
            raise ConfigError(
                f"ts65a_slave.smoothing_num_points must satisfy "
                f"1 <= value <= 600, "
                f"got {state.ts65a_slave.smoothing_num_points}"
            )

    # -- persistence ---------------------------------------------------------

    def schedule_persist(self) -> None:
        """Mark config as dirty for debounced write-back."""
        self._dirty = True
        self._last_dirty = time.monotonic()

    def _flush_loop(self) -> None:
        """Background daemon loop: check dirty every 1s, debounce 5s, then write."""
        while not self._stop_event.is_set():
            self._stop_event.wait(1)
            if not self._dirty:
                continue
            elapsed = time.monotonic() - self._last_dirty
            if elapsed < 5:
                remaining = 5 - elapsed
                self._stop_event.wait(remaining)
            if not self._dirty:
                continue
            self._dirty = False
            try:
                self._write()
            except Exception:
                logger.exception("Failed to write config to %s", self._path)
                self._dirty = True

    def _write(self) -> None:
        """Write current PERSISTED_FIELDS values back to the YAML config file."""
        path = Path(self._path)
        raw = path.read_text()
        data = yaml.safe_load(raw) or {}

        for field_path in PERSISTED_FIELDS:
            section, key = field_path.split(".")
            sub_config = getattr(self._state, section, None)
            if sub_config is not None and hasattr(sub_config, key):
                if section not in data:
                    data[section] = {}
                data[section][key] = getattr(sub_config, key)

        with open(self._path, "w") as f:
            yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)

    def start_flush_loop(self) -> None:
        """Create and start the background daemon flush thread."""
        self._stop_event.clear()
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True, name="config-flush")
        self._flush_thread.start()

    def stop(self) -> None:
        """Signal the flush loop to terminate."""
        self._stop_event.set()
        if self._flush_thread is not None:
            self._flush_thread.join(timeout=10)
            self._flush_thread = None
