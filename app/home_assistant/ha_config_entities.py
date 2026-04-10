"""Home Assistant config entities exposed via MQTT discovery.

Each PERSISTED_FIELDS entry becomes an editable HA number entity so that
settings can be changed at runtime from the Home Assistant UI.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable

from app.config import AppState, ConfigManager
from app.version import __version__

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Device info — matches the pattern used in ha_sensors.py
# ---------------------------------------------------------------------------

DEVICE_INFO: dict[str, Any] = {
    "name": "EM540 Energy Meter Bridge",
    "identifiers": ["em540_bridge"],
    "manufacturer": "LeRebel",
    "model": "EM540 Bridge",
    "sw_version": __version__,
}


# ---------------------------------------------------------------------------
# ConfigEntity dataclass
# ---------------------------------------------------------------------------


@dataclass
class ConfigEntity:
    """Describes a single HA config entity backed by an AppState field."""

    name: str
    field_path: str  # e.g. "mqtt.update_interval"
    config_section: Any  # reference to the dataclass instance (e.g. state.mqtt)
    field_name: str  # attribute name on config_section
    min_value: float | None = None
    max_value: float | None = None
    step: float | None = None
    parse_value: Callable[[str], Any] | None = None
    unit: str | None = None
    entity_type: str = "number"  # "number" or "switch"

    @property
    def safe_name(self) -> str:
        return self.field_path.replace(".", "_")


# ---------------------------------------------------------------------------
# HAConfigEntities
# ---------------------------------------------------------------------------


class HAConfigEntities:
    """Expose PERSISTED_FIELDS as editable HA entities via MQTT discovery."""

    def __init__(
        self,
        state: AppState,
        mqtt_client: Any,
        config_manager: ConfigManager,
    ) -> None:
        self._state = state
        self._mqtt_client = mqtt_client
        self._config_manager = config_manager

        self._entities: list[ConfigEntity] = self._build_entities()

        # Lookup from command topic → entity for fast dispatch
        self._topic_to_entity: dict[str, ConfigEntity] = {
            f"lerebel/config/em540_bridge/{e.safe_name}/set": e for e in self._entities
        }

    # -- entity definitions --------------------------------------------------

    def _build_entities(self) -> list[ConfigEntity]:
        """Build the list of ConfigEntity objects from PERSISTED_FIELDS."""
        defs: list[ConfigEntity] = [
            ConfigEntity(
                name="Grid Feed-In Hard Limit",
                field_path="ts65a_slave.grid_feed_in_hard_limit",
                config_section=self._state.ts65a_slave,
                field_name="grid_feed_in_hard_limit",
                min_value=-50000,
                max_value=0,
                step=100,
                parse_value=float,
                unit="W",
            ),
            ConfigEntity(
                name="Smoothing Num Points",
                field_path="ts65a_slave.smoothing_num_points",
                config_section=self._state.ts65a_slave,
                field_name="smoothing_num_points",
                min_value=1,
                max_value=600,
                step=1,
                parse_value=int,
            ),
            ConfigEntity(
                name="MQTT Update Interval",
                field_path="mqtt.update_interval",
                config_section=self._state.mqtt,
                field_name="update_interval",
                min_value=0.1,
                max_value=60,
                step=0.1,
                unit="s",
                parse_value=float,
            ),
            ConfigEntity(
                name="EM540 Master Update Interval",
                field_path="em540_master.update_interval",
                config_section=self._state.em540_master,
                field_name="update_interval",
                min_value=0.05,
                max_value=10,
                step=0.05,
                unit="s",
                parse_value=float,
            ),
            ConfigEntity(
                name="EM540 Master Retries",
                field_path="em540_master.retries",
                config_section=self._state.em540_master,
                field_name="retries",
                min_value=0,
                max_value=9,
                step=1,
                parse_value=int,
            ),
            ConfigEntity(
                name="EM540 Master Timeout",
                field_path="em540_master.timeout",
                config_section=self._state.em540_master,
                field_name="timeout",
                min_value=0.05,
                max_value=10,
                step=0.05,
                unit="s",
                parse_value=float,
            ),
            ConfigEntity(
                name="EM540 Slave Update Timeout",
                field_path="em540_slave.update_timeout",
                config_section=self._state.em540_slave,
                field_name="update_timeout",
                min_value=0.1,
                max_value=10,
                step=0.1,
                unit="s",
                parse_value=float,
            ),
            ConfigEntity(
                name="TS65A Slave Update Timeout",
                field_path="ts65a_slave.update_timeout",
                config_section=self._state.ts65a_slave,
                field_name="update_timeout",
                min_value=0.1,
                max_value=10,
                step=0.1,
                unit="s",
                parse_value=float,
            ),
            ConfigEntity(
                name="Home Assistant Sensor Publishing",
                field_path="mqtt.enable_ha_publish",
                config_section=self._state.mqtt,
                field_name="enable_ha_publish",
                parse_value=lambda x: x.lower() in ("on", "true", "1"),
                entity_type="switch",
            ),
        ]
        return defs

    # -- MQTT discovery ------------------------------------------------------

    def advertise(self) -> list[tuple[str, str]]:
        """Generate MQTT discovery payloads for all config entities."""
        payloads: list[tuple[str, str]] = []
        for entity in self._entities:
            if entity.entity_type == "switch":
                topic = f"homeassistant/switch/em540_bridge_{entity.safe_name}/config"
                payload_obj: dict[str, Any] = {
                    "name": entity.name,
                    "unique_id": f"em540_bridge_config_{entity.safe_name}",
                    "command_topic": f"lerebel/config/em540_bridge/{entity.safe_name}/set",
                    "state_topic": f"lerebel/config/em540_bridge/{entity.safe_name}/state",
                    "payload_on": "on",
                    "payload_off": "off",
                    "device": DEVICE_INFO,
                    "entity_category": "config",
                }
            else:  # number
                topic = f"homeassistant/number/em540_bridge_{entity.safe_name}/config"
                payload_obj: dict[str, Any] = {
                    "name": entity.name,
                    "unique_id": f"em540_bridge_config_{entity.safe_name}",
                    "command_topic": f"lerebel/config/em540_bridge/{entity.safe_name}/set",
                    "state_topic": f"lerebel/config/em540_bridge/{entity.safe_name}/state",
                    "min": entity.min_value,
                    "max": entity.max_value,
                    "step": entity.step,
                    "device": DEVICE_INFO,
                    "entity_category": "config",
                }
                if entity.unit is not None:
                    payload_obj["unit_of_measurement"] = entity.unit
            payloads.append((topic, json.dumps(payload_obj)))
        return payloads

    # -- MQTT subscriptions --------------------------------------------------

    def subscribe(self) -> None:
        """Subscribe to command topics for each config entity."""
        for topic in self._topic_to_entity:
            self._mqtt_client.subscribe(topic)
            self._mqtt_client.message_callback_add(topic, self._on_command)

    # -- command handling ----------------------------------------------------

    def _on_command(self, client: Any, userdata: Any, message: Any) -> None:
        """Handle incoming MQTT command to update a config value."""
        entity = self._topic_to_entity.get(message.topic)
        if entity is None:
            return

        try:
            value = entity.parse_value(message.payload.decode())
        except (ValueError, TypeError):
            logger.warning("Invalid value for %s: %s", entity.name, message.payload)
            return

        # Update the AppState field
        setattr(entity.config_section, entity.field_name, value)

        # Schedule persistence
        self._config_manager.schedule_persist()

        # Publish updated state (echo back for HA UI to update)
        state_topic = f"lerebel/config/em540_bridge/{entity.safe_name}/state"
        if entity.entity_type == "switch":
            # Switches use on/off payloads
            state_value = "on" if value else "off"
        else:
            # Numbers use string representation
            state_value = str(value)
        self._mqtt_client.publish(state_topic, state_value, retain=True)
