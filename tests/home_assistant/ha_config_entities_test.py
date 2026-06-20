"""Property-based tests for HAConfigEntities MQTT discovery payloads."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from app.config import PERSISTED_FIELDS, AppState, ConfigManager
from app.home_assistant.ha_config_entities import HAConfigEntities

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_entities(state: AppState | None = None) -> HAConfigEntities:
    """Build HAConfigEntities with mocked mqtt_client and config_manager."""
    if state is None:
        state = AppState()
    mqtt_client = MagicMock()
    config_manager = MagicMock(spec=ConfigManager)
    return HAConfigEntities(state, mqtt_client, config_manager)


def test_topic_prefix_is_applied_to_config_entity_topics_and_ids():
    state = AppState()
    mqtt_client = MagicMock()
    config_manager = MagicMock(spec=ConfigManager)
    entities = HAConfigEntities(state, mqtt_client, config_manager, topic_prefix="qa/test-stack")

    discovery_topic, raw_payload = entities.advertise()[0]
    payload = json.loads(raw_payload)

    assert discovery_topic.startswith("homeassistant/")
    assert payload["command_topic"].startswith("qa/test-stack/lerebel/")
    assert payload["state_topic"].startswith("qa/test-stack/lerebel/")
    assert payload["name"] == "Grid Feed-In Hard Limit"
    assert payload["unique_id"].startswith("em540_bridge_qa_test_stack_config_")
    assert payload["device"]["identifiers"] == ["em540_bridge_qa_test_stack"]
    assert payload["device"]["model"] == "[qa/test-stack] EM540 Bridge"


# Required keys that every discovery payload must contain (Req 9.2, 9.3).
_REQUIRED_KEYS = {"name", "unique_id", "command_topic", "state_topic", "device", "entity_category"}

# Additional keys required for number entities (Req 9.4).
_NUMBER_KEYS = {"min", "max", "step", "mode"}


# ---------------------------------------------------------------------------
# Property 7 — MQTT discovery payload validity
# ---------------------------------------------------------------------------

# Strategy: generate valid values for each PERSISTED_FIELD and build an AppState.
_grid_limit = st.floats(min_value=-50000, max_value=0, allow_nan=False, allow_infinity=False)
_smoothing = st.integers(min_value=1, max_value=600)
_mqtt_interval = st.floats(min_value=0.1, max_value=60, allow_nan=False, allow_infinity=False)
_master_interval = st.floats(min_value=0.05, max_value=10, allow_nan=False, allow_infinity=False)
_master_retries = st.integers(min_value=0, max_value=9)
_master_timeout = st.floats(min_value=0.05, max_value=10, allow_nan=False, allow_infinity=False)
_slave_timeout = st.floats(min_value=0.1, max_value=10, allow_nan=False, allow_infinity=False)
_ts65a_timeout = st.floats(min_value=0.1, max_value=10, allow_nan=False, allow_infinity=False)


@st.composite
def app_states(draw):
    """Generate an AppState with random valid values for all persisted fields."""
    state = AppState()
    state.ts65a_slave.grid_feed_in_hard_limit = draw(_grid_limit)
    state.ts65a_slave.smoothing_num_points = draw(_smoothing)
    state.mqtt.update_interval = draw(_mqtt_interval)
    state.em540_master.update_interval = draw(_master_interval)
    state.em540_master.retries = draw(_master_retries)
    state.em540_master.timeout = draw(_master_timeout)
    state.em540_slave.update_timeout = draw(_slave_timeout)
    state.ts65a_slave.update_timeout = draw(_ts65a_timeout)
    return state


@given(state=app_states())
@settings(max_examples=200, deadline=None)
def test_property_mqtt_discovery_payload_validity(state: AppState):
    """**Validates: Requirements 9.1, 9.2, 9.3, 9.4**

    Property 7 — MQTT discovery payload validity:
    For any PERSISTED_FIELDS entry, the generated MQTT_Discovery_Payload
    SHALL be valid JSON containing at minimum: name, unique_id,
    command_topic, state_topic, device, entity_category ("config").
    For number entities: min, max, and step.
    For switch entities: payload_on and payload_off.
    """
    entities = _make_entities(state)
    payloads = entities.advertise()

    # There must be exactly one payload per PERSISTED_FIELD.
    assert len(payloads) == len(PERSISTED_FIELDS), f"Expected {len(PERSISTED_FIELDS)} payloads, got {len(payloads)}"

    for topic, raw_payload in payloads:
        # Payload must be valid JSON.
        payload = json.loads(raw_payload)

        # Must contain all required keys (Req 9.2).
        missing = _REQUIRED_KEYS - payload.keys()
        assert not missing, f"Payload for {topic} missing keys: {missing}"

        # entity_category must be "config" (Req 9.3).
        assert payload["entity_category"] == "config", (
            f"entity_category should be 'config', got {payload['entity_category']!r}"
        )

        # For number entities, min/max/step must be present (Req 9.4).
        # For switch entities, payload_on/payload_off must be present.
        is_switch = "payload_on" in payload
        if not is_switch:
            missing_number = _NUMBER_KEYS - payload.keys()
            assert not missing_number, f"Number payload for {topic} missing keys: {missing_number}"
            assert payload["mode"] == "box"
        else:
            # Switches must have payload_on and payload_off
            assert "payload_on" in payload and "payload_off" in payload, (
                f"Switch payload for {topic} missing payload_on/payload_off"
            )

        # device must be a dict with at least an identifiers key.
        assert isinstance(payload["device"], dict), "device must be a dict"
        assert "identifiers" in payload["device"], "device must have identifiers"

        # name and unique_id must be non-empty strings.
        assert isinstance(payload["name"], str) and payload["name"], (
            f"name must be a non-empty string, got {payload['name']!r}"
        )
        assert isinstance(payload["unique_id"], str) and payload["unique_id"], (
            f"unique_id must be a non-empty string, got {payload['unique_id']!r}"
        )

        # command_topic and state_topic must be non-empty strings.
        assert isinstance(payload["command_topic"], str) and payload["command_topic"]
        assert isinstance(payload["state_topic"], str) and payload["state_topic"]


# ---------------------------------------------------------------------------
# Property 8 — Valid command updates state and triggers persist
# ---------------------------------------------------------------------------

# Strategy: pick a random entity index and a valid value within its range.
_ENTITY_COUNT = 7  # number of entities built by HAConfigEntities

_entity_index = st.integers(min_value=0, max_value=_ENTITY_COUNT - 1)


@st.composite
def entity_and_value(draw):
    """Pick a random entity index and generate a valid value within its range."""
    state = AppState()
    mqtt_client = MagicMock()
    config_manager = MagicMock(spec=ConfigManager)
    entities_obj = HAConfigEntities(state, mqtt_client, config_manager)

    idx = draw(_entity_index)
    entity = entities_obj._entities[idx]

    # Generate a value within [min_value, max_value] respecting the type
    if entity.parse_value is int:
        value = draw(
            st.integers(
                min_value=int(entity.min_value),
                max_value=int(entity.max_value),
            )
        )
    else:
        value = draw(
            st.floats(
                min_value=entity.min_value,
                max_value=entity.max_value,
                allow_nan=False,
                allow_infinity=False,
            )
        )

    return idx, value


@given(data=entity_and_value())
@settings(max_examples=200, deadline=None)
def test_property_valid_command_updates_state_and_triggers_persist(data):
    """**Validates: Requirements 10.1, 10.2, 10.3**

    Property 8 — Valid command updates state and triggers persist:
    For any config entity and for any valid value within its defined range,
    receiving that value on the entity's command topic SHALL update the
    corresponding AppState field, call schedule_persist(), and publish the
    new value to the entity's state topic with retain.
    """
    idx, value = data

    # Fresh AppState + mocks for each example
    state = AppState()
    mqtt_client = MagicMock()
    config_manager = MagicMock(spec=ConfigManager)
    entities_obj = HAConfigEntities(state, mqtt_client, config_manager)

    entity = entities_obj._entities[idx]

    # Build a fake MQTT message
    command_topic = f"lerebel/config/em540_bridge/{entity.safe_name}/set"
    message = MagicMock()
    message.topic = command_topic
    message.payload = str(value).encode()

    # Invoke the command handler
    entities_obj._on_command(None, None, message)

    # 1) AppState field was updated (Req 10.1)
    actual = getattr(entity.config_section, entity.field_name)
    expected = entity.parse_value(str(value))
    if isinstance(expected, float):
        assert abs(actual - expected) < 1e-9, f"Expected {entity.field_path} = {expected}, got {actual}"
    else:
        assert actual == expected, f"Expected {entity.field_path} = {expected}, got {actual}"

    # 2) schedule_persist() was called (Req 10.2)
    config_manager.schedule_persist.assert_called_once()

    # 3) Published new value to state topic with retain=True (Req 10.3)
    state_topic = f"lerebel/config/em540_bridge/{entity.safe_name}/state"
    mqtt_client.publish.assert_called_once_with(state_topic, str(value), retain=True)


# ---------------------------------------------------------------------------
# Property 9 — Invalid command leaves state unchanged
# ---------------------------------------------------------------------------

# Strategy: generate invalid string values that cannot be parsed as int or float.
_invalid_values = st.sampled_from(
    [
        "abc",
        "not_a_number",
        "",
        "  ",
        "NaN",
        "inf",
        "-inf",
        "12.34.56",
        "1,000",
        "true",
        "null",
        "None",
        "0x1F",
    ]
) | st.text(
    alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz!@#$%^&*()_"),
    min_size=1,
    max_size=10,
)


@st.composite
def entity_index_and_invalid_value(draw):
    """Pick a random entity index and generate an invalid (non-numeric) value."""
    idx = draw(_entity_index)
    value = draw(_invalid_values)

    # Ensure the value truly cannot be parsed by the entity's parse_value.
    # Build a temporary entities object to check.
    tmp = _make_entities()
    entity = tmp._entities[idx]
    try:
        entity.parse_value(value)
        # If parsing succeeds, this is actually a valid value — skip it.
        from hypothesis import assume

        assume(False)
    except (ValueError, TypeError):
        pass

    return idx, value


@given(data=entity_index_and_invalid_value())
@settings(max_examples=200, deadline=None)
def test_property_invalid_command_leaves_state_unchanged(data):
    """**Validates: Requirement 10.4**

    Property 9 — Invalid command leaves state unchanged:
    For any config entity and for any value that is outside its valid range
    or of the wrong type, receiving that value on the entity's command topic
    SHALL leave the AppState unchanged.
    """
    idx, invalid_value = data

    # Fresh AppState + mocks
    state = AppState()
    mqtt_client = MagicMock()
    config_manager = MagicMock(spec=ConfigManager)
    entities_obj = HAConfigEntities(state, mqtt_client, config_manager)

    entity = entities_obj._entities[idx]

    # Record original value before the command
    original_value = getattr(entity.config_section, entity.field_name)

    # Build a fake MQTT message with the invalid value
    command_topic = f"lerebel/config/em540_bridge/{entity.safe_name}/set"
    message = MagicMock()
    message.topic = command_topic
    message.payload = invalid_value.encode()

    # Invoke the command handler
    entities_obj._on_command(None, None, message)

    # 1) AppState field is unchanged (Req 10.4)
    actual = getattr(entity.config_section, entity.field_name)
    assert actual == original_value, f"Expected {entity.field_path} to remain {original_value}, got {actual}"

    # 2) schedule_persist() was NOT called
    config_manager.schedule_persist.assert_not_called()

    # 3) No state was published
    mqtt_client.publish.assert_not_called()
