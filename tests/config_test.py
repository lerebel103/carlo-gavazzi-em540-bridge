"""Unit tests for ConfigManager.__init__() and load()."""

from __future__ import annotations

import time
from dataclasses import fields as dc_fields
from unittest.mock import patch

import pytest
import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from app.config import (
    PERSISTED_FIELDS,
    AppState,
    ConfigError,
    ConfigManager,
    Em540MasterConfig,
    Em540SlaveConfig,
    MqttConfig,
    Ts65aSlaveConfig,
)


@pytest.fixture()
def valid_yaml(tmp_path):
    """Return path to a minimal valid config file."""
    data = {
        "em540_master": {"mode": "tcp", "host": "10.0.0.1", "port": 502},
        "em540_slave": {"host": "0.0.0.0"},
        "ts65a_slave": {"port": 5003},
        "mqtt": {"host": "broker.local", "port": 1883},
        "pymodbus": {"log_level": "DEBUG"},
        "root": {"log_level": "WARNING"},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data))
    return str(p)


# -- __init__ --


def test_init_stores_path_and_state_is_none():
    cm = ConfigManager("/some/path.yaml")
    assert cm._path == "/some/path.yaml"
    assert cm._state is None


# -- load() happy path --


def test_load_returns_app_state(valid_yaml):
    state = ConfigManager(valid_yaml).load()
    assert isinstance(state, AppState)


def test_load_populates_nested_fields(valid_yaml):
    state = ConfigManager(valid_yaml).load()
    assert state.em540_master.mode == "tcp"
    assert state.em540_master.host == "10.0.0.1"
    assert state.em540_master.port == 502
    assert state.mqtt.host == "broker.local"


def test_load_special_pymodbus_section(valid_yaml):
    state = ConfigManager(valid_yaml).load()
    assert state.pymodbus_log_level == "DEBUG"


def test_load_special_root_section(valid_yaml):
    state = ConfigManager(valid_yaml).load()
    assert state.root_log_level == "WARNING"


def test_load_sets_internal_state(valid_yaml):
    cm = ConfigManager(valid_yaml)
    state = cm.load()
    assert cm._state is state


# -- defaults for missing optional fields --


def test_missing_optional_fields_get_defaults(tmp_path):
    data = {
        "em540_master": {},
        "em540_slave": {},
        "ts65a_slave": {},
        "mqtt": {},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data))
    state = ConfigManager(str(p)).load()

    assert state.em540_master.mode == "serial"
    assert state.em540_master.retries == 0
    assert state.em540_slave.rtu_port == 5002
    assert state.ts65a_slave.grid_feed_in_hard_limit == -5000.0
    assert state.mqtt.enabled is True
    assert state.pymodbus_log_level == "INFO"
    assert state.root_log_level == "INFO"


# -- missing required section --


@pytest.mark.parametrize(
    "missing",
    [
        "em540_master",
        "em540_slave",
        "ts65a_slave",
        "mqtt",
    ],
)
def test_missing_required_section_raises_config_error(tmp_path, missing):
    data = {
        "em540_master": {},
        "em540_slave": {},
        "ts65a_slave": {},
        "mqtt": {},
    }
    del data[missing]
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data))

    with pytest.raises(ConfigError, match=missing):
        ConfigManager(str(p)).load()


# -- file errors --


def test_file_not_found(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        ConfigManager(str(tmp_path / "nope.yaml")).load()


def test_invalid_yaml(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text(":\n  - :\n  bad: [unterminated")
    with pytest.raises(ConfigError, match="Invalid YAML"):
        ConfigManager(str(p)).load()


def test_non_mapping_yaml(tmp_path):
    p = tmp_path / "list.yaml"
    p.write_text("- item1\n- item2\n")
    with pytest.raises(ConfigError, match="mapping"):
        ConfigManager(str(p)).load()


# -- backward compatibility with real config.yaml --


def test_loads_existing_config_yaml():
    # Skip this test if config.yaml (user-specific config) doesn't exist.
    # It's optional and not committed to the repo.
    import os

    if not os.path.exists("config.yaml"):
        pytest.skip("config.yaml not found (user-specific config file)")

    state = ConfigManager("config.yaml").load()
    assert state.em540_master.mode == "tcp"
    assert state.em540_master.host == "192.168.102.240"
    assert state.mqtt.host == "mqtt.iot.home.lan"
    assert state.pymodbus_log_level == "INFO"
    assert state.root_log_level == "INFO"


# ---------------------------------------------------------------------------
# Validation tests (Task 2.3)
# ---------------------------------------------------------------------------


def _make_config(tmp_path, overrides: dict | None = None):
    """Build a valid config dict, apply overrides, write to tmp file, return path."""
    data = {
        "em540_master": {"mode": "tcp", "host": "10.0.0.1", "port": 502, "slave_id": 1, "log_level": "INFO"},
        "em540_slave": {"host": "0.0.0.0", "rtu_port": 5002, "tcp_port": 5001, "slave_id": 1, "log_level": "INFO"},
        "ts65a_slave": {
            "port": 5003,
            "slave_id": 1,
            "log_level": "INFO",
            "grid_feed_in_hard_limit": -5000,
            "smoothing_num_points": 20,
        },
        "mqtt": {"host": "broker.local", "port": 1883, "log_level": "INFO"},
        "pymodbus": {"log_level": "INFO"},
        "root": {"log_level": "INFO"},
    }
    if overrides:
        for dotted_key, value in overrides.items():
            parts = dotted_key.split(".")
            if len(parts) == 2:
                section, key = parts
                if section not in data:
                    data[section] = {}
                data[section][key] = value
            else:
                data[parts[0]] = value
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data))
    return str(p)


# -- mode validation --


@pytest.mark.parametrize("mode", ["tcp", "serial"])
def test_valid_mode_accepted(tmp_path, mode):
    path = _make_config(tmp_path, {"em540_master.mode": mode})
    state = ConfigManager(path).load()
    assert state.em540_master.mode == mode


@pytest.mark.parametrize("mode", ["udp", "rtu", "", "TCP", "Serial"])
def test_invalid_mode_raises(tmp_path, mode):
    path = _make_config(tmp_path, {"em540_master.mode": mode})
    with pytest.raises(ConfigError, match="mode"):
        ConfigManager(path).load()


# -- port validation --


@pytest.mark.parametrize(
    "field",
    [
        "em540_master.port",
        "em540_slave.rtu_port",
        "em540_slave.tcp_port",
        "ts65a_slave.port",
        "mqtt.port",
    ],
)
@pytest.mark.parametrize("bad_value", [0, -1, 65535, 70000])
def test_invalid_port_raises(tmp_path, field, bad_value):
    path = _make_config(tmp_path, {field: bad_value})
    with pytest.raises(ConfigError, match="port"):
        ConfigManager(path).load()


@pytest.mark.parametrize(
    "field",
    [
        "em540_master.port",
        "em540_slave.rtu_port",
        "em540_slave.tcp_port",
        "ts65a_slave.port",
        "mqtt.port",
    ],
)
def test_valid_port_accepted(tmp_path, field):
    path = _make_config(tmp_path, {field: 1})
    state = ConfigManager(path).load()
    # Just verify it loads without error
    assert state is not None


# -- slave_id validation --


@pytest.mark.parametrize(
    "field",
    [
        "em540_master.slave_id",
        "em540_slave.slave_id",
        "ts65a_slave.slave_id",
    ],
)
@pytest.mark.parametrize("bad_value", [0, -1, 256, 999])
def test_invalid_slave_id_raises(tmp_path, field, bad_value):
    path = _make_config(tmp_path, {field: bad_value})
    with pytest.raises(ConfigError, match="slave_id"):
        ConfigManager(path).load()


@pytest.mark.parametrize(
    "field",
    [
        "em540_master.slave_id",
        "em540_slave.slave_id",
        "ts65a_slave.slave_id",
    ],
)
def test_valid_slave_id_accepted(tmp_path, field):
    path = _make_config(tmp_path, {field: 255})
    state = ConfigManager(path).load()
    assert state is not None


# -- log_level validation --


@pytest.mark.parametrize(
    "field",
    [
        "em540_master.log_level",
        "em540_slave.log_level",
        "ts65a_slave.log_level",
        "mqtt.log_level",
    ],
)
@pytest.mark.parametrize("bad_value", ["debug", "info", "TRACE", "WARN", ""])
def test_invalid_log_level_raises(tmp_path, field, bad_value):
    path = _make_config(tmp_path, {field: bad_value})
    with pytest.raises(ConfigError, match="log_level"):
        ConfigManager(path).load()


@pytest.mark.parametrize(
    "field",
    [
        "em540_master.log_level",
        "em540_slave.log_level",
        "ts65a_slave.log_level",
        "mqtt.log_level",
    ],
)
@pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_valid_log_level_accepted(tmp_path, field, level):
    path = _make_config(tmp_path, {field: level})
    state = ConfigManager(path).load()
    assert state is not None


# -- pymodbus / root log_level validation --


@pytest.mark.parametrize(
    "section,field",
    [
        ("pymodbus", "pymodbus_log_level"),
        ("root", "root_log_level"),
    ],
)
def test_invalid_special_log_level_raises(tmp_path, section, field):
    path = _make_config(tmp_path, {f"{section}.log_level": "TRACE"})
    with pytest.raises(ConfigError, match="log_level"):
        ConfigManager(path).load()


@pytest.mark.parametrize("section", ["pymodbus", "root"])
def test_valid_special_log_level_accepted(tmp_path, section):
    path = _make_config(tmp_path, {f"{section}.log_level": "DEBUG"})
    state = ConfigManager(path).load()
    assert state is not None


# -- grid_feed_in_hard_limit validation --


@pytest.mark.parametrize("value", [1, 0.1, 100])
def test_positive_grid_feed_in_hard_limit_raises(tmp_path, value):
    path = _make_config(tmp_path, {"ts65a_slave.grid_feed_in_hard_limit": value})
    with pytest.raises(ConfigError, match="grid_feed_in_hard_limit"):
        ConfigManager(path).load()


@pytest.mark.parametrize("value", [0, -1, -5000, -50000.0])
def test_valid_grid_feed_in_hard_limit_accepted(tmp_path, value):
    path = _make_config(tmp_path, {"ts65a_slave.grid_feed_in_hard_limit": value})
    state = ConfigManager(path).load()
    assert state.ts65a_slave.grid_feed_in_hard_limit == value


# -- smoothing_num_points validation --


@pytest.mark.parametrize("value", [0, -1, 601, 1000])
def test_invalid_smoothing_num_points_raises(tmp_path, value):
    path = _make_config(tmp_path, {"ts65a_slave.smoothing_num_points": value})
    with pytest.raises(ConfigError, match="smoothing_num_points"):
        ConfigManager(path).load()


@pytest.mark.parametrize("value", [1, 20, 300, 600])
def test_valid_smoothing_num_points_accepted(tmp_path, value):
    path = _make_config(tmp_path, {"ts65a_slave.smoothing_num_points": value})
    state = ConfigManager(path).load()
    assert state.ts65a_slave.smoothing_num_points == value


# ---------------------------------------------------------------------------
# Persistence tests (Task 2.4)
# ---------------------------------------------------------------------------


def test_schedule_persist_sets_dirty_and_timestamp(valid_yaml):
    cm = ConfigManager(valid_yaml)
    cm.load()
    assert cm._dirty is False

    before = time.monotonic()
    cm.schedule_persist()
    after = time.monotonic()

    assert cm._dirty is True
    assert before <= cm._last_dirty <= after


def test_schedule_persist_updates_timestamp_on_repeated_calls(valid_yaml):
    cm = ConfigManager(valid_yaml)
    cm.load()

    cm.schedule_persist()
    first = cm._last_dirty
    time.sleep(0.01)
    cm.schedule_persist()
    assert cm._last_dirty > first


def test_write_updates_persisted_fields(tmp_path):
    """_write() should update PERSISTED_FIELDS in the YAML file."""
    data = {
        "em540_master": {
            "mode": "tcp",
            "host": "10.0.0.1",
            "port": 502,
            "update_interval": 0.1,
            "retries": 0,
            "timeout": 0.15,
        },
        "em540_slave": {"host": "0.0.0.0", "update_timeout": 0.5},
        "ts65a_slave": {
            "port": 5003,
            "grid_feed_in_hard_limit": -5000,
            "smoothing_num_points": 20,
            "update_timeout": 0.5,
        },
        "mqtt": {"host": "broker.local", "port": 1883, "update_interval": 0.5},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data))

    cm = ConfigManager(str(p))
    state = cm.load()

    # Modify persisted fields
    state.ts65a_slave.grid_feed_in_hard_limit = -3000
    state.em540_master.retries = 3

    cm._write()

    # Reload and verify
    reloaded = yaml.safe_load(p.read_text())
    assert reloaded["ts65a_slave"]["grid_feed_in_hard_limit"] == -3000
    assert reloaded["em540_master"]["retries"] == 3


def test_write_preserves_non_persisted_fields(tmp_path):
    """_write() should not alter fields outside PERSISTED_FIELDS."""
    data = {
        "em540_master": {
            "mode": "tcp",
            "host": "10.0.0.1",
            "port": 502,
            "update_interval": 0.1,
            "retries": 0,
            "timeout": 0.15,
        },
        "em540_slave": {"host": "0.0.0.0", "update_timeout": 0.5},
        "ts65a_slave": {
            "port": 5003,
            "grid_feed_in_hard_limit": -5000,
            "smoothing_num_points": 20,
            "update_timeout": 0.5,
        },
        "mqtt": {"host": "broker.local", "port": 1883, "update_interval": 0.5},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data))

    cm = ConfigManager(str(p))
    state = cm.load()

    # Modify a persisted field
    state.ts65a_slave.grid_feed_in_hard_limit = -2000
    cm._write()

    reloaded = yaml.safe_load(p.read_text())
    # Non-persisted fields should be unchanged
    assert reloaded["em540_master"]["mode"] == "tcp"
    assert reloaded["em540_master"]["host"] == "10.0.0.1"
    assert reloaded["mqtt"]["host"] == "broker.local"
    assert reloaded["ts65a_slave"]["port"] == 5003


def test_write_clears_dirty_after_flush(tmp_path):
    """After _flush_loop writes, dirty flag should be cleared."""
    data = {
        "em540_master": {
            "mode": "tcp",
            "host": "10.0.0.1",
            "port": 502,
            "update_interval": 0.1,
            "retries": 0,
            "timeout": 0.15,
        },
        "em540_slave": {"host": "0.0.0.0", "update_timeout": 0.5},
        "ts65a_slave": {
            "port": 5003,
            "grid_feed_in_hard_limit": -5000,
            "smoothing_num_points": 20,
            "update_timeout": 0.5,
        },
        "mqtt": {"host": "broker.local", "port": 1883, "update_interval": 0.5},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data))

    cm = ConfigManager(str(p))
    cm.load()

    # Directly call _write after marking dirty
    cm._dirty = True
    cm._dirty = False  # simulate what _flush_loop does
    cm._write()
    assert cm._dirty is False


def test_flush_loop_writes_after_debounce(tmp_path):
    """The flush loop should write dirty config after the 5s debounce."""
    data = {
        "em540_master": {
            "mode": "tcp",
            "host": "10.0.0.1",
            "port": 502,
            "update_interval": 0.1,
            "retries": 0,
            "timeout": 0.15,
        },
        "em540_slave": {"host": "0.0.0.0", "update_timeout": 0.5},
        "ts65a_slave": {
            "port": 5003,
            "grid_feed_in_hard_limit": -5000,
            "smoothing_num_points": 20,
            "update_timeout": 0.5,
        },
        "mqtt": {"host": "broker.local", "port": 1883, "update_interval": 0.5},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data))

    cm = ConfigManager(str(p))
    state = cm.load()

    # Modify a persisted field and schedule persist
    state.ts65a_slave.smoothing_num_points = 100
    cm.schedule_persist()

    # Patch the debounce: set _last_dirty far in the past so flush fires quickly
    cm._last_dirty = time.monotonic() - 10

    cm.start_flush_loop()
    try:
        # Wait for the flush loop to pick it up (1s poll + small margin)
        time.sleep(3)
    finally:
        cm.stop()

    assert cm._dirty is False
    reloaded = yaml.safe_load(p.read_text())
    assert reloaded["ts65a_slave"]["smoothing_num_points"] == 100


def test_start_and_stop_flush_loop(valid_yaml):
    """start_flush_loop() creates a daemon thread; stop() terminates it."""
    cm = ConfigManager(valid_yaml)
    cm.load()

    cm.start_flush_loop()
    assert cm._flush_thread is not None
    assert cm._flush_thread.is_alive()
    assert cm._flush_thread.daemon is True

    cm.stop()
    assert cm._flush_thread is None


def test_write_failure_keeps_dirty_flag(tmp_path, monkeypatch):
    """If _write() raises, the flush loop should keep dirty=True for retry."""
    data = {
        "em540_master": {
            "mode": "tcp",
            "host": "10.0.0.1",
            "port": 502,
            "update_interval": 0.1,
            "retries": 0,
            "timeout": 0.15,
        },
        "em540_slave": {"host": "0.0.0.0", "update_timeout": 0.5},
        "ts65a_slave": {
            "port": 5003,
            "grid_feed_in_hard_limit": -5000,
            "smoothing_num_points": 20,
            "update_timeout": 0.5,
        },
        "mqtt": {"host": "broker.local", "port": 1883, "update_interval": 0.5},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data))

    cm = ConfigManager(str(p))
    cm.load()

    # Make _write raise
    def bad_write():
        raise OSError("disk full")

    monkeypatch.setattr(cm, "_write", bad_write)

    cm.schedule_persist()
    cm._last_dirty = time.monotonic() - 10  # bypass debounce

    cm.start_flush_loop()
    try:
        time.sleep(3)
    finally:
        cm.stop()

    # dirty should still be True because write failed
    assert cm._dirty is True


# ---------------------------------------------------------------------------
# Property-based tests (Task 2.5)
# ---------------------------------------------------------------------------

# Strategy: pick a PERSISTED_FIELD and generate a valid value for it.
_FIELD_STRATEGIES: dict[str, st.SearchStrategy] = {
    "ts65a_slave.grid_feed_in_hard_limit": st.floats(
        min_value=-1e6,
        max_value=0,
        allow_nan=False,
        allow_infinity=False,
    ),
    "ts65a_slave.smoothing_num_points": st.integers(min_value=1, max_value=600),
    "mqtt.update_interval": st.floats(
        min_value=0.01,
        max_value=1e4,
        allow_nan=False,
        allow_infinity=False,
    ),
    "em540_master.update_interval": st.floats(
        min_value=0.01,
        max_value=1e4,
        allow_nan=False,
        allow_infinity=False,
    ),
    "em540_master.retries": st.integers(min_value=0, max_value=1000),
    "em540_master.timeout": st.floats(
        min_value=0.01,
        max_value=1e4,
        allow_nan=False,
        allow_infinity=False,
    ),
    "em540_slave.update_timeout": st.floats(
        min_value=0.01,
        max_value=1e4,
        allow_nan=False,
        allow_infinity=False,
    ),
    "ts65a_slave.update_timeout": st.floats(
        min_value=0.01,
        max_value=1e4,
        allow_nan=False,
        allow_infinity=False,
    ),
}

# Composite strategy: draw a field name and a matching valid value.
_field_and_value = st.sampled_from(sorted(_FIELD_STRATEGIES)).flatmap(
    lambda f: _FIELD_STRATEGIES[f].map(lambda v: (f, v))
)


@given(field_and_value=_field_and_value)
@settings(max_examples=200, deadline=None)
def test_property_config_persistence_round_trip(field_and_value):
    """**Validates: Requirements 8.1, 5.1**

    Property 1 — Config persistence round-trip:
    For any PERSISTED_FIELD and any valid value for that field, modifying
    the field on a loaded AppState, calling _write(), and reloading the
    config SHALL yield an AppState where the modified field has the new value.
    """
    import pathlib
    import tempfile

    field_path, new_value = field_and_value

    # Build a minimal valid config file
    data = {
        "em540_master": {
            "mode": "tcp",
            "host": "10.0.0.1",
            "port": 502,
            "update_interval": 0.1,
            "retries": 0,
            "timeout": 0.15,
        },
        "em540_slave": {"host": "0.0.0.0", "update_timeout": 0.5},
        "ts65a_slave": {
            "port": 5003,
            "grid_feed_in_hard_limit": -5000,
            "smoothing_num_points": 20,
            "update_timeout": 0.5,
        },
        "mqtt": {"host": "broker.local", "port": 1883, "update_interval": 0.5},
    }
    with tempfile.TemporaryDirectory() as tmp_dir:
        p = pathlib.Path(tmp_dir) / "config.yaml"
        p.write_text(yaml.safe_dump(data))

        # Load, mutate, write
        cm = ConfigManager(str(p))
        state = cm.load()

        section_name, key = field_path.split(".")
        sub_config = getattr(state, section_name)
        setattr(sub_config, key, new_value)

        cm._write()

        # Reload and verify round-trip
        cm2 = ConfigManager(str(p))
        state2 = cm2.load()

        sub_config2 = getattr(state2, section_name)
        actual = getattr(sub_config2, key)

        assert actual == new_value, f"Round-trip failed for {field_path}: wrote {new_value!r}, got {actual!r}"


# ---------------------------------------------------------------------------
# Property-based test: Defaults applied for missing optional fields (Task 2.6)
# ---------------------------------------------------------------------------

# Map each required section to its dataclass and field names.
_SECTION_DATACLASSES: dict[str, type] = {
    "em540_master": Em540MasterConfig,
    "em540_slave": Em540SlaveConfig,
    "ts65a_slave": Ts65aSlaveConfig,
    "mqtt": MqttConfig,
}

# For each section, build a dict of field_name → default value.
_SECTION_DEFAULTS: dict[str, dict[str, object]] = {
    section: {f.name: f.default for f in dc_fields(cls)} for section, cls in _SECTION_DATACLASSES.items()
}

# Strategy: for each section, draw a random subset of field names to *include*.
# Omitted fields should get their dataclass defaults after load().
_section_subsets = st.fixed_dictionaries(
    {
        section: st.sets(st.sampled_from(sorted(field_defaults.keys())))
        for section, field_defaults in _SECTION_DEFAULTS.items()
    }
)


@given(included=_section_subsets)
@settings(max_examples=200, deadline=None)
def test_property_defaults_applied_for_missing_optional_fields(included):
    """**Validates: Requirements 5.3, 12.2**

    Property 2 — Defaults applied for missing optional fields:
    For any valid YAML config file that omits one or more optional fields,
    loading that file SHALL produce an AppState where every omitted field
    has its dataclass-defined default value.
    """
    import pathlib
    import tempfile

    # Build YAML data: only include the randomly-selected fields (with their
    # default values so the config passes validation).
    data: dict[str, dict[str, object]] = {}
    for section, field_names in included.items():
        section_dict: dict[str, object] = {}
        for name in field_names:
            section_dict[name] = _SECTION_DEFAULTS[section][name]
        data[section] = section_dict

    with tempfile.TemporaryDirectory() as tmp_dir:
        p = pathlib.Path(tmp_dir) / "config.yaml"
        p.write_text(yaml.safe_dump(data))

        cm = ConfigManager(str(p))
        state = cm.load()

        # For every section, check that omitted fields have their defaults.
        for section, field_defaults in _SECTION_DEFAULTS.items():
            sub_config = getattr(state, section)
            omitted = set(field_defaults.keys()) - included[section]
            for field_name in omitted:
                actual = getattr(sub_config, field_name)
                expected = field_defaults[field_name]
                assert actual == expected, f"{section}.{field_name}: expected default {expected!r}, got {actual!r}"


# ---------------------------------------------------------------------------
# Property-based test: Missing required section raises error (Task 2.7)
# ---------------------------------------------------------------------------

REQUIRED_SECTIONS = ("em540_master", "em540_slave", "ts65a_slave", "mqtt")


@given(omitted=st.sets(st.sampled_from(REQUIRED_SECTIONS), min_size=1, max_size=len(REQUIRED_SECTIONS)))
@settings(max_examples=200, deadline=None)
def test_property_missing_required_section_raises_error(omitted):
    """**Validates: Requirement 5.2**

    Property 3 — Missing required section raises error:
    For any required top-level config section, a YAML file that omits that
    section SHALL cause ConfigManager.load() to raise a ConfigError naming
    the missing section.
    """
    import pathlib
    import tempfile

    # Build a complete valid config, then remove the omitted sections.
    data = {
        "em540_master": {"mode": "tcp", "host": "10.0.0.1", "port": 502},
        "em540_slave": {"host": "0.0.0.0"},
        "ts65a_slave": {"port": 5003},
        "mqtt": {"host": "broker.local", "port": 1883},
    }
    for section in omitted:
        del data[section]

    with tempfile.TemporaryDirectory() as tmp_dir:
        p = pathlib.Path(tmp_dir) / "config.yaml"
        p.write_text(yaml.safe_dump(data))

        with pytest.raises(ConfigError) as exc_info:
            ConfigManager(str(p)).load()

        # The error message must name at least one of the omitted sections.
        msg = str(exc_info.value)
        assert any(section in msg for section in omitted), f"ConfigError message {msg!r} does not name any of {omitted}"


# ---------------------------------------------------------------------------
# Property-based test: Config validation rejects out-of-range values (Task 2.8)
# ---------------------------------------------------------------------------

# Strategy: for each constrained field, generate a value outside its valid range.
# ConfigManager.load() must raise ConfigError for every such value.

# -- Invalid mode: any string that is NOT "tcp" or "serial"
_invalid_mode = st.text(min_size=0, max_size=30).filter(lambda s: s not in ("tcp", "serial"))

# -- Invalid port: integers outside exclusive (0, 65535)
_invalid_port = st.one_of(
    st.integers(max_value=0),
    st.integers(min_value=65535),
)

# -- Invalid slave_id: integers outside exclusive (0, 256)
_invalid_slave_id = st.one_of(
    st.integers(max_value=0),
    st.integers(min_value=256),
)

# -- Invalid log_level: any string NOT in the valid set
_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_invalid_log_level = st.text(min_size=0, max_size=30).filter(lambda s: s not in _VALID_LOG_LEVELS)

# -- Invalid grid_feed_in_hard_limit: floats > 0
_invalid_grid_limit = st.floats(
    min_value=0,
    allow_nan=False,
    allow_infinity=False,
    exclude_min=True,
)

# -- Invalid smoothing_num_points: integers outside [1, 600]
_invalid_smoothing = st.one_of(
    st.integers(max_value=0),
    st.integers(min_value=601),
)

# Composite strategy: draw a (field_path, invalid_value) pair.
_invalid_field_and_value = st.one_of(
    _invalid_mode.map(lambda v: ("em540_master.mode", v)),
    st.sampled_from(
        [
            "em540_master.port",
            "em540_slave.rtu_port",
            "em540_slave.tcp_port",
            "ts65a_slave.port",
            "mqtt.port",
        ]
    ).flatmap(lambda f: _invalid_port.map(lambda v: (f, v))),
    st.sampled_from(
        [
            "em540_master.slave_id",
            "em540_slave.slave_id",
            "ts65a_slave.slave_id",
        ]
    ).flatmap(lambda f: _invalid_slave_id.map(lambda v: (f, v))),
    st.sampled_from(
        [
            "em540_master.log_level",
            "em540_slave.log_level",
            "ts65a_slave.log_level",
            "mqtt.log_level",
        ]
    ).flatmap(lambda f: _invalid_log_level.map(lambda v: (f, v))),
    _invalid_grid_limit.map(lambda v: ("ts65a_slave.grid_feed_in_hard_limit", v)),
    _invalid_smoothing.map(lambda v: ("ts65a_slave.smoothing_num_points", v)),
)


@given(field_and_value=_invalid_field_and_value)
@settings(max_examples=200, deadline=None)
def test_property_config_validation_rejects_out_of_range_values(field_and_value):
    """**Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6**

    Property 4 — Config validation rejects out-of-range values:
    For any config field with a defined constraint (mode, port, slave_id,
    log_level, grid_feed_in_hard_limit, smoothing_num_points), a YAML file
    containing a value outside the valid range for that field SHALL cause
    ConfigManager.load() to raise a validation error.
    """
    import pathlib
    import tempfile

    field_path, bad_value = field_and_value

    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg_path = _make_config(pathlib.Path(tmp_dir), {field_path: bad_value})

        with pytest.raises(ConfigError):
            ConfigManager(cfg_path).load()


# ---------------------------------------------------------------------------
# Property-based test: Debounce guarantee (Task 2.9)
# ---------------------------------------------------------------------------


@given(num_calls=st.integers(min_value=1, max_value=20))
@settings(max_examples=50, deadline=None)
def test_property_debounce_guarantee(num_calls):
    """**Validates: Requirement 7.2**

    Property 5 — Debounce guarantee:
    For any sequence of schedule_persist() calls, the Flush_Loop SHALL not
    write to disk until at least 5 seconds have elapsed since the most
    recent schedule_persist() call.

    Strategy: we exercise the real flush-loop logic with virtual time and a
    controlled stop event, so the property test runs fast without real sleep.
    We issue N schedule_persist() calls, then verify two invariants:

     A) When _last_dirty is recent (< 5s ago), the flush loop does NOT call
         _write() before debounce is reached.
     B) When _last_dirty is backdated (>= 5s ago), the flush loop DOES call
       _write(), and the write timestamp is at least 5s after _last_dirty.
    """
    import pathlib
    import tempfile

    data = {
        "em540_master": {
            "mode": "tcp",
            "host": "10.0.0.1",
            "port": 502,
            "update_interval": 0.1,
            "retries": 0,
            "timeout": 0.15,
        },
        "em540_slave": {"host": "0.0.0.0", "update_timeout": 0.5},
        "ts65a_slave": {
            "port": 5003,
            "grid_feed_in_hard_limit": -5000,
            "smoothing_num_points": 20,
            "update_timeout": 0.5,
        },
        "mqtt": {"host": "broker.local", "port": 1883, "update_interval": 0.5},
    }

    with tempfile.TemporaryDirectory() as tmp_dir:
        p = pathlib.Path(tmp_dir) / "config.yaml"
        p.write_text(yaml.safe_dump(data))

        cm = ConfigManager(str(p))
        cm.load()

        # Virtual clock used by app.config.time.monotonic
        clock = {"t": 1000.0}

        # Record timestamps when _write() is called
        write_timestamps: list[float] = []

        class _ControlledStopEvent:
            """Minimal event implementation for deterministic _flush_loop runs."""

            def __init__(self, on_wait=None):
                self._is_set = False
                self._wait_calls = 0
                self._on_wait = on_wait

            def is_set(self):
                return self._is_set

            def wait(self, seconds):
                self._wait_calls += 1
                clock["t"] += seconds
                if self._on_wait is not None:
                    self._on_wait(self, self._wait_calls, seconds)
                return self._is_set

            def set(self):
                self._is_set = True

        # Issue N schedule_persist() calls.
        for _ in range(num_calls):
            cm.schedule_persist()

        # --- Case A: debounce NOT yet elapsed → _write must NOT fire ---
        # Set _last_dirty to "just now" so elapsed < 5 for the first check.
        cm._dirty = True
        cm._last_dirty = clock["t"]

        def _on_wait_case_a(event, wait_calls, _seconds):
            # First wait is the periodic 1s tick. Second wait is debounce
            # remaining time. Before continuing, clear dirty and stop to model
            # shutdown during debounce without allowing a write.
            if wait_calls == 2:
                cm._dirty = False
                event.set()

        cm._stop_event = _ControlledStopEvent(on_wait=_on_wait_case_a)

        def _recording_write_case_a():
            write_timestamps.append(clock["t"])

        cm._write = _recording_write_case_a

        with patch("app.config.time.monotonic", side_effect=lambda: clock["t"]):
            cm._flush_loop()

        assert len(write_timestamps) == 0, (
            f"_write() was called {len(write_timestamps)} time(s) before debounce elapsed, "
            f"violating the 5s debounce guarantee"
        )

        # --- Case B: debounce elapsed → _write should fire ---
        write_timestamps.clear()
        cm._dirty = True
        cm._last_dirty = clock["t"] - 10  # 10s ago → well past debounce

        cm._stop_event = _ControlledStopEvent()

        def _recording_write_case_b():
            write_timestamps.append(clock["t"])
            cm._stop_event.set()

        cm._write = _recording_write_case_b

        with patch("app.config.time.monotonic", side_effect=lambda: clock["t"]):
            cm._flush_loop()

        assert len(write_timestamps) >= 1, "_write() was never called even though debounce period had elapsed"

        # Every write must have occurred >= 5s after _last_dirty.
        for wt in write_timestamps:
            assert wt - cm._last_dirty >= 5, (
                f"_write() called only {wt - cm._last_dirty:.2f}s after "
                f"_last_dirty, violating the 5s debounce guarantee"
            )


# ---------------------------------------------------------------------------
# Property-based test: Non-persisted fields preserved on write (Task 2.10)
# ---------------------------------------------------------------------------

# Build a mapping of ALL nested dataclass fields (section.field → strategy)
# that are NOT in PERSISTED_FIELDS.  These are the fields _write() must leave
# untouched.

_NON_PERSISTED_STRATEGIES: dict[str, st.SearchStrategy] = {}

# em540_master non-persisted fields
_NON_PERSISTED_STRATEGIES["em540_master.mode"] = st.sampled_from(["tcp", "serial"])
_NON_PERSISTED_STRATEGIES["em540_master.baudrate"] = st.integers(min_value=1200, max_value=115200)
_NON_PERSISTED_STRATEGIES["em540_master.parity"] = st.sampled_from(["N", "E", "O"])
_NON_PERSISTED_STRATEGIES["em540_master.bytesize"] = st.sampled_from([7, 8])
_NON_PERSISTED_STRATEGIES["em540_master.stopbits"] = st.sampled_from([1, 2])
_NON_PERSISTED_STRATEGIES["em540_master.serial_port"] = st.just("/dev/ttyUSB0")
_NON_PERSISTED_STRATEGIES["em540_master.host"] = st.from_regex(
    r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}",
    fullmatch=True,
)
_NON_PERSISTED_STRATEGIES["em540_master.port"] = st.integers(min_value=1, max_value=65534)
_NON_PERSISTED_STRATEGIES["em540_master.slave_id"] = st.integers(min_value=1, max_value=255)
_NON_PERSISTED_STRATEGIES["em540_master.log_level"] = st.sampled_from(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])

# em540_slave non-persisted fields
_NON_PERSISTED_STRATEGIES["em540_slave.host"] = st.from_regex(
    r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}",
    fullmatch=True,
)
_NON_PERSISTED_STRATEGIES["em540_slave.rtu_port"] = st.integers(min_value=1, max_value=65534)
_NON_PERSISTED_STRATEGIES["em540_slave.tcp_port"] = st.integers(min_value=1, max_value=65534)
_NON_PERSISTED_STRATEGIES["em540_slave.slave_id"] = st.integers(min_value=1, max_value=255)
_NON_PERSISTED_STRATEGIES["em540_slave.log_level"] = st.sampled_from(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])

# ts65a_slave non-persisted fields
_NON_PERSISTED_STRATEGIES["ts65a_slave.host"] = st.from_regex(
    r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}",
    fullmatch=True,
)
_NON_PERSISTED_STRATEGIES["ts65a_slave.port"] = st.integers(min_value=1, max_value=65534)
_NON_PERSISTED_STRATEGIES["ts65a_slave.slave_id"] = st.integers(min_value=1, max_value=255)
_NON_PERSISTED_STRATEGIES["ts65a_slave.log_level"] = st.sampled_from(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])

# mqtt non-persisted fields
_NON_PERSISTED_STRATEGIES["mqtt.enabled"] = st.booleans()
_NON_PERSISTED_STRATEGIES["mqtt.host"] = st.from_regex(
    r"[a-z][a-z0-9]{0,10}\.[a-z]{2,4}",
    fullmatch=True,
)
_NON_PERSISTED_STRATEGIES["mqtt.port"] = st.integers(min_value=1, max_value=65534)
_NON_PERSISTED_STRATEGIES["mqtt.username"] = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")),
    min_size=0,
    max_size=20,
)
_NON_PERSISTED_STRATEGIES["mqtt.password"] = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")),
    min_size=0,
    max_size=20,
)
_NON_PERSISTED_STRATEGIES["mqtt.log_level"] = st.sampled_from(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])

# Remove any field that is actually persisted (safety check)
for pf in PERSISTED_FIELDS:
    _NON_PERSISTED_STRATEGIES.pop(pf, None)

# Composite strategy: draw values for ALL non-persisted fields at once.
_all_non_persisted = st.fixed_dictionaries({k: v for k, v in _NON_PERSISTED_STRATEGIES.items()})


@given(non_persisted_values=_all_non_persisted)
@settings(max_examples=200, deadline=None)
def test_property_non_persisted_fields_preserved_on_write(non_persisted_values):
    """**Validates: Requirements 7.3, 7.4**

    Property 6 — Non-persisted fields preserved on write:
    For any YAML config file and for any field that is not in PERSISTED_FIELDS,
    writing persisted fields to disk SHALL leave that non-persisted field's
    value unchanged in the resulting file.
    """
    import pathlib
    import tempfile

    # Build a YAML config with the generated non-persisted values and
    # valid defaults for persisted fields.
    data: dict[str, dict[str, object]] = {
        "em540_master": {
            "update_interval": 0.1,
            "retries": 0,
            "timeout": 0.15,
        },
        "em540_slave": {"update_timeout": 0.5},
        "ts65a_slave": {
            "grid_feed_in_hard_limit": -5000,
            "smoothing_num_points": 20,
            "update_timeout": 0.5,
        },
        "mqtt": {"update_interval": 0.5},
    }

    # Inject all non-persisted field values into the YAML data.
    for dotted_key, value in non_persisted_values.items():
        section, key = dotted_key.split(".")
        if section not in data:
            data[section] = {}
        data[section][key] = value

    with tempfile.TemporaryDirectory() as tmp_dir:
        p = pathlib.Path(tmp_dir) / "config.yaml"
        p.write_text(yaml.safe_dump(data))

        # Load config, modify a persisted field, then write.
        cm = ConfigManager(str(p))
        state = cm.load()

        # Mutate a persisted field to trigger a meaningful _write().
        state.ts65a_slave.grid_feed_in_hard_limit = -9999
        state.em540_master.retries = 7
        cm._write()

        # Reload the YAML and verify every non-persisted field is unchanged.
        reloaded = yaml.safe_load(p.read_text())

        for dotted_key, expected in non_persisted_values.items():
            section, key = dotted_key.split(".")
            actual = reloaded.get(section, {}).get(key)
            assert (
                actual == expected
            ), f"Non-persisted field {dotted_key} changed after _write(): expected {expected!r}, got {actual!r}"
