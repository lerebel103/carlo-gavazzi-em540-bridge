"""Unit tests for app version resolution and display formatting."""

from __future__ import annotations

import importlib


def test_version_for_display_adds_single_v_for_semver(monkeypatch):
    monkeypatch.setenv("EM540_BRIDGE_VERSION", "1.2.3")
    import app.version as version

    importlib.reload(version)
    assert version.__version__ == "1.2.3"
    assert version.version_for_display() == "v1.2.3"


def test_version_for_display_does_not_double_prefix(monkeypatch):
    monkeypatch.setenv("EM540_BRIDGE_VERSION", "v2.3.4")
    import app.version as version

    importlib.reload(version)
    assert version.__version__ == "v2.3.4"
    assert version.version_for_display() == "v2.3.4"


def test_version_for_display_keeps_non_semver_as_is(monkeypatch):
    monkeypatch.setenv("EM540_BRIDGE_VERSION", "dev")
    import app.version as version

    importlib.reload(version)
    assert version.__version__ == "dev"
    assert version.version_for_display() == "dev"
