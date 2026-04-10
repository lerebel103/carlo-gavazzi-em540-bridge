"""Version resolution for the EM540 bridge integration."""

from __future__ import annotations

import os
import re
import subprocess


def _resolve_version() -> str:
    """Resolve app version from env, then git metadata, then a static fallback."""
    env_version = os.getenv("EM540_BRIDGE_VERSION")
    if env_version:
        return env_version.strip()

    try:
        git_version = subprocess.check_output(
            ["git", "describe", "--tags", "--always", "--dirty"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        if git_version:
            return git_version
    except Exception:
        pass

    return "0.1.0"


__version__ = _resolve_version()


def version_for_display() -> str:
    """Return a user-facing version string with at most one leading 'v'."""
    if __version__.startswith("v"):
        return __version__

    # Prefix semver-like versions with 'v' for UI/log display consistency.
    if re.match(r"^\d+\.\d+\.\d+([-.].*)?$", __version__):
        return f"v{__version__}"

    return __version__
