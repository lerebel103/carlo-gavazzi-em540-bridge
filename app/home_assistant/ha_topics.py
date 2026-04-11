from __future__ import annotations

import re

_BASE_NAMESPACE = "em540_bridge"


def normalize_topic_prefix(topic_prefix: str) -> str:
    return topic_prefix.strip().strip("/")


def prefix_topic(topic: str, topic_prefix: str) -> str:
    normalized = normalize_topic_prefix(topic_prefix)
    return f"{normalized}/{topic}" if normalized else topic


def topic_prefix_identifier(topic_prefix: str) -> str:
    normalized = normalize_topic_prefix(topic_prefix)
    if not normalized:
        return ""

    # Keep IDs Home Assistant friendly: letters, numbers and underscores.
    identifier = re.sub(r"[^a-zA-Z0-9]+", "_", normalized).strip("_").lower()
    return identifier


def topic_namespace(topic_prefix: str) -> str:
    identifier = topic_prefix_identifier(topic_prefix)
    if not identifier:
        return _BASE_NAMESPACE
    return f"{_BASE_NAMESPACE}_{identifier}"


def discovery_name_prefix(topic_prefix: str) -> str:
    normalized = normalize_topic_prefix(topic_prefix)
    if not normalized:
        return ""
    return f"[{normalized}] "


def discovery_model_name(topic_prefix: str, base_model: str = "EM540 Bridge") -> str:
    prefix = discovery_name_prefix(topic_prefix)
    return f"{prefix}{base_model}" if prefix else base_model
