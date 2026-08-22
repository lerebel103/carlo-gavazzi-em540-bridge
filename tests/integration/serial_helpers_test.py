"""Unit tests for serial integration helpers."""

from __future__ import annotations

import os
import threading

from .serial_helpers import SerialCable


def test_serial_cable_write_all_retries_partial_writes(monkeypatch):
    cable = object.__new__(SerialCable)
    cable.label = "test-cable"
    cable._stop = threading.Event()

    writes: list[bytes] = []

    def fake_write(fd: int, payload):
        data = bytes(payload)
        writes.append(data)
        if len(writes) == 1:
            return 2
        return len(data)

    monkeypatch.setattr(os, "write", fake_write)

    SerialCable._write_all(cable, 123, b"abcdef")

    assert writes == [b"abcdef", b"cdef"]
