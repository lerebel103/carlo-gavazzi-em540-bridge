"""Tests for the temporary PduTraceRing diagnostic.

TEMPORARY DIAGNOSTIC — REMOVE BEFORE MERGE (alongside app/utils/pdu_trace_ring.py).
"""

import threading
import unittest
from types import SimpleNamespace

from app.utils.pdu_trace_ring import (
    _CSV_HEADER,
    PduTraceRing,
    TraceRecord,
)


class _CapturingLogger:
    """Minimal logger stub that records emitted messages thread-safely."""

    def __init__(self):
        self._lock = threading.Lock()
        self.messages: list[str] = []
        self.debug_messages: list[str] = []

    def info(self, msg, *args, **kwargs):
        with self._lock:
            self.messages.append(msg % args if args else msg)

    def debug(self, msg, *args, **kwargs):
        with self._lock:
            self.debug_messages.append(msg)

    def snapshot(self) -> list[str]:
        with self._lock:
            return list(self.messages)


def _make_pdu(function_code=3, dev_id=1, address=0, count=2, exception_code=0, registers=None):
    ns = SimpleNamespace(
        function_code=function_code,
        dev_id=dev_id,
        address=address,
        count=count,
        exception_code=exception_code,
    )
    if registers is not None:
        ns.registers = registers
    return ns


def _wait_for(predicate, timeout=2.0, interval=0.005):
    """Poll until predicate() is true or timeout elapses (for the async dump)."""
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class TestPduTraceRingCapture(unittest.TestCase):
    def setUp(self):
        self.logger = _CapturingLogger()
        self.ring = PduTraceRing(self.logger, ring_size=5, dump_cooldown_s=0.0)

    def tearDown(self):
        self.ring.stop()

    def test_record_packet_and_pdu_are_captured(self):
        self.ring.record_packet(False, bytes([0x01, 0x03, 0x00, 0x00]))
        self.ring.record_pdu(True, _make_pdu(registers=[0x1234, 0x5678]))

        records = list(self.ring._ring)
        self.assertEqual(len(records), 2)

        pkt, pdu = records
        self.assertEqual(pkt.direction, "rx")
        self.assertEqual(pkt.layer, "packet")
        self.assertEqual(pkt.payload_hex, "01 03 00 00")

        self.assertEqual(pdu.direction, "tx")
        self.assertEqual(pdu.layer, "pdu")
        self.assertEqual(pdu.fc, 3)
        self.assertEqual(pdu.payload_hex, "1234 5678")

    def test_seq_is_monotonic(self):
        for _ in range(4):
            self.ring.record_packet(False, b"\x00")
        seqs = [r.seq for r in self.ring._ring]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(len(set(seqs)), len(seqs))

    def test_ring_evicts_oldest_beyond_maxlen(self):
        # ring_size is 5; push 8 records.
        for _ in range(8):
            self.ring.record_packet(False, b"\x00")
        records = list(self.ring._ring)
        self.assertEqual(len(records), 5)
        # Oldest three (seq 0,1,2) evicted; tail should be seq 3..7.
        self.assertEqual([r.seq for r in records], [3, 4, 5, 6, 7])

    def test_record_pdu_handles_missing_attributes(self):
        # A PDU-like object with no modbus attributes must not raise on capture.
        self.ring.record_pdu(False, SimpleNamespace())
        rec = list(self.ring._ring)[-1]
        self.assertIsNone(rec.fc)
        self.assertIsNone(rec.addr)
        self.assertEqual(rec.payload_hex, "")

    def test_full_rtu_frame_is_retained_without_truncation(self):
        # A ~90-register read response is ~185 bytes on the wire. The whole point
        # of the raw-packet capture is framing/CRC evidence, so a legal-size frame
        # must be retained in full (including the trailing CRC).
        frame = bytes(range(256))[:185]
        self.ring.record_packet(True, frame)
        rec = list(self.ring._ring)[-1]
        # payload_hex is space-separated bytes; count must equal the frame length.
        self.assertEqual(len(rec.payload_hex.split(" ")), len(frame))
        self.assertTrue(rec.payload_hex.endswith(f"{frame[-1]:02x}"))

    def test_all_pdu_registers_are_retained(self):
        # The TS65A dynamic block is ~90 registers; none must be dropped.
        registers = list(range(90))
        self.ring.record_pdu(True, _make_pdu(registers=registers))
        rec = list(self.ring._ring)[-1]
        self.assertEqual(len(rec.payload_hex.split(" ")), len(registers))


class TestPduTraceRingDump(unittest.TestCase):
    def setUp(self):
        self.logger = _CapturingLogger()
        self.ring = PduTraceRing(self.logger, ring_size=100, dump_cooldown_s=0.0)

    def tearDown(self):
        self.ring.stop()

    def test_dump_emits_single_csv_block(self):
        self.ring.record_packet(False, bytes([0xAA, 0xBB]))
        self.ring.record_pdu(False, _make_pdu(address=50000, count=2))

        scheduled = self.ring.request_dump("read 50000")
        self.assertTrue(scheduled)

        self.assertTrue(_wait_for(lambda: len(self.logger.snapshot()) >= 1))
        msgs = self.logger.snapshot()

        # Exactly one logging call for the whole dump (atomic emit).
        self.assertEqual(len(msgs), 1)
        block = msgs[0]

        lines = block.splitlines()
        self.assertIn("TS65A serial PDU trace dump", lines[0])
        self.assertIn("read 50000", lines[0])
        self.assertEqual(lines[1], _CSV_HEADER)
        self.assertEqual(lines[-1], "=== end dump ===")

        # Two data rows between header and footer.
        data_rows = lines[2:-1]
        self.assertEqual(len(data_rows), 2)
        # Every data row must have the same column count as the header.
        expected_cols = len(_CSV_HEADER.split(","))
        for row in data_rows:
            self.assertEqual(len(row.split(",")), expected_cols)

    def test_delta_ms_first_row_empty_then_populated(self):
        self.ring._ring.append(TraceRecord(seq=0, mono_ts=10.0, wall_ts=1000.0, direction="rx", layer="packet"))
        self.ring._ring.append(TraceRecord(seq=1, mono_ts=10.5, wall_ts=1000.5, direction="tx", layer="pdu"))

        self.ring.request_dump("manual")
        self.assertTrue(_wait_for(lambda: len(self.logger.snapshot()) >= 1))

        lines = self.logger.snapshot()[0].splitlines()
        data_rows = lines[2:-1]
        first_delta = data_rows[0].split(",")[-1]
        second_delta = data_rows[1].split(",")[-1]
        self.assertEqual(first_delta, "")  # no previous record
        self.assertEqual(second_delta, "500.000")  # 0.5s => 500ms

    def test_wall_timestamp_is_iso_utc(self):
        self.ring._ring.append(TraceRecord(seq=0, mono_ts=1.0, wall_ts=0.0, direction="rx", layer="packet"))
        self.ring.request_dump("manual")
        self.assertTrue(_wait_for(lambda: len(self.logger.snapshot()) >= 1))
        row = self.logger.snapshot()[0].splitlines()[2]
        wall = row.split(",")[2]
        # Epoch 0 in UTC.
        self.assertEqual(wall, "1970-01-01T00:00:00.000+00:00")


class TestPduTraceRingCooldown(unittest.TestCase):
    def test_cooldown_suppresses_second_dump(self):
        logger = _CapturingLogger()
        ring = PduTraceRing(logger, ring_size=10, dump_cooldown_s=60.0)
        self.addCleanup(ring.stop)

        ring.record_packet(False, b"\x00")

        first = ring.request_dump("read 50000")
        second = ring.request_dump("read 50000")

        self.assertTrue(first)
        self.assertFalse(second)  # within cooldown window

        self.assertTrue(_wait_for(lambda: len(logger.snapshot()) >= 1))
        # Only the first dump was emitted.
        self.assertEqual(len(logger.snapshot()), 1)

    def test_first_dump_not_suppressed_near_boot(self):
        # With a large cooldown and a fresh ring, the very first trigger must
        # still fire. Regression guard for initialising _last_dump_mono to 0.0,
        # which suppressed the first dump when monotonic uptime < cooldown.
        logger = _CapturingLogger()
        ring = PduTraceRing(logger, ring_size=10, dump_cooldown_s=3600.0)
        self.addCleanup(ring.stop)

        ring.record_packet(False, b"\x00")
        self.assertTrue(ring.request_dump("read 50000"))
        self.assertTrue(_wait_for(lambda: len(logger.snapshot()) >= 1))


if __name__ == "__main__":
    unittest.main()
