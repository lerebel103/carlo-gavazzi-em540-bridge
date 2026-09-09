"""Tests for the standalone RS485 bus sniffer diagnostic (tools/rs485_sniffer).

TEMPORARY DIAGNOSTIC — remove alongside tools/rs485_sniffer/.

The sniffer lives under tools/ (not an installed package), so it is loaded here
by file path. These tests cover the parts whose correctness the diagnostic's
value depends on: the Modbus CRC, frame decoding (request / response / exception
/ corrupt), and the idle-gap frame-assembly state machine — including that two
adjacent frames separated by only the minimum inter-frame gap are NOT coalesced.
"""

import importlib.util
import pathlib
import sys
import unittest

_SNIFFER_PATH = pathlib.Path(__file__).resolve().parents[2] / "tools" / "rs485_sniffer" / "sniffer.py"
_spec = importlib.util.spec_from_file_location("rs485_sniffer", _SNIFFER_PATH)
sniffer = importlib.util.module_from_spec(_spec)
# Register in sys.modules before exec so @dataclass introspection (which looks up
# the class's __module__ in sys.modules) works for a path-loaded module.
sys.modules["rs485_sniffer"] = sniffer
_spec.loader.exec_module(sniffer)


def _cfg(baudrate=9600):
    return sniffer.Config(
        port="x",
        baudrate=baudrate,
        parity="N",
        bytesize=8,
        stopbits=1,
        raw_csv="/tmp/_r.csv",
        parsed_csv="/tmp/_p.csv",
        tz=None,
    )


class TestCrc(unittest.TestCase):
    def test_known_good_request_crc(self):
        # Read holding 40071 count=58 — CRC 5b a0 (from real captured traffic).
        frame = bytes.fromhex("01039c87003a5ba0")
        self.assertTrue(sniffer.crc_ok(frame))

    def test_known_good_response_crc(self):
        # id=1 fc=3 bytecount=4 zeros — CRC fa 33 (from real captured traffic).
        self.assertTrue(sniffer.crc_ok(bytes.fromhex("01030400000000fa33")))

    def test_bad_crc_detected(self):
        frame = bytearray(bytes.fromhex("01039c87003a5ba0"))
        frame[3] ^= 0xFF  # corrupt a byte
        self.assertFalse(sniffer.crc_ok(bytes(frame)))

    def test_runt_frame_is_not_crc_ok(self):
        self.assertFalse(sniffer.crc_ok(b"\x01\x03"))


class TestDecode(unittest.TestCase):
    def test_read_request(self):
        d = sniffer.decode_frame(bytes.fromhex("01039c87003a5ba0"))
        self.assertEqual(d["direction"], "request")
        self.assertEqual(d["fc"], 3)
        self.assertEqual(d["addr"], 40071)
        self.assertEqual(d["count"], 58)
        self.assertTrue(d["crc_ok"])

    def test_read_response(self):
        # id=1 fc=3 bytecount=4 data(4 zero bytes) crc(fa 33) = 9 bytes total.
        d = sniffer.decode_frame(bytes.fromhex("01030400000000fa33"))
        self.assertEqual(d["direction"], "response")
        self.assertEqual(d["bytecount"], 4)

    def test_exception_response(self):
        d = sniffer.decode_frame(bytes.fromhex("288304913b"))
        self.assertEqual(d["direction"], "response")
        self.assertEqual(d["fc"], 3)
        self.assertEqual(d["exception"], 4)

    def test_corrupt_frame_flagged(self):
        frame = bytearray(bytes.fromhex("01039c87003a5ba0"))
        frame[3] ^= 0xFF
        d = sniffer.decode_frame(bytes(frame))
        self.assertFalse(d["crc_ok"])

    def test_fc3_response_not_mislabelled_as_request(self):
        # An FC3 read response with exactly 3 data bytes is 8 bytes total
        # (id, fc, bytecount=3, d0, d1, d2, crc, crc). It must decode as a
        # response (bytecount==len-5 wins), not a request.
        body = bytes([0x01, 0x03, 0x03, 0x11, 0x22, 0x33])
        c = sniffer.modbus_crc(body)
        frame = body + bytes([c & 0xFF, (c >> 8) & 0xFF])
        self.assertEqual(len(frame), 8)
        d = sniffer.decode_frame(frame)
        self.assertEqual(d["direction"], "response")
        self.assertEqual(d["bytecount"], 3)

    def test_fc1_eight_byte_frame_is_ambiguous(self):
        # An 8-byte FC1 (bit read) frame cannot be reliably classified: it could
        # be a request, or a response carrying 3 data bytes. Must NOT assert
        # 'request' with invented address/count.
        body = bytes([0x01, 0x01, 0x00, 0x13, 0x00, 0x25])  # request-shaped
        c = sniffer.modbus_crc(body)
        frame = body + bytes([c & 0xFF, (c >> 8) & 0xFF])
        d = sniffer.decode_frame(frame)
        self.assertNotEqual(d["direction"], "request")
        self.assertEqual(d["addr"], "")
        self.assertEqual(d["count"], "")
        self.assertIn("ambiguous", d["summary"])


class TestInterframeGap(unittest.TestCase):
    def test_char_and_gap_math_9600(self):
        cfg = _cfg(9600)
        # 8N1 -> 10 bits/char -> ~1.042ms; 3.5 chars ~3.65ms, above the 2ms floor.
        self.assertAlmostEqual(cfg.char_time_s() * 1000, 1.0416, places=3)
        self.assertAlmostEqual(cfg.interframe_gap_s() * 1000, 3.6458, places=3)

    def test_gap_floor_at_high_baud(self):
        # At high baud, 3.5 char times is tiny; the 2ms floor applies.
        self.assertEqual(_cfg(1_000_000).interframe_gap_s(), 0.002)


class TestFraming(unittest.TestCase):
    """Exercise the _feed state machine directly (no serial port)."""

    def setUp(self):
        # Avoid opening files / starting threads: build the object and only use _feed.
        self.s = sniffer.Sniffer.__new__(sniffer.Sniffer)
        self.s._buf = bytearray()
        self.s._last_byte_mono = None
        self.s._gap = _cfg().interframe_gap_s()

    def _feed_bytes(self, data: bytes, t: float):
        out = []
        for i, byte in enumerate(data):
            # Feed byte-by-byte at the same instant (arrivals within a frame).
            out.extend(self.s._feed(bytes([byte]), t))
        return out

    def test_complete_crc_frame_emitted_on_completion(self):
        # A CRC-valid frame is emitted as soon as its last byte arrives (the CRC
        # splitter), without waiting for an idle gap. Feeding byte-by-byte, the
        # frame appears on the final byte.
        frame = bytes.fromhex("01039c87003a5ba0")
        got = self._feed_bytes(frame, t=100.0)
        self.assertEqual(got, [frame])
        self.assertEqual(len(self.s._buf), 0)

    def test_adjacent_frames_min_gap_not_coalesced(self):
        # Two valid frames separated only by the minimum gap must not merge. The
        # first is emitted (by CRC completion), the second on its completion.
        req = bytes.fromhex("01039c87003a5ba0")
        resp = bytes.fromhex("01030400000000fa33")
        t = 100.0
        got = self._feed_bytes(req, t)
        self.assertEqual(got, [req])
        t2 = t + self.s._gap
        flushed = self._feed_bytes(resp, t2)
        self.assertEqual(flushed, [resp])

    def test_incomplete_frame_flushed_after_idle_gap(self):
        # A frame that never completes to a valid CRC (partial/corrupt) is still
        # delimited by the idle-gap path so it reaches the CSV as evidence.
        partial = bytes.fromhex("0103")
        got = self._feed_bytes(partial, t=100.0)
        self.assertEqual(got, [])  # not CRC-complete, no idle yet
        flushed = list(self.s._feed(b"", 100.0 + self.s._gap))
        self.assertEqual(flushed, [partial])

    def test_bytes_within_gap_stay_one_frame(self):
        # Two bursts closer than the gap, neither CRC-complete, stay one frame.
        t = 100.0
        self._feed_bytes(b"\x01\x03", t)
        got = self._feed_bytes(b"\x9c\x00", t + self.s._gap / 2.0)
        self.assertEqual(got, [])  # not flushed; still within one frame
        flushed = list(self.s._feed(b"", t + self.s._gap / 2.0 + self.s._gap))
        self.assertEqual(flushed, [b"\x01\x03\x9c\x00"])

    def test_crc_split_separates_driver_batched_frames(self):
        # Driver batches a request and its response back-to-back with NO idle gap
        # between them (both delivered in one read). The CRC-based splitter must
        # separate them instead of coalescing into one false bad-CRC frame.
        req = bytes.fromhex("01039c87003a5ba0")  # valid CRC
        resp = bytes.fromhex("01030400000000fa33")  # valid CRC
        got = list(self.s._feed(req + resp, 100.0))
        self.assertEqual(got, [req, resp])
        # Buffer fully drained.
        self.assertEqual(len(self.s._buf), 0)

    def test_crc_split_leaves_trailing_partial(self):
        # A complete frame followed by a partial next frame: emit the complete
        # one immediately, keep the partial for the idle path.
        req = bytes.fromhex("01039c87003a5ba0")
        partial = bytes.fromhex("0103")  # start of another frame, incomplete
        got = list(self.s._feed(req + partial, 100.0))
        self.assertEqual(got, [req])
        self.assertEqual(bytes(self.s._buf), partial)

    def test_buffer_cap_force_flushes(self):
        # A continuous non-CRC-framing stream must not grow without bound: once
        # the buffer exceeds the cap it is force-flushed to the CSV.
        cap = sniffer._MAX_BUFFER_BYTES
        # Feed cap+10 bytes of 0xff (never a valid CRC frame) in one batch.
        got = list(self.s._feed(b"\xff" * (cap + 10), 100.0))
        self.assertEqual(len(got), 1)
        self.assertGreaterEqual(len(got[0]), cap)
        self.assertEqual(len(self.s._buf), 0)


if __name__ == "__main__":
    unittest.main()
