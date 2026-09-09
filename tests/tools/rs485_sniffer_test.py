"""Tests for the standalone RS485 bus sniffer diagnostic (tools/rs485_sniffer).

TEMPORARY DIAGNOSTIC — remove alongside tools/rs485_sniffer/.

The sniffer lives under tools/ (not an installed package), so it is loaded here
by file path. These tests cover the parts whose correctness the diagnostic's
value depends on: the Modbus CRC, frame decoding (request / response / exception
/ corrupt), and the idle-gap frame-assembly state machine — including that two
adjacent frames separated by only the minimum inter-frame gap are NOT coalesced.
"""

import importlib.util
import math
import pathlib
import sys
import unittest

_TOOLS_DIR = pathlib.Path(__file__).resolve().parents[2] / "tools" / "rs485_sniffer"

_spec = importlib.util.spec_from_file_location("rs485_sniffer", _TOOLS_DIR / "sniffer.py")
sniffer = importlib.util.module_from_spec(_spec)
# Register in sys.modules before exec so @dataclass introspection (which looks up
# the class's __module__ in sys.modules) works for a path-loaded module.
sys.modules["rs485_sniffer"] = sniffer
_spec.loader.exec_module(sniffer)

_pc_spec = importlib.util.spec_from_file_location("parse_capture", _TOOLS_DIR / "parse_capture.py")
parse_capture = importlib.util.module_from_spec(_pc_spec)
sys.modules["parse_capture"] = parse_capture
_pc_spec.loader.exec_module(parse_capture)


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


class TestExpectedFrameLen(unittest.TestCase):
    def test_fc3_response_length_from_bytecount(self):
        # id, fc=3, bytecount=116 -> 3 + 116 + 2 = 121.
        self.assertEqual(sniffer.expected_frame_len(bytes([0x01, 0x03, 0x74])), 121)

    def test_fc3_request_length(self):
        # An 8-byte request header (bytecount byte 0x00 -> resp_len 5 < 8 -> 8).
        self.assertEqual(sniffer.expected_frame_len(bytes([0x01, 0x03, 0x00, 0x00])), 8)

    def test_exception_length(self):
        self.assertEqual(sniffer.expected_frame_len(bytes([0x01, 0x83])), 5)

    def test_unrecognised_returns_none(self):
        self.assertIsNone(sniffer.expected_frame_len(bytes([0x01, 0x30])))

    def test_too_short_returns_none(self):
        self.assertIsNone(sniffer.expected_frame_len(b"\x01"))


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

    def test_unrecognised_bytes_flushed_after_idle_gap(self):
        # Bytes with no recognisable Modbus header shape are delimited by the
        # idle-gap path so they still reach the CSV as evidence (garbage/noise).
        # fc=0x30 (48) is not a known function code and has the high bit clear,
        # so expected_frame_len() returns None -> the shape is unrecognised and
        # an idle gap is treated as a real boundary.
        junk = bytes([0x09, 0x30, 0x11])
        got = self._feed_bytes(junk, t=100.0)
        self.assertEqual(got, [])  # no idle yet
        flushed = list(self.s._feed(b"", 100.0 + self.s._gap))
        self.assertEqual(flushed, [junk])

    def test_incomplete_frame_not_split_by_idle_gap(self):
        # A large response delivered in packets spaced WIDER than the RTU gap
        # must NOT be split: the buffer is a recognisable header whose declared
        # length is unmet, so idle flushes are suppressed until it completes.
        # id=1 fc=3 bytecount=4 -> total 9 bytes; feed 3 bytes, idle, then rest.
        head = bytes([0x01, 0x03, 0x04])
        got = self._feed_bytes(head, t=100.0)
        self.assertEqual(got, [])
        # Idle gap elapses but frame is incomplete (expected 9, have 3) -> hold.
        held = list(self.s._feed(b"", 100.0 + 5 * self.s._gap))
        self.assertEqual(held, [])
        self.assertEqual(len(self.s._buf), 3)
        # Remaining bytes arrive later; frame completes and is emitted by CRC.
        rest = bytes([0x00, 0x00, 0x00, 0x00, 0xFA, 0x33])
        out = self._feed_bytes(rest, t=100.0 + 10 * self.s._gap)
        self.assertEqual(out, [bytes.fromhex("01030400000000fa33")])

    def test_multipacket_response_reassembled(self):
        # Regression for the real capture: a 121-byte FC3 response (bytecount
        # 0x74) delivered in 16-byte packets spaced ~16ms apart (well over the
        # ~3.6ms gap) must reassemble into ONE frame, not fragment into many.
        frags = [
            "01 03 74 40 82 f5 b2 3f a7 c2 5d 3f ba 35 1e 3f",
            "a9 df 4c 43 6d e7 9e 43 6f 03 40 43 6e c2 a4 43",
            "6c 12 49 43 ce 0d 01 43 ce 5b d6 43 ce 66 66 43",
            "cd 72 cb 42 48 00 00 42 b5 08 f1 41 ad 00 d0 42",
            "23 9c da 41 df e9 3f 43 cf d0 48 43 12 e4 83 43",
            "0c 6e ea 43 01 15 cd c3 ca d3 4d c3 11 4a c5 c3",
            "06 58 2e c2 fc 07 50 3e 5f 03 16 3e 16 c0 99 3e",
            "95 20 93 3e 5e 07 5f 1b 5f",
        ]
        emitted = []
        t = 100.0
        for fr in frags:
            t += 0.016  # 16ms apart, > gap
            emitted.extend(self.s._feed(bytes.fromhex(fr.replace(" ", "")), t))
        self.assertEqual(len(emitted), 1)
        frame = emitted[0]
        self.assertEqual(len(frame), 121)
        d = sniffer.decode_frame(frame)
        self.assertEqual(d["direction"], "response")
        self.assertEqual(d["bytecount"], 116)
        self.assertTrue(d["crc_ok"])

    def test_bytes_within_gap_stay_one_frame(self):
        # Two bursts closer than the gap, neither CRC-complete, stay one frame.
        # Use an unrecognised shape (fc=0x30) so framing is governed purely by
        # timing: two sub-gap bursts stay one frame, flushed on the next idle.
        t = 100.0
        self._feed_bytes(b"\x09\x30", t)
        got = self._feed_bytes(b"\x11\x22", t + self.s._gap / 2.0)
        self.assertEqual(got, [])  # not flushed; still within one frame
        flushed = list(self.s._feed(b"", t + self.s._gap / 2.0 + self.s._gap))
        self.assertEqual(flushed, [b"\x09\x30\x11\x22"])

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


class TestParseCapture(unittest.TestCase):
    """Decode a real reassembled 40071 response into named meter values."""

    # A genuine 121-byte FC3 response captured on the bus (seq 7086-7093
    # reassembled), bytecount 0x74, CRC valid.
    _FRAME_HEX = (
        "01 03 74 40 85 b7 1e 3f b4 11 d0 3f bc b8 84 3f "
        "a6 12 24 43 6d 87 50 43 6e ed 4f 43 6e 56 f5 43 "
        "6b 6b af 43 cd b8 e4 43 ce 23 c2 43 cd b0 5b 43 "
        "cd 62 22 42 48 00 00 42 87 07 50 41 4e 73 67 41 "
        "ec 03 40 41 c8 e0 4e 43 d6 38 c1 43 22 e1 51 43 "
        "09 90 06 43 00 d4 ba c3 d3 8b 95 c3 22 5e 46 c3 "
        "06 5c a6 c2 fc b8 7c 3e 21 5c c3 3d a2 3d 7b 3e "
        "5b 9b 3e 3e 47 94 9d 9c 35"
    )

    def _decode(self):
        frame = bytes.fromhex(self._FRAME_HEX.replace(" ", ""))
        regs = parse_capture._response_registers(frame)
        return regs, parse_capture._decode_dynamic(regs, field_offset=0)

    def test_response_registers_extracted(self):
        regs, _ = self._decode()
        # bytecount 116 -> 58 registers.
        self.assertEqual(len(regs), 58)

    def test_meter_values_physically_plausible(self):
        _, d = self._decode()
        self.assertAlmostEqual(d["frequency"], 50.0, places=2)
        for ph in ("a", "b", "c"):
            self.assertTrue(220 <= d[f"voltage_ln_{ph}"] <= 250, d[f"voltage_ln_{ph}"])
            self.assertTrue(0 <= d[f"current_{ph}"] <= 65)

    def test_power_triangle_consistent(self):
        # The whole point of the S/PF derivation fix: S == sqrt(P^2 + Q^2).
        _, d = self._decode()
        self.assertAlmostEqual(d["S_ratio_total"], 1.0, places=3)
        self.assertAlmostEqual(d["va_total"], math.hypot(d["power_total"], d["var_total"]), places=2)

    _REQ_ROW = "1,2026-01-01T00:00:00.000+00:00,1.0,10,request,1,3,40071,58,,,1,8,read,01 03 9c 87 00 3a 5b a0"

    def _run_parser(self, lines):
        import csv as _csv
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            inp = pathlib.Path(tmp) / "in.csv"
            outp = pathlib.Path(tmp) / "out.csv"
            inp.write_text("\n".join(lines) + "\n")
            parse_capture.main(str(inp), str(outp))
            return [r for r in _csv.DictReader(open(outp)) if r["block"] == "40071"]

    def test_response_paired_to_request_block_with_header(self):
        header = (
            "seq,wall_local,mono,delta_ms,direction,dev_id,fc,addr,count,bytecount,exception,crc_ok,n_bytes,summary,hex"
        )
        resp = f"2,2026-01-01T00:00:00.020+00:00,1.02,20,response,1,3,,,116,,1,121,resp,{self._FRAME_HEX}"
        decoded = self._run_parser([header, self._REQ_ROW, resp])
        self.assertEqual(len(decoded), 1)
        self.assertTrue(decoded[0]["power_total"])
        self.assertEqual(decoded[0]["S_ratio_total"], "1.0")

    def test_parses_capture_without_header_row(self):
        # Regression: real sniffer captures often have NO header row (the sniffer
        # only writes one when it creates a fresh file). The parser must decode
        # by column position, not treat the first data row as a header.
        resp = f"2,2026-01-01T00:00:00.020+00:00,1.02,20,response,1,3,,,116,,1,121,resp,{self._FRAME_HEX}"
        decoded = self._run_parser([self._REQ_ROW, resp])  # no header line
        self.assertEqual(len(decoded), 1)
        self.assertTrue(decoded[0]["power_total"])
        self.assertEqual(decoded[0]["S_ratio_total"], "1.0")


if __name__ == "__main__":
    unittest.main()
