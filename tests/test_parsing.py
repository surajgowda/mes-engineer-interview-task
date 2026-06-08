#!/usr/bin/env python3
"""
test_parsing.py
---------------
Unit testing framework verifying Layer 1 (Parser) structural robustness bounds.
Ensures resilience against scrambled keys, missing fields, system banners, and torn lines.
"""

import unittest
from datetime import datetime, timezone
from models import RecordType
from ingest import LogParser


class TestLogParserRobustness(unittest.TestCase):
    """Deep structural validation suite matching real-world log wrinkles."""

    def setUp(self):
        self.parser = LogParser()

    def test_parse_perfect_state_line(self):
        """Verify standard STATE log transitions parse into clean typed envelopes."""
        line = "2026-05-20T08:00:00.000Z|STATE|equipment_state=STANDBY|prev=NON_SCHEDULED"
        record = self.parser.parse_line(line)
        
        self.assertIsNotNone(record)
        self.assertEqual(record.record_type, RecordType.STATE)
        self.assertEqual(record.timestamp, datetime(2026, 5, 20, 8, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(record.payload["equipment_state"], "STANDBY")
        self.assertEqual(record.payload["prev"], "NON_SCHEDULED")
        self.assertEqual(self.parser.malformed_count, 0)

    def test_parse_scrambled_order_telem_line(self):
        """Verify fields are decoded correctly when key order shifts randomly."""
        line = "2026-05-20T08:00:18.201Z|TELEM|laser_mj=15.17|focus_nm=-2|stage_temp_c=22.39|illum_pct=99.7|chuck_vac_kpa=87.6"
        record = self.parser.parse_line(line)
        
        self.assertIsNotNone(record)
        self.assertEqual(record.record_type, RecordType.TELEM)
        # Ensure elements are mapped properly regardless of index permutation
        self.assertEqual(record.payload["laser_mj"], "15.17")
        self.assertEqual(record.payload["focus_nm"], "-2")
        self.assertEqual(record.payload["stage_temp_c"], "22.39")
        self.assertEqual(self.parser.malformed_count, 0)

    def test_parse_missing_key_telem_line(self):
        """Verify lines dropping expected variables are still cleanly packaged by Layer 1."""
        line = "2026-05-20T08:02:04.343Z|TELEM|stage_temp_c=22.25|chuck_vac_kpa=88.4|laser_mj=15.05|illum_pct=99.4"
        record = self.parser.parse_line(line)
        
        self.assertIsNotNone(record)
        self.assertNotIn("focus_nm", record.payload)
        self.assertEqual(record.payload["stage_temp_c"], "22.25")
        self.assertEqual(self.parser.malformed_count, 0)

    def test_ignore_logrotate_banners(self):
        """Verify structural lines injected by standard OS log rotators drop out silently."""
        banner_line = "==== logrotate: reopened LS-200.log ===="
        record = self.parser.parse_line(banner_line)
        
        # FIX HERE: Changed from assertNone to assertIsNone
        self.assertIsNone(record)
        self.assertEqual(self.parser.malformed_count, 0)

    def test_quarantine_torn_terminal_line(self):
        """Verify an split, truncated terminal fragment drops into quarantine."""
        torn_line = "2026-05-20T08:01:52|EVENT|evt=WAFER_STAR"
        record = self.parser.parse_line(torn_line)
        
        # FIX HERE: Changed from assertNone to assertIsNone
        self.assertIsNone(record)
        self.assertEqual(self.parser.malformed_count, 1)

    def test_quarantine_corrupted_timestamp(self):
        """Verify lines with mangled envelopes trigger immediate recovery routines."""
        bad_ts_line = "2026-05-XX-INVALID|STATE|equipment_state=STANDBY"
        record = self.parser.parse_line(bad_ts_line)
        
        # FIX HERE: Changed from assertNone to assertIsNone
        self.assertIsNone(record)
        self.assertEqual(self.parser.malformed_count, 1)


if __name__ == "__main__":
    unittest.main()