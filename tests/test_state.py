#!/usr/bin/env python3
"""
test_state.py
-------------
Integration testing framework verifying Layer 2 (State Engine) stateful logic.
Ensures perfect state tracking, active alarm lifecycles, and cross-file lot continuity.
"""

import unittest
from datetime import datetime, timezone
from models import ParsedRecord, RecordType
from engine import EquipmentStateMachine


class TestEquipmentStateMachine(unittest.TestCase):
    """Stateful integration verification suite for the Canonical Engine."""

    def setUp(self):
        self.engine = EquipmentStateMachine()

    def test_e10_state_transitions(self):
        """Assert that SEMI E10 equipment state tracking maintains proper histories."""
        # Initial default state bounds
        self.assertEqual(self.engine.state.current_state, "NON_SCHEDULED")

        # First shift
        rec1 = ParsedRecord(
            timestamp=datetime(2026, 5, 20, 8, 0, 0, tzinfo=timezone.utc),
            record_type=RecordType.STATE,
            payload={"equipment_state": "STANDBY", "prev": "NON_SCHEDULED"}
        )
        state = self.engine.handle_record(rec1)
        self.assertEqual(state.current_state, "STANDBY")
        self.assertEqual(state.previous_state, "NON_SCHEDULED")

        # Subsequent production shift
        rec2 = ParsedRecord(
            timestamp=datetime(2026, 5, 20, 8, 0, 8, tzinfo=timezone.utc),
            record_type=RecordType.STATE,
            payload={"equipment_state": "PRODUCTIVE", "prev": "STANDBY"}
        )
        state = self.engine.handle_record(rec2)
        self.assertEqual(state.current_state, "PRODUCTIVE")
        self.assertEqual(state.previous_state, "STANDBY")

    def test_active_alarm_lifecycle(self):
        """Verify alarms populate the active dictionary map and clear out seamlessly."""
        # 1. Trigger an alarm
        rec_set = ParsedRecord(
            timestamp=datetime(2026, 5, 20, 8, 0, 27, tzinfo=timezone.utc),
            record_type=RecordType.ALARM,
            payload={"alid": "2107", "state": "SET", "sev": "MAJOR", "text": "Focus out of spec"}
        )
        state = self.engine.handle_record(rec_set)
        
        self.assertIn("2107", state.active_alarms)
        self.assertEqual(state.active_alarms["2107"]["severity"], "MAJOR")
        self.assertEqual(state.active_alarms["2107"]["text"], "Focus out of spec")

        # 2. Clear the alarm
        rec_clear = ParsedRecord(
            timestamp=datetime(2026, 5, 20, 8, 1, 12, tzinfo=timezone.utc),
            record_type=RecordType.ALARM,
            payload={"alid": "2107", "state": "CLEAR"}
        )
        state = self.engine.handle_record(rec_clear)
        
        # Must be seamlessly flushed out of the active snapshot map
        self.assertNotIn("2107", state.active_alarms)

    def test_cross_file_lot_continuity(self):
        """Simulate a log rotation point to guarantee batch state context is retained."""
        # Step A: Lot starts processing in part1.log context
        rec_start = ParsedRecord(
            timestamp=datetime(2026, 5, 20, 8, 1, 38, tzinfo=timezone.utc),
            record_type=RecordType.EVENT,
            payload={"evt": "LOT_START", "lot": "LOT4472", "recipe": "METAL1", "wafers": "5"}
        )
        self.engine.handle_record(rec_start)
        
        rec_w1 = ParsedRecord(
            timestamp=datetime(2026, 5, 20, 8, 1, 40, tzinfo=timezone.utc),
            record_type=RecordType.EVENT,
            payload={"evt": "WAFER_START", "lot": "LOT4472", "wafer": "01", "slot": "1"}
        )
        state = self.engine.handle_record(rec_w1)
        self.assertEqual(state.current_lot, "LOT4472")
        self.assertEqual(state.current_wafer, "01")

        # --- SIMULATE LOG ROTATION BOUNDARY ---
        # Part 1 ends. A new file segment (part2.log) opens up.
        # The engine must retain internal data parameters in memory cleanly.

        rec_w3 = ParsedRecord(
            timestamp=datetime(2026, 5, 20, 8, 1, 54, tzinfo=timezone.utc),
            record_type=RecordType.EVENT,
            payload={"evt": "WAFER_START", "lot": "LOT4472", "wafer": "03", "slot": "3"}
        )
        state = self.engine.handle_record(rec_w3)
        
        # Verify context mapping survived the boundary gap intact
        self.assertEqual(state.current_lot, "LOT4472")
        self.assertEqual(state.recipe, "METAL1")
        self.assertEqual(state.current_wafer, "03")
        self.assertEqual(state.current_slot, "3")

    def test_telemetry_cache_mutation(self):
        """Verify telemetry parameter mapping mutates cleanly without key-leakage."""
        rec_tel = ParsedRecord(
            timestamp=datetime(2026, 5, 20, 8, 1, 40, tzinfo=timezone.utc),
            record_type=RecordType.TELEM,
            payload={"stage_temp_c": "22.32", "chuck_vac_kpa": "88.3", "focus_nm": "2"}
        )
        state = self.engine.handle_record(rec_tel)
        
        self.assertEqual(state.telemetry["stage_temp_c"], 22.32)
        self.assertEqual(state.telemetry["chuck_vac_kpa"], 88.3)
        self.assertEqual(state.telemetry["focus_nm"], 2.0)


if __name__ == "__main__":
    unittest.main()