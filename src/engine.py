#!/usr/bin/env python3
"""
engine.py
---------
Layer 2: Canonical Stateful Engine Layer.
Maintains an equipment-agnostic, live structural model of the LithoStepper LS-200.
Translates a flat stream of events into a coherent, queryable, stateful state model.
"""

import logging
from typing import Optional
from models import ParsedRecord, CanonicalState, RecordType

# Configure localized engine logger pointing to stderr
logger = logging.getLogger("LithoAdapter.Engine")


class EquipmentStateMachine:
    """
    The stateful brain of the adapter. Mutates a single internal CanonicalState 
    instance incrementally based on incoming event data.
    """

    def __init__(self, parser_ref=None):
        self.state = CanonicalState()
        # Maintain a reference to the parser to sync malformed line counters if available
        self._parser_ref = parser_ref

    def handle_record(self, record: ParsedRecord) -> CanonicalState:
        """
        Ingests a single ParsedRecord and mutates the canonical state.
        
        Returns:
            CanonicalState: The updated global state snapshot (read-only to projections).
        """
        # Sync metrics and updates
        self.state.total_processed_lines += 1
        self.state.last_updated_at = record.timestamp
        if self._parser_ref:
            self.state.malformed_lines_count = self._parser_ref.malformed_count

        # Route processing rules based on log line type
        if record.record_type == RecordType.STATE:
            self._process_state_transition(record)
        elif record.record_type == RecordType.ALARM:
            self._process_alarm(record)
        elif record.record_type == RecordType.EVENT:
            self._process_lifecycle_event(record)
        elif record.record_type == RecordType.TELEM:
            self._process_telemetry(record)

        return self.state

    def _process_state_transition(self, record: ParsedRecord):
        """Processes SEMI E10 equipment state movements."""
        p = record.payload
        new_state = p.get("equipment_state")
        prev_state = p.get("prev")

        if new_state:
            # Safely capture previous state, defaulting to what we have if the log has anomalies
            self.state.previous_state = prev_state if prev_state else self.state.current_state
            self.state.current_state = new_state
            self.state.state_changed_at = record.timestamp
            logger.info(f"State Shift: {self.state.previous_state} -> {self.state.current_state}")

    def _process_alarm(self, record: ParsedRecord):
        """Tracks active alarms across the SET and CLEAR time-gap window."""
        p = record.payload
        alid = p.get("alid")
        alarm_state = p.get("state")  # "SET" or "CLEAR"

        if not alid:
            logger.warning(f"Ignored ALARM record missing vital 'alid' identifier key at {record.timestamp}")
            return

        if alarm_state == "SET":
            # Add or overwrite active alarm entry
            self.state.active_alarms[alid] = {
                "severity": p.get("sev", "UNKNOWN"),
                "text": p.get("text", "No descriptive text provided."),
                "set_at": record.timestamp
            }
            logger.warning(f"Alarm SET: [ALID {alid}] {p.get('text')} ({p.get('sev')})")
        
        elif alarm_state == "CLEAR":
            # Remove the alarm from active map seamlessly
            removed = self.state.active_alarms.pop(alid, None)
            if removed:
                logger.info(f"Alarm CLEAR: [ALID {alid}] resolved after "
                            f"{(record.timestamp - removed['set_at']).total_seconds()}s")
            else:
                logger.debug(f"Received CLEAR for untracked ALID {alid} at {record.timestamp}")

    def _process_lifecycle_event(self, record: ParsedRecord):
        """Drives the production wafer & lot lifecycle context engine."""
        p = record.payload
        evt = p.get("evt")

        if not evt:
            return

        if evt == "LOT_START":
            self.state.current_lot = p.get("lot")
            self.state.recipe = p.get("recipe")
            self.state.reticle = p.get("reticle")
            
            # Extract expected wafers count safely
            try:
                self.state.expected_wafers_in_lot = int(p.get("wafers", "0"))
            except ValueError:
                self.state.expected_wafers_in_lot = None

        elif evt == "WAFER_START":
            # State continuity check: handles lot assignment spanning multi-file boundaries seamlessly
            self.state.current_lot = p.get("lot", self.state.current_lot)
            self.state.current_wafer = p.get("wafer")
            self.state.current_slot = p.get("slot")

        elif evt == "ALIGN_COMPLETE":
            try:
                self.state.latest_alignment_residual_nm = float(p.get("align_resid_nm", "0.0"))
            except ValueError:
                pass

        elif evt == "EXPOSE_COMPLETE":
            try:
                self.state.latest_exposure_fields = int(p.get("fields", "0"))
                self.state.latest_exposure_dose_mj_cm2 = float(p.get("dose_mj_cm2", "0.0"))
            except ValueError:
                pass

        elif evt == "WAFER_COMPLETE":
            # Clear individual wafer context upon cycle completion
            self.state.current_wafer = None
            self.state.current_slot = None

        elif evt == "LOT_COMPLETE":
            # Cleanly flush out batch context upon lot finalization
            self.state.current_lot = None
            self.state.current_wafer = None
            self.state.current_slot = None
            self.state.recipe = None
            self.state.reticle = None
            self.state.expected_wafers_in_lot = None

    def _process_telemetry(self, record: ParsedRecord):
        """Updates high-frequency telemetry registers dynamically."""
        # Loop through order-independent elements mapping floats to our cache matrix
        for key, val_str in record.payload.items():
            # If a new unknown metric appears in a future tool update, it scales dynamically
            try:
                self.state.telemetry[key] = float(val_str)
            except ValueError:
                # If a field is present but corrupted (e.g. string text inside an float field), skip mutation
                logger.warning(f"Failed to parse telemetry value for key '{key}': '{val_str}'")
                pass