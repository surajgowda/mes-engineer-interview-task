#!/usr/bin/env python3
"""
ingest.py
---------
Layer 1: Ingest & Robust Parsing Layer.
Responsible for streaming raw text log files sequentially, handling rotation
boundaries, parsing pipe-delimited data tolerantly, and generating clean,
strongly typed ParsedRecord objects.
"""

import sys
import logging
from datetime import datetime
from typing import Generator, List, Optional

from models import ParsedRecord, RecordType

# Configure localized layer logger pointing to stderr
logger = logging.getLogger("LithoAdapter.Ingest")


class LogParser:
    """Handles the string manipulation, normalization, and tokenization of flat log lines."""
    
    def __init__(self):
        self.malformed_count = 0

    def parse_line(self, line: str) -> Optional[ParsedRecord]:
        """
        Parses a single line from the log file.
        
        Malformed Policy: If a line is structurally invalid (torn, bad timestamp, etc.),
        this method logs a warning, increments the malformed counter, and returns None.
        """
        clean_line = line.strip()
        
        # Rule 1: Handle blank lines and stray non-conforming lines (logrotate banners)
        if not clean_line:
            return None
        if clean_line.startswith("===") or "logrotate:" in clean_line:
            logger.info(f"Filtered out system/logrotate banner line: '{clean_line}'")
            return None

        try:
            # Rule 2: Validate base structural boundaries
            tokens = clean_line.split("|")
            if len(tokens) < 2:
                raise ValueError("Line lacks minimal structural pipe delimiters (|).")

            ts_str = tokens[0].strip()
            type_str = tokens[1].strip()

            # Rule 3: Robust ISO8601 Timestamp Conversion
            # Replaces 'Z' with explicit UTC offset representation to handle Python's fromisoformat
            if ts_str.endswith("Z"):
                ts_str = ts_str[:-1] + "+00:00"
            
            try:
                timestamp = datetime.fromisoformat(ts_str)
            except ValueError as val_err:
                raise ValueError(f"Invalid or truncated ISO8601 timestamp format '{tokens[0]}'.") from val_err

            # Rule 4: Validate Record Type matches our specification Enum
            try:
                record_type = RecordType(type_str)
            except ValueError as val_err:
                raise ValueError(f"Unknown or truncated record TYPE '{type_str}'.") from val_err

            # Rule 5: Tokenize order-independent key=value payload structures
            payload: dict[str, str] = {}
            for item in tokens[2:]:
                item_clean = item.strip()
                if not item_clean:
                    continue
                
                if "=" not in item_clean:
                    raise ValueError(f"Malformed payload field element missing '=' boundary: '{item_clean}'")
                
                k, v = item_clean.split("=", 1)
                payload[k.strip()] = v.strip()

            if record_type == RecordType.EVENT:
                valid_events = {
                    "LOT_START", "WAFER_START", "ALIGN_COMPLETE", 
                    "EXPOSE_COMPLETE", "WAFER_COMPLETE", "LOT_COMPLETE"
                }
                evt_val = payload.get("evt")
                if not evt_val or evt_val not in valid_events:
                    raise ValueError(f"Truncated or unrecognized lifecycle event value: '{evt_val}'")
            return ParsedRecord(timestamp=timestamp, record_type=record_type, payload=payload)
        except Exception as exc:
            self.malformed_count += 1
            # Write out context safely to stderr without blowing up the thread execution
            logger.warning(f"Malformed line skipped [Count: {self.malformed_count}]. Reason: {exc}. Line context: '{clean_line}'")
            return None


class LogStreamer:
    """Provides a continuous, sequentially unified stream across file rotation segments."""
    
    def __init__(self, file_paths: List[str], parser: Optional[LogParser] = None):
        self.file_paths = file_paths
        self.parser = parser or LogParser()

    def stream_records(self) -> Generator[ParsedRecord, None, None]:
        """
        Sequentially loops through files to process logs as a single continuous chronological stream.
        This guarantees that state contexts (such as LOT4472 spanning rotation files) 
        are preserved intact.
        """
        for path in self.file_paths:
            logger.info(f"Opening segment file for streaming: {path}")
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        record = self.parser.parse_line(line)
                        if record is not None:
                            yield record
            except FileNotFoundError:
                logger.error(f"Target log file segment not found: {path}")
                # Continue gracefully to next file in sequence or raise depending on deployment critical path
                continue