"""
Protocol parser, framing, and CRC32 validation for biosignal sensor mock stream.
"""

import json
import re
import zlib
from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class LossItem:
    from_index: int
    to_index: Optional[int]
    reason: str


@dataclass
class SensorResponse:
    session_id: str
    sensor: int
    first_index: int
    count: int
    lost: List[LossItem]
    session_end: bool
    crc32: str
    data: List[List[int]]
    raw_line: str


def extract_data_substring(raw_text: str) -> Optional[str]:
    """
    Extracts the exact serialized data text from the opening '[' to the matching ']'
    following the 'data' key in raw_text.
    """
    # Locate "data" key
    data_key_match = re.search(r'"data"\s*:', raw_text)
    if not data_key_match:
        return None

    start_search = data_key_match.end()
    open_bracket_idx = raw_text.find("[", start_search)
    if open_bracket_idx == -1:
        return None

    # Track nested brackets
    depth = 0
    in_string = False
    escape = False

    for i in range(open_bracket_idx, len(raw_text)):
        char = raw_text[i]
        if escape:
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue

        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return raw_text[open_bracket_idx : i + 1]

    return None


def calculate_crc32(text: str) -> str:
    """Calculates CRC-32 (zlib) as an 8-character lowercase hex string."""
    crc = zlib.crc32(text.encode("utf-8")) & 0xFFFFFFFF
    return f"{crc:08x}"


def validate_crc32(raw_line: str, expected_crc32: str) -> bool:
    """
    Validates that the CRC32 of the exact serialized data text matches expected_crc32.
    """
    data_substr = extract_data_substring(raw_line)
    if data_substr is None:
        return False
    computed_crc = calculate_crc32(data_substr)
    return computed_crc.lower() == expected_crc32.strip().lower()


def parse_response(raw_line: str) -> Tuple[Optional[SensorResponse], Optional[str]]:
    """
    Parses a newline-delimited JSON line into a validated SensorResponse.
    Returns (response, error_message).
    """
    clean_line = raw_line.strip()
    if not clean_line:
        return None, "empty_line"

    try:
        payload = json.loads(clean_line)
    except json.JSONDecodeError as e:
        return None, f"json_decode_error: {e}"

    if not isinstance(payload, dict):
        return None, "payload_not_object"

    # Required fields
    required_keys = ["session_id", "sensor", "first_index", "count", "lost", "session_end", "crc32", "data"]
    for k in required_keys:
        if k not in payload:
            return None, f"missing_field_{k}"

    session_id = payload["session_id"]
    if not isinstance(session_id, str) or not session_id:
        return None, "invalid_session_id"

    sensor = payload["sensor"]
    if sensor not in (1, 2):
        return None, f"invalid_sensor_id_{sensor}"

    first_index = payload["first_index"]
    if not isinstance(first_index, int) or first_index < 0:
        return None, "invalid_first_index"

    count = payload["count"]
    if not isinstance(count, int) or count < 0:
        return None, "invalid_count"

    session_end = payload["session_end"]
    if not isinstance(session_end, bool):
        return None, "invalid_session_end"

    crc32_str = payload["crc32"]
    if not isinstance(crc32_str, str):
        return None, "invalid_crc32_format"

    data = payload["data"]
    if not isinstance(data, list):
        return None, "invalid_data_not_list"

    if len(data) != count:
        return None, f"count_mismatch: declared {count}, actual {len(data)}"

    # Validate channel dimensions
    expected_channels = 3 if sensor == 1 else 2
    for idx_offset, sample in enumerate(data):
        if not isinstance(sample, list) or len(sample) != expected_channels:
            return None, f"sample_channels_mismatch at offset {idx_offset}"
        for val in sample:
            if not isinstance(val, int):
                return None, f"sample_value_not_int at offset {idx_offset}"

    # CRC32 validation
    if not validate_crc32(clean_line, crc32_str):
        return None, "corrupt_crc32_mismatch"

    # Parse loss items
    lost_raw = payload["lost"]
    if not isinstance(lost_raw, list):
        return None, "invalid_lost_not_list"

    lost_items = []
    for item in lost_raw:
        if isinstance(item, dict) and "from_index" in item:
            f_idx = item.get("from_index")
            t_idx = item.get("to_index")
            reason = item.get("reason", "unknown")
            lost_items.append(LossItem(from_index=f_idx, to_index=t_idx, reason=reason))

    response = SensorResponse(
        session_id=session_id,
        sensor=sensor,
        first_index=first_index,
        count=count,
        lost=lost_items,
        session_end=session_end,
        crc32=crc32_str,
        data=data,
        raw_line=clean_line,
    )
    return response, None
