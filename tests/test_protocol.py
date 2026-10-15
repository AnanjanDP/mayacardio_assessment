"""
Unit tests for protocol parser, framing, and CRC32 validation.
"""

import json
import zlib
from heartbeat.protocol import (
    calculate_crc32,
    extract_data_substring,
    parse_response,
)


def test_extract_data_substring_simple():
    raw = '{"session_id": "abc", "data": [[1, 2, 3], [4, 5, 6]], "crc32": "12345678"}'
    substr = extract_data_substring(raw)
    assert substr == "[[1, 2, 3], [4, 5, 6]]"


def test_extract_data_substring_nested():
    raw = '{"session_id": "test", "data": [[10, 20], [30, [40, 50]]], "sensor": 1}'
    substr = extract_data_substring(raw)
    assert substr == "[[10, 20], [30, [40, 50]]]"


def test_crc32_calculation():
    text = "[[720,1200,1560],[708,1180,1534],[726,1210,1573]]"
    expected_crc = f"{zlib.crc32(text.encode('utf-8')) & 0xFFFFFFFF:08x}"
    assert calculate_crc32(text) == expected_crc


def test_parse_valid_response():
    data = [[720, 1200, 1560], [708, 1180, 1534]]
    data_text = json.dumps(data, separators=(",", ":"))
    crc_str = calculate_crc32(data_text)

    line = json.dumps({
        "session_id": "5f0c2a9e",
        "sensor": 1,
        "first_index": 120000,
        "count": 2,
        "lost": [],
        "session_end": False,
        "crc32": crc_str,
        "data": data,
    }, separators=(",", ":"))

    resp, err = parse_response(line)
    assert err is None
    assert resp is not None
    assert resp.session_id == "5f0c2a9e"
    assert resp.sensor == 1
    assert resp.first_index == 120000
    assert resp.count == 2
    assert resp.data == data
    assert resp.session_end is False


def test_parse_corrupt_crc():
    data = [[720, 1200, 1560]]
    line = json.dumps({
        "session_id": "5f0c2a9e",
        "sensor": 1,
        "first_index": 120000,
        "count": 1,
        "lost": [],
        "session_end": False,
        "crc32": "deadbeef",
        "data": data,
    }, separators=(",", ":"))

    resp, err = parse_response(line)
    assert resp is None
    assert err == "corrupt_crc32_mismatch"


def test_parse_malformed_json():
    line = '{"session_id": "bad json'
    resp, err = parse_response(line)
    assert resp is None
    assert "json_decode_error" in err


def test_parse_missing_fields():
    line = json.dumps({"session_id": "123", "sensor": 1})
    resp, err = parse_response(line)
    assert resp is None
    assert "missing_field" in err


def test_parse_count_mismatch():
    data = [[1, 2, 3]]
    data_text = json.dumps(data, separators=(",", ":"))
    crc_str = calculate_crc32(data_text)

    line = json.dumps({
        "session_id": "sess1",
        "sensor": 1,
        "first_index": 0,
        "count": 5,  # Mismatch: declares 5, gives 1
        "lost": [],
        "session_end": False,
        "crc32": crc_str,
        "data": data,
    }, separators=(",", ":"))

    resp, err = parse_response(line)
    assert resp is None
    assert "count_mismatch" in err


def test_parse_channel_dimensions():
    # Sensor 1 requires 3 channels
    data = [[1, 2]]  # Only 2 channels
    data_text = json.dumps(data, separators=(",", ":"))
    crc_str = calculate_crc32(data_text)

    line = json.dumps({
        "session_id": "sess1",
        "sensor": 1,
        "first_index": 0,
        "count": 1,
        "lost": [],
        "session_end": False,
        "crc32": crc_str,
        "data": data,
    }, separators=(",", ":"))

    resp, err = parse_response(line)
    assert resp is None
    assert "sample_channels_mismatch" in err
