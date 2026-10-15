"""
check_ground_truth: Independent validation tool for pipeline exports and results.
Compares device/cloud exports and processing results against mathematical ground truth.
"""

import argparse
import json
import sys
from typing import Dict, List, Optional, Set, Tuple

SAMPLE_RATE = 400


def ramp_sensor1(i: int) -> List[int]:
    return [(3 * i + c) % 8388608 for c in range(3)]


def ramp_sensor2(i: int) -> List[int]:
    return [(2 * i + c) % 8388608 for c in range(2)]


def check_export(export_file: str, mode: str = "ramp") -> Tuple[bool, dict]:
    """
    Validates exported sample and loss records from device or cloud.
    Returns (success, summary_dict).
    """
    samples_s1: Dict[int, List[int]] = {}
    samples_s2: Dict[int, List[int]] = {}
    loss_records: List[dict] = []
    duplicate_records: List[Tuple[int, int]] = []
    value_mismatches: List[Tuple[int, int]] = []

    with open(export_file, "r", encoding="utf-8") as f:
        line_num = 0
        for line in f:
            line_num += 1
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception as e:
                print(f"Error parsing JSON on line {line_num}: {e}")
                return False, {"error": f"JSON parse error at line {line_num}"}

            if "values" in rec:
                # Sample record: {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
                sensor = rec["sensor"]
                idx = rec["index"]
                vals = rec["values"]

                target_dict = samples_s1 if sensor == 1 else samples_s2
                if idx in target_dict:
                    duplicate_records.append((sensor, idx))
                else:
                    target_dict[idx] = vals

                # If ramp mode, verify value integrity
                if mode == "ramp":
                    expected = ramp_sensor1(idx) if sensor == 1 else ramp_sensor2(idx)
                    if vals != expected:
                        value_mismatches.append((sensor, idx))
            elif "from_index" in rec:
                # Loss record: {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}
                loss_records.append(rec)
            else:
                print(f"Unknown record type at line {line_num}: {line}")
                return False, {"error": f"Unknown record type at line {line_num}"}

    # Verify no overlap between present samples and loss records
    loss_overlaps = []
    lost_indices_s1: Set[int] = set()
    lost_indices_s2: Set[int] = set()

    for lr in loss_records:
        s = lr["sensor"]
        f_idx = lr["from_index"]
        t_idx = lr["to_index"]
        if t_idx is not None:
            target_set = lost_indices_s1 if s == 1 else lost_indices_s2
            present_dict = samples_s1 if s == 1 else samples_s2
            for i in range(f_idx, t_idx + 1):
                target_set.add(i)
                if i in present_dict:
                    loss_overlaps.append((s, i))

    # Detect unaccounted gaps
    unaccounted_gaps_s1 = []
    unaccounted_gaps_s2 = []
    if samples_s1:
        max_s1 = max(samples_s1.keys())
        for i in range(max_s1):
            if i not in samples_s1 and i not in lost_indices_s1:
                unaccounted_gaps_s1.append(i)

    if samples_s2:
        max_s2 = max(samples_s2.keys())
        for i in range(max_s2):
            if i not in samples_s2 and i not in lost_indices_s2:
                unaccounted_gaps_s2.append(i)

    summary = {
        "samples_sensor1": len(samples_s1),
        "samples_sensor2": len(samples_s2),
        "loss_records": len(loss_records),
        "duplicates": len(duplicate_records),
        "value_mismatches": len(value_mismatches),
        "loss_overlaps": len(loss_overlaps),
        "unaccounted_gaps_s1": len(unaccounted_gaps_s1),
        "unaccounted_gaps_s2": len(unaccounted_gaps_s2),
    }

    success = (
        len(duplicate_records) == 0
        and len(value_mismatches) == 0
        and len(loss_overlaps) == 0
        and len(unaccounted_gaps_s1) == 0
        and len(unaccounted_gaps_s2) == 0
    )

    return success, summary


def check_results(results_file: str, raw_export_file: Optional[str] = None) -> Tuple[bool, dict]:
    """Validates processing results against requirements."""
    results = []
    with open(results_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                results.append(json.loads(line))

    invalid_windows = []
    for r in results:
        w = r.get("window")
        mode = r.get("mode")
        if mode not in ("online", "offline"):
            invalid_windows.append({"window": w, "reason": f"invalid mode {mode}"})
            continue

        # Check required fields
        for field in ("missing_1", "missing_2", "complete", "channels", "magnitude", "heart_rate", "mode"):
            if field not in r:
                invalid_windows.append({"window": w, "reason": f"missing field {field}"})
                break

    success = len(invalid_windows) == 0
    return success, {"total_results": len(results), "invalid": len(invalid_windows), "details": invalid_windows[:5]}


def main():
    parser = argparse.ArgumentParser(description="Ground Truth Validation Tool")
    parser.add_argument("--export", help="Path to exported JSON Lines file")
    parser.add_argument("--results", help="Path to results JSON Lines file")
    parser.add_argument("--mode", choices=["ramp", "signal"], default="ramp", help="Mode for checking values")

    args = parser.parse_args()

    all_passed = True
    if args.export:
        print(f"Validating export: {args.export} (mode={args.mode})")
        passed, summary = check_export(args.export, mode=args.mode)
        print(json.dumps(summary, indent=2))
        if passed:
            print("[PASS] Ground truth export verification passed!")
        else:
            print("[FAIL] Ground truth export verification failed!")
            all_passed = False

    if args.results:
        print(f"Validating results: {args.results}")
        passed, summary = check_results(args.results)
        print(json.dumps(summary, indent=2))
        if passed:
            print("[PASS] Results validation passed!")
        else:
            print("[FAIL] Results validation failed!")
            all_passed = False

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
