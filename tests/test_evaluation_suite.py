"""Tests for demos and automated evaluation suite."""

import csv
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_count_to_infinity_mitigation_poison_reverse():
    """Verify --mitigation poison-reverse runs poison reverse scenario."""
    res = subprocess.run(
        [sys.executable, str(ROOT / "demos" / "run_count_to_infinity.py"), "--mitigation", "poison-reverse", "--watch", "0.5"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert res.returncode == 0, f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}"
    assert "poison reverse" in res.stdout.lower()


def test_count_to_infinity_mitigation_none():
    """Verify --mitigation none runs unmitigated scenario."""
    res = subprocess.run(
        [sys.executable, str(ROOT / "demos" / "run_count_to_infinity.py"), "--mitigation", "none", "--watch", "0.5"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert res.returncode == 0, f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}"
    assert "without split horizon" in res.stdout.lower() or "scenario: without" in res.stdout.lower()


def test_count_to_infinity_mitigation_split_horizon():
    """Verify --mitigation split-horizon runs split horizon scenario."""
    res = subprocess.run(
        [sys.executable, str(ROOT / "demos" / "run_count_to_infinity.py"), "--mitigation", "split-horizon", "--watch", "0.5"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert res.returncode == 0, f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}"
    assert "split horizon" in res.stdout.lower()


def test_count_to_infinity_backward_compatibility():
    """Verify legacy --only flag remains supported."""
    res = subprocess.run(
        [sys.executable, str(ROOT / "demos" / "run_count_to_infinity.py"), "--only", "without", "--watch", "0.5"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert res.returncode == 0, f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}"
    assert "without" in res.stdout.lower()


def test_failure_recovery_demo_sim_mode():
    """Verify run_failure_recovery.py supports --mode sim."""
    res = subprocess.run(
        [sys.executable, str(ROOT / "demos" / "run_failure_recovery.py"), "--mode", "sim"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert res.returncode == 0, f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}"
    assert "=== BEFORE FAILURE" in res.stdout
    assert "=== AFTER RECOVERY" in res.stdout


def test_evaluation_suite_generates_all_8_csv_logs(tmp_path):
    """Verify run_evaluation_suite.py generates T1-T8 CSV logs with valid headers and data."""
    res = subprocess.run(
        [
            sys.executable,
            str(ROOT / "demos" / "run_evaluation_suite.py"),
            "--output-dir",
            str(tmp_path),
            "--trials",
            "1",
            "--fast",
        ],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert res.returncode == 0, f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}"

    expected_schemas = {
        "t1_baseline.csv": [
            "trial", "timestamp", "router", "dest", "next_hop", "cost", "converged_time_sec"
        ],
        "t2_failure.csv": [
            "trial", "timestamp", "event", "route_a_to_b_cost", "route_a_to_b_next_hop", "reroute_time_sec"
        ],
        "t3_loop.csv": [
            "trial", "step", "time_offset_sec", "cost_a_to_c", "cost_b_to_c"
        ],
        "t4_poison.csv": [
            "trial", "step", "time_offset_sec", "cost_a_to_c", "cost_b_to_c", "loop_stopped"
        ],
        "t5_split.csv": [
            "trial", "step", "time_offset_sec", "cost_a_to_c", "cost_b_to_c", "loop_stopped"
        ],
        "t6_sweep.csv": [
            "trial", "interval_sec", "convergence_time_sec", "message_count"
        ],
        "t7_cost_change.csv": [
            "trial", "old_cost", "new_cost", "old_route", "new_route", "adaptation_time_sec"
        ],
        "t8_transient.csv": [
            "trial", "cycle", "fail_reconverged", "restore_reconverged", "time_sec"
        ],
    }

    for filename, expected_cols in expected_schemas.items():
        csv_file = tmp_path / filename
        assert csv_file.exists(), f"Expected CSV file {filename} was not generated."
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert len(rows) >= 2, f"{filename} should contain header and at least one data row, got {len(rows)} lines."
        header = [col.strip() for col in rows[0]]
        assert header == expected_cols, f"{filename} header mismatch: expected {expected_cols}, got {header}"
