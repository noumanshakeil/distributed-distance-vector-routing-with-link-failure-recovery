"""Tests for Hybrid CLI Controller (cli.py)."""

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable

import pytest

from dv_routing.launcher import launch_network
from dv_routing.router import INF

TOPOLOGIES_DIR = Path(__file__).resolve().parent.parent / "topologies"
DEFAULT_TOPO = str(TOPOLOGIES_DIR / "default.txt")


def wait_for(condition: Callable[[], bool], timeout: float = 4.0, poll_interval: float = 0.05) -> bool:
    """Helper to poll for a condition until timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(poll_interval)
    return False


def test_cli_help():
    """Verify cli.py top-level help lists all report-compliant subcommands."""
    res = subprocess.run([sys.executable, "cli.py", "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "send" in res.stdout
    assert "link-fail" in res.stdout
    assert "link-restore" in res.stdout
    assert "link-cost" in res.stdout
    assert "show" in res.stdout
    assert "tables" in res.stdout
    assert "interactive" in res.stdout


def test_cli_subcommand_helps():
    """Verify each subcommand displays expected arguments in help output."""
    # send
    res = subprocess.run([sys.executable, "cli.py", "send", "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "--src" in res.stdout
    assert "--dst" in res.stdout
    assert "--payload" in res.stdout
    assert "--hop-limit" in res.stdout
    assert "--base-port" in res.stdout

    # link-fail
    res = subprocess.run([sys.executable, "cli.py", "link-fail", "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "--endpoint1" in res.stdout
    assert "--endpoint2" in res.stdout
    assert "--base-port" in res.stdout

    # link-restore
    res = subprocess.run([sys.executable, "cli.py", "link-restore", "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "--endpoint1" in res.stdout
    assert "--endpoint2" in res.stdout
    assert "--base-port" in res.stdout

    # link-cost
    res = subprocess.run([sys.executable, "cli.py", "link-cost", "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "--endpoint1" in res.stdout
    assert "--endpoint2" in res.stdout
    assert "--cost" in res.stdout
    assert "--base-port" in res.stdout

    # show path
    res = subprocess.run([sys.executable, "cli.py", "show", "path", "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "src" in res.stdout
    assert "dst" in res.stdout
    assert "--base-port" in res.stdout

    # tables
    res = subprocess.run([sys.executable, "cli.py", "tables", "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "--base-port" in res.stdout

    # interactive
    res = subprocess.run([sys.executable, "cli.py", "interactive", "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "--mode" in res.stdout
    assert "--topology" in res.stdout
    assert "--interval" in res.stdout


def test_show_path_and_tables_against_running_network():
    """Verify show path and tables against live router processes."""
    base_port = 19310
    controller = launch_network(
        topology_file=DEFAULT_TOPO,
        interval=0.15,
        mitigation="split-horizon",
        base_port=base_port,
        mode="process",
    )
    try:
        # Wait for full convergence
        assert wait_for(
            lambda: (
                len(controller.query_all_tables(timeout=0.3)) == 6
                and (tbl_a := controller.query_table("A", timeout=0.3)) is not None
                and tbl_a.get("B", {}).get("cost") == 1
                and tbl_a.get("D", {}).get("cost") == 4
            ),
            timeout=5.0,
        )

        # Test tables command
        res_tbl = subprocess.run(
            [sys.executable, "cli.py", "tables", "--base-port", str(base_port)],
            capture_output=True,
            text=True,
        )
        assert res_tbl.returncode == 0
        assert "Router A" in res_tbl.stdout
        assert "Router B" in res_tbl.stdout
        # Verify no double printing: Router A header should occur once
        assert res_tbl.stdout.count("Router A") == 1

        # Test show path A B: direct link cost 1
        res_path_ab = subprocess.run(
            [sys.executable, "cli.py", "show", "path", "A", "B", "--base-port", str(base_port)],
            capture_output=True,
            text=True,
        )
        assert res_path_ab.returncode == 0
        assert "Path: A -> B (cost=1) | Total cost: 1" in res_path_ab.stdout

        # Test show path A D: multi-hop A -> B -> E -> D
        res_path_ad = subprocess.run(
            [sys.executable, "cli.py", "show", "path", "A", "D", "--base-port", str(base_port)],
            capture_output=True,
            text=True,
        )
        assert res_path_ad.returncode == 0
        assert "Path: A -> B (cost=1) -> E (cost=3) -> D (cost=4) | Total cost: 4" in res_path_ad.stdout
    finally:
        controller.stop()


def test_link_fail_restore_and_cost_against_running_network():
    """Verify link-fail, link-restore, and link-cost against live router processes."""
    base_port = 19320
    controller = launch_network(
        topology_file=DEFAULT_TOPO,
        interval=0.15,
        mitigation="split-horizon",
        base_port=base_port,
        mode="process",
    )
    try:
        # Wait for full convergence
        assert wait_for(
            lambda: (
                len(controller.query_all_tables(timeout=0.3)) == 6
                and (tbl_a := controller.query_table("A", timeout=0.3)) is not None
                and tbl_a.get("B", {}).get("cost") == 1
            ),
            timeout=5.0,
        )

        # 1. Fail link A-B
        res_fail = subprocess.run(
            [
                sys.executable,
                "cli.py",
                "link-fail",
                "--endpoint1",
                "A",
                "--endpoint2",
                "B",
                "--base-port",
                str(base_port),
            ],
            capture_output=True,
            text=True,
        )
        assert res_fail.returncode == 0

        # Wait for re-convergence via alternate path A -> F -> E -> B (cost=7)
        # Silence timeout = 3 * 0.15s = 0.45s
        assert wait_for(
            lambda: (
                (tbl_a := controller.query_table("A", timeout=0.3)) is not None
                and tbl_a.get("B", {}).get("cost") == 7
                and tbl_a.get("B", {}).get("next_hop") == "F"
            ),
            timeout=4.0,
        )

        # Verify show path reflects alternate path
        res_alt = subprocess.run(
            [sys.executable, "cli.py", "show", "path", "A", "B", "--base-port", str(base_port)],
            capture_output=True,
            text=True,
        )
        assert res_alt.returncode == 0
        assert "Path: A -> F (cost=4) -> E (cost=5) -> B (cost=7) | Total cost: 7" in res_alt.stdout

        # 2. Restore link A-B
        res_restore = subprocess.run(
            [
                sys.executable,
                "cli.py",
                "link-restore",
                "--endpoint1",
                "A",
                "--endpoint2",
                "B",
                "--base-port",
                str(base_port),
            ],
            capture_output=True,
            text=True,
        )
        assert res_restore.returncode == 0

        # Wait for re-convergence to direct path
        assert wait_for(
            lambda: (
                (tbl_a := controller.query_table("A", timeout=0.3)) is not None
                and tbl_a.get("B", {}).get("cost") == 1
                and tbl_a.get("B", {}).get("next_hop") == "B"
            ),
            timeout=4.0,
        )

        # 3. Change link cost of A-B to 10
        res_cost = subprocess.run(
            [
                sys.executable,
                "cli.py",
                "link-cost",
                "--endpoint1",
                "A",
                "--endpoint2",
                "B",
                "--cost",
                "10",
                "--base-port",
                str(base_port),
            ],
            capture_output=True,
            text=True,
        )
        assert res_cost.returncode == 0

        # Now path A to B should prefer A -> F -> E -> B (cost 7 < 10)
        assert wait_for(
            lambda: (
                (tbl_a := controller.query_table("A", timeout=0.3)) is not None
                and tbl_a.get("B", {}).get("cost") == 7
                and tbl_a.get("B", {}).get("next_hop") == "F"
            ),
            timeout=4.0,
        )
    finally:
        controller.stop()


def test_send_command_against_running_network():
    """Verify send packet injection via CLI against live router processes."""
    base_port = 19330
    controller = launch_network(
        topology_file=DEFAULT_TOPO,
        interval=0.15,
        mitigation="split-horizon",
        base_port=base_port,
        mode="process",
    )
    try:
        assert wait_for(
            lambda: (
                len(controller.query_all_tables(timeout=0.3)) == 6
                and (tbl_a := controller.query_table("A", timeout=0.3)) is not None
                and tbl_a.get("B", {}).get("cost") == 1
            ),
            timeout=5.0,
        )

        res_send = subprocess.run(
            [
                sys.executable,
                "cli.py",
                "send",
                "--src",
                "A",
                "--dst",
                "D",
                "--payload",
                "Hello D from A",
                "--base-port",
                str(base_port),
            ],
            capture_output=True,
            text=True,
        )
        assert res_send.returncode == 0
        assert "Sent" in res_send.stdout or "DATA" in res_send.stdout
    finally:
        controller.stop()


def test_interactive_session_sim_mode():
    """Verify interactive REPL runs in sim mode and processes interactive commands."""
    commands = "\n".join([
        "topo",
        "status",
        "tables",
        "path A B",
        "help",
        "quit",
    ]) + "\n"
    res = subprocess.run(
        [
            sys.executable,
            "cli.py",
            "interactive",
            "--mode",
            "sim",
            "--interval",
            "0.1",
        ],
        input=commands,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0
    assert "Distributed Distance-Vector Routing Simulator" in res.stdout
    assert "Router A" in res.stdout
    assert "Path: A -> B (cost=1) | Total cost: 1" in res.stdout
    assert "Goodbye." in res.stdout

