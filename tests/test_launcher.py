"""Tests for multi-process NetworkLauncherController and launch_network."""

import subprocess
import sys
import time
from pathlib import Path
from typing import Callable, Dict

import pytest
from dv_routing.launcher import NetworkLauncherController, launch_network
from dv_routing.router import INF
from dv_routing.topology import load_topology

TOPOLOGIES_DIR = Path(__file__).resolve().parent.parent / "topologies"
TRIANGLE_TOPO = str(TOPOLOGIES_DIR / "triangle.txt")
DEFAULT_TOPO = str(TOPOLOGIES_DIR / "default.txt")


def wait_for(condition: Callable[[], bool], timeout: float = 4.0, poll_interval: float = 0.05) -> bool:
    """Helper to poll for a condition until timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(poll_interval)
    return False


def test_port_assignment():
    """Verify deterministic port assignment from base_port."""
    topo_default = load_topology(DEFAULT_TOPO)
    controller = NetworkLauncherController(topo_default, base_port=10001)
    # Default 6-router mesh: A to F mapped to 10001-10006
    assert controller.router_ports["A"] == 10001
    assert controller.router_ports["B"] == 10002
    assert controller.router_ports["C"] == 10003
    assert controller.router_ports["D"] == 10004
    assert controller.router_ports["E"] == 10005
    assert controller.router_ports["F"] == 10006

    topo_triangle = load_topology(TRIANGLE_TOPO)
    controller_tri = NetworkLauncherController(topo_triangle, base_port=19050)
    assert controller_tri.router_ports["X"] == 19050
    assert controller_tri.router_ports["Y"] == 19051
    assert controller_tri.router_ports["Z"] == 19052


def test_launch_network_starts_processes_and_converges():
    """Verify launch_network starts router processes, queries tables, and verifies convergence."""
    controller = launch_network(
        topology_file=TRIANGLE_TOPO,
        interval=0.15,
        mitigation="split-horizon",
        base_port=19210,
        mode="process",
    )
    try:
        assert controller.is_running() is True

        # Wait for all 3 routers to start and fully converge
        converged = wait_for(
            lambda: (
                len(controller.query_all_tables(timeout=0.3)) == 3
                and (tbl_x := controller.query_table("X", timeout=0.3)) is not None
                and tbl_x.get("Z", {}).get("cost") == 3
                and (tbl_z := controller.query_table("Z", timeout=0.3)) is not None
                and tbl_z.get("X", {}).get("cost") == 3
            ),
            timeout=4.0,
        )
        assert converged is True

        tables = controller.query_all_tables(timeout=0.5)
        assert set(tables.keys()) == {"X", "Y", "Z"}

        # X -> Z should be cost 3 via Y (1 + 2 = 3 < direct 4)
        assert tables["X"]["Z"]["cost"] == 3
        assert tables["X"]["Z"]["next_hop"] == "Y"

        # Z -> X should be cost 3 via Y (2 + 1 = 3 < direct 4)
        assert tables["Z"]["X"]["cost"] == 3
        assert tables["Z"]["X"]["next_hop"] == "Y"

        # Y -> X is direct 1, Y -> Z is direct 2
        assert tables["Y"]["X"]["cost"] == 1
        assert tables["Y"]["Z"]["cost"] == 2
    finally:
        controller.stop()
        assert controller.is_running() is False


def test_fail_link_and_restore_link_via_controller():
    """Verify fail_link and restore_link update routes dynamically."""
    controller = launch_network(
        topology_file=TRIANGLE_TOPO,
        interval=0.15,
        mitigation="split-horizon",
        base_port=19220,
        mode="process",
    )
    try:
        # Wait for initial convergence: X -> Y cost 1, X -> Z cost 3 via Y
        assert wait_for(
            lambda: (
                controller.query_table("X", timeout=0.3) is not None
                and controller.query_table("X", timeout=0.3).get("Z", {}).get("cost") == 3
            ),
            timeout=3.0,
        )

        # Fail link X - Y
        controller.fail_link("X", "Y")

        # After silence timeout (3 * 0.15 = 0.45s) and DV propagation,
        # X should route to Y via Z (direct X-Z cost 4 + Z-Y cost 2 = 6)
        rerouted = wait_for(
            lambda: (
                controller.query_table("X", timeout=0.3) is not None
                and controller.query_table("X", timeout=0.3).get("Y", {}).get("cost") == 6
                and controller.query_table("X", timeout=0.3).get("Y", {}).get("next_hop") == "Z"
            ),
            timeout=3.0,
        )
        assert rerouted is True

        # Restore link X - Y
        controller.restore_link("X", "Y")

        # After advertisements resume, X -> Y cost returns to 1, and X -> Z returns to 3
        restored = wait_for(
            lambda: (
                controller.query_table("X", timeout=0.3) is not None
                and controller.query_table("X", timeout=0.3).get("Y", {}).get("cost") == 1
                and controller.query_table("X", timeout=0.3).get("Z", {}).get("cost") == 3
            ),
            timeout=3.0,
        )
        assert restored is True
    finally:
        controller.stop()
        assert controller.is_running() is False


def test_set_link_cost_via_controller():
    """Verify set_link_cost triggers re-advertisement and path recomputation."""
    controller = launch_network(
        topology_file=TRIANGLE_TOPO,
        interval=0.15,
        mitigation="split-horizon",
        base_port=19230,
        mode="process",
    )
    try:
        # Wait for initial convergence
        assert wait_for(
            lambda: (
                controller.query_table("X", timeout=0.3) is not None
                and controller.query_table("X", timeout=0.3).get("Z", {}).get("cost") == 3
            ),
            timeout=3.0,
        )

        # Increase link X-Y cost from 1 to 10
        controller.set_link_cost("X", "Y", 10)

        # Now path from X to Y should re-route via Z (cost 4 + 2 = 6 < 10)
        cost_updated = wait_for(
            lambda: (
                controller.query_table("X", timeout=0.3) is not None
                and controller.query_table("X", timeout=0.3).get("Y", {}).get("cost") == 6
                and controller.query_table("X", timeout=0.3).get("Y", {}).get("next_hop") == "Z"
            ),
            timeout=3.0,
        )
        assert cost_updated is True
    finally:
        controller.stop()
        assert controller.is_running() is False


def test_send_data_via_controller():
    """Verify send_data injects packet at source router."""
    controller = launch_network(
        topology_file=TRIANGLE_TOPO,
        interval=0.15,
        mitigation="split-horizon",
        base_port=19240,
        mode="process",
    )
    try:
        # Wait for initial convergence
        assert wait_for(
            lambda: (
                controller.query_table("X", timeout=0.3) is not None
                and controller.query_table("X", timeout=0.3).get("Z", {}).get("cost") == 3
            ),
            timeout=3.0,
        )

        # Inject data packet from X to Z
        controller.send_data(src="X", dst="Z", payload="Hello Z from Controller", hop_limit=10)
        time.sleep(0.3)
    finally:
        controller.stop()
        assert controller.is_running() is False


def test_launcher_cli_help():
    """Verify dv_routing.launcher CLI module displays help message."""
    proc = subprocess.run(
        [sys.executable, "-m", "dv_routing.launcher", "--help"],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0
    assert "--topology" in proc.stdout
    assert "--interval" in proc.stdout
    assert "--mitigation" in proc.stdout
    assert "--base-port" in proc.stdout
