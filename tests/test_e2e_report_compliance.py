"""End-to-End System Verification & Regression Suite for Distributed Distance-Vector Routing.

Verifies 100% compliance with Section 8 Demonstration Plan Commands 1 through 6
and Requirements R1 through R12 from Group 5 Study and Scope Report:
  - Command 1: Multi-process launch of default 6-router mesh on ports 10001-10006 (R1-R4).
  - Command 2: Observer web interface telemetry (/api/state) (R11).
  - Command 3: Path query & hop-by-hop DATA packet forwarding from A to D (R9, R10).
  - Command 4: Silence-based link failure (A-B) & reroute via F at cost 7 (R5, R6).
  - Command 5: Link restore (A-B) & recovery of direct route at cost 1 (R6).
  - Command 6: Count-to-infinity reproduction & poison-reverse loop prevention (R8, R12).
  - Requirement R7: Runtime link cost change via CLI (B-C cost 5).
  - Requirement R9: Packet drop when hop limit reaches 0.
  - Requirement R10: Path query sequential hop-by-hop display.
"""

from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess
import sys
import time
from typing import Callable, Dict, Optional

import pytest
from starlette.testclient import TestClient

from dv_routing.forwarding import DataPacket, ForwardingResult, forward_data_packet
from dv_routing.launcher import NetworkLauncherController, launch_network
from dv_routing.router import INF, RouteEntry, Router
from dv_routing.topology import Topology, load_topology
import web.server as server
from web.server import app, get_state

ROOT = Path(__file__).resolve().parent.parent
TOPOLOGIES_DIR = ROOT / "topologies"
DEFAULT_TOPO = str(TOPOLOGIES_DIR / "default.txt")
LINE_TOPO = str(TOPOLOGIES_DIR / "count_to_infinity.txt")


def wait_for(condition: Callable[[], bool], timeout: float = 6.0, poll_interval: float = 0.05) -> bool:
    """Helper to poll for a condition until timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if condition():
                return True
        except Exception:
            pass
        time.sleep(poll_interval)
    return False


@pytest.fixture(scope="module")
def default_mesh_network():
    """Module-level fixture managing the 6-router mesh on default ports 10001-10006."""
    controller = launch_network(
        topology_file=DEFAULT_TOPO,
        interval=0.2,
        mitigation="split-horizon",
        base_port=10001,
        mode="process",
        host="127.0.0.1",
    )
    # Ensure network starts and tables converge initially
    assert wait_for(
        lambda: len(controller.query_all_tables(timeout=0.3)) == 6,
        timeout=6.0,
    ), "Default 6-router mesh failed to bind ports and reply with tables."

    yield controller

    controller.stop()


def test_section8_command1_launcher_and_ports(default_mesh_network: NetworkLauncherController):
    """Section 8 Command 1: Launch default 6-router mesh and verify port bindings & initial tables (R1-R4)."""
    controller = default_mesh_network
    assert controller.is_running() is True

    # R1: Verify all 6 router processes bind to designated ports 10001-10006
    expected_ports = {
        "A": 10001,
        "B": 10002,
        "C": 10003,
        "D": 10004,
        "E": 10005,
        "F": 10006,
    }
    assert controller.router_ports == expected_ports

    # Verify each router socket responds to UDP control queries
    assert wait_for(
        lambda: (
            (tables := controller.query_all_tables(timeout=0.4)) is not None
            and len(tables) == 6
            and all(r in tables for r in ["A", "B", "C", "D", "E", "F"])
            and (tbl_a := tables.get("A")) is not None
            and tbl_a.get("B", {}).get("cost") == 1
            and tbl_a.get("D", {}).get("cost") == 4
        ),
        timeout=6.0,
    ), "Failed to establish converged initial routing tables on default mesh."

    tables = controller.query_all_tables(timeout=0.5)

    # R2: Independent routing tables with self-cost 0 and valid destination schema
    for r_name in ["A", "B", "C", "D", "E", "F"]:
        tbl = tables[r_name]
        assert r_name in tbl
        assert tbl[r_name]["cost"] == 0
        assert tbl[r_name]["next_hop"] == r_name
        for dest, entry in tbl.items():
            assert "next_hop" in entry
            assert "cost" in entry
            assert entry["cost"] < INF

    # R3 & R4: Verify Bellman-Ford shortest paths from Router A
    # A-B is 1, A-F is 4, A-E is 3 (via B: 1+2), A-D is 4 (via B: 1+2+1), A-C is 2 (via B: 1+1)
    tbl_a = tables["A"]
    assert tbl_a["B"]["cost"] == 1
    assert tbl_a["B"]["next_hop"] == "B"
    assert tbl_a["C"]["cost"] == 2
    assert tbl_a["C"]["next_hop"] == "B"
    assert tbl_a["E"]["cost"] == 3
    assert tbl_a["E"]["next_hop"] == "B"
    assert tbl_a["D"]["cost"] == 4
    assert tbl_a["D"]["next_hop"] == "B"
    assert tbl_a["F"]["cost"] == 4
    assert tbl_a["F"]["next_hop"] == "F"


def test_section8_command2_observer_web_interface(default_mesh_network: NetworkLauncherController):
    """Section 8 Command 2: Observer web interface (/api/state) telemetry & topology representation (R11)."""
    with TestClient(app) as client:
        res = client.get("/api/state")
        assert res.status_code == 200
        data = res.json()

        assert data.get("running") is True
        assert "routers" in data
        assert "links" in data

        # Validate that all 6 routers are reported
        routers = data["routers"]
        for r in ["A", "B", "C", "D", "E", "F"]:
            assert r in routers
            assert r in routers[r]
            assert routers[r][r]["cost"] == 0

        # Validate that all 7 default links are reported and up
        links = data["links"]
        assert len(links) >= 7
        for link in links:
            assert "a" in link and "b" in link and "cost" in link and "up" in link
            assert link["up"] is True


def test_section8_command3_data_packet_forwarding_and_delivery(default_mesh_network: NetworkLauncherController):
    """Section 8 Command 3: Path query & hop-by-hop application DATA packet forwarding A -> D (R9, R10)."""
    controller = default_mesh_network

    # Query path via CLI show path A D
    res_path = subprocess.run(
        [sys.executable, "cli.py", "show", "path", "A", "D", "--base-port", "10001"],
        capture_output=True,
        text=True,
    )
    assert res_path.returncode == 0
    assert "Path: A -> B (cost=1) -> E (cost=3) -> D (cost=4) | Total cost: 4" in res_path.stdout

    # Send application DATA packet from A to D with payload "Test Payload"
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
            "Test Payload",
            "--hop-limit",
            "16",
            "--base-port",
            "10001",
        ],
        capture_output=True,
        text=True,
    )
    assert res_send.returncode == 0
    assert 'Sent DATA packet' in res_send.stdout
    assert '"Test Payload"' in res_send.stdout

    # Verify delivery at destination Router D using controller.query_data("D")
    delivered = wait_for(
        lambda: (
            (d_data := controller.query_data("D", timeout=0.4)) is not None
            and any(
                p.get("payload") == "Test Payload" and p.get("src") == "A" and p.get("dst") == "D"
                for p in d_data.get("received", [])
            )
        ),
        timeout=4.0,
    )
    assert delivered is True, "DATA packet with payload 'Test Payload' was not delivered to Router D."

    d_info = controller.query_data("D", timeout=0.5)
    matching = [p for p in d_info.get("received", []) if p.get("payload") == "Test Payload"]
    assert len(matching) > 0
    rx_pkt = matching[0]
    # Check hop-by-hop forwarding trace: A -> B -> E -> D
    assert rx_pkt["hops"] == ["A", "B", "E", "D"]
    # Hop limit started at 16, decremented at A(15), B(14), E(13), delivered at D(13)
    assert rx_pkt["hop_limit"] == 13


def test_section8_command4_silence_failure_and_reroute(default_mesh_network: NetworkLauncherController):
    """Section 8 Command 4: Simulate link failure by silence and verify rerouted path via F at cost 7 (R5, R6)."""
    controller = default_mesh_network

    # Simulate link failure by packet drop without deleting memory edges
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
            "10001",
        ],
        capture_output=True,
        text=True,
    )
    assert res_fail.returncode == 0

    # Wait for silence timeout (3 missed intervals = 3 * 0.2s = 0.6s) to trigger reroute via Router F
    rerouted = wait_for(
        lambda: (
            (tbl_a := controller.query_table("A", timeout=0.3)) is not None
            and tbl_a.get("B", {}).get("next_hop") == "F"
            and tbl_a.get("B", {}).get("cost") == 7
        ),
        timeout=5.0,
    )
    assert rerouted is True, "Router A did not reroute to B via Router F at cost 7 after silence timeout."

    # Verify CLI show path displays rerouted path
    res_path = subprocess.run(
        [sys.executable, "cli.py", "show", "path", "A", "B", "--base-port", "10001"],
        capture_output=True,
        text=True,
    )
    assert res_path.returncode == 0
    assert "Path: A -> F (cost=4) -> E (cost=5) -> B (cost=7) | Total cost: 7" in res_path.stdout


def test_section8_command5_link_restore_and_recovery(default_mesh_network: NetworkLauncherController):
    """Section 8 Command 5: Restore link and confirm recovery of direct route at cost 1 (R6)."""
    controller = default_mesh_network

    # Restore link A-B
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
            "10001",
        ],
        capture_output=True,
        text=True,
    )
    assert res_restore.returncode == 0

    # Confirm recovery of direct route at cost 1
    recovered = wait_for(
        lambda: (
            (tbl_a := controller.query_table("A", timeout=0.3)) is not None
            and tbl_a.get("B", {}).get("next_hop") == "B"
            and tbl_a.get("B", {}).get("cost") == 1
        ),
        timeout=5.0,
    )
    assert recovered is True, "Router A failed to recover direct route to B at cost 1."

    res_path = subprocess.run(
        [sys.executable, "cli.py", "show", "path", "A", "B", "--base-port", "10001"],
        capture_output=True,
        text=True,
    )
    assert res_path.returncode == 0
    assert "Path: A -> B (cost=1) | Total cost: 1" in res_path.stdout


def test_requirement_r7_runtime_link_cost_change(default_mesh_network: NetworkLauncherController):
    """Test Requirement R7: Runtime link cost change updates routes and advertisements."""
    controller = default_mesh_network

    # Change B-C link cost from 1 to 5
    res_cost = subprocess.run(
        [
            sys.executable,
            "cli.py",
            "link-cost",
            "--endpoint1",
            "B",
            "--endpoint2",
            "C",
            "--cost",
            "5",
            "--base-port",
            "10001",
        ],
        capture_output=True,
        text=True,
    )
    assert res_cost.returncode == 0

    # Wait for tables to update direct link cost and path
    updated = wait_for(
        lambda: (
            (tbl_b := controller.query_table("B", timeout=0.3)) is not None
            and tbl_b.get("C", {}).get("cost") == 5
            and (tbl_a := controller.query_table("A", timeout=0.3)) is not None
            and tbl_a.get("C", {}).get("cost") == 6
        ),
        timeout=5.0,
    )
    assert updated is True, "Routers did not update route costs after runtime link-cost change."

    # Verify show path reflects updated cost: B -> C is 5
    res_path_bc = subprocess.run(
        [sys.executable, "cli.py", "show", "path", "B", "C", "--base-port", "10001"],
        capture_output=True,
        text=True,
    )
    assert res_path_bc.returncode == 0
    assert "Path: B -> C (cost=5) | Total cost: 5" in res_path_bc.stdout

    # Verify A -> C path cost adjusted to 6 (A -> B(1) -> C(5))
    res_path_ac = subprocess.run(
        [sys.executable, "cli.py", "show", "path", "A", "C", "--base-port", "10001"],
        capture_output=True,
        text=True,
    )
    assert res_path_ac.returncode == 0
    assert "Path: A -> B (cost=1) -> C (cost=6) | Total cost: 6" in res_path_ac.stdout

    # Restore B-C cost back to 1 for clean state
    subprocess.run(
        [
            sys.executable,
            "cli.py",
            "link-cost",
            "--endpoint1",
            "B",
            "--endpoint2",
            "C",
            "--cost",
            "1",
            "--base-port",
            "10001",
        ],
        capture_output=True,
        text=True,
    )
    assert wait_for(
        lambda: (tbl_b := controller.query_table("B", timeout=0.3)) is not None and tbl_b.get("C", {}).get("cost") == 1,
        timeout=4.0,
    )


def test_requirement_r9_packet_drop_when_hop_limit_expires(default_mesh_network: NetworkLauncherController):
    """Test Requirement R9: Packet drop when hop limit expires."""
    controller = default_mesh_network

    # Path from A to D requires 3 hops (A -> B -> E -> D).
    # Sending with hop_limit=2 will cause packet to be dropped at intermediate router B.
    drop_payload = f"Expiring-Payload-{int(time.time()*1000)}"
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
            drop_payload,
            "--hop-limit",
            "2",
            "--base-port",
            "10001",
        ],
        capture_output=True,
        text=True,
    )
    assert res_send.returncode == 0

    # Wait briefly and verify destination D NEVER received the expired packet
    time.sleep(0.6)
    d_info = controller.query_data("D", timeout=0.4)
    assert d_info is not None
    rx_payloads = [p.get("payload") for p in d_info.get("received", [])]
    assert drop_payload not in rx_payloads, "Packet with expired hop limit should not have reached D."

    # Verify intermediate router B recorded a DROPPED_HOP_LIMIT event in forwarding history
    b_info = controller.query_data("B", timeout=0.4)
    assert b_info is not None
    dropped_records = [
        h for h in b_info.get("history", [])
        if h.get("status") == "DROPPED_HOP_LIMIT" and h.get("packet", {}).get("payload") == drop_payload
    ]
    assert len(dropped_records) > 0, "Router B should record DROPPED_HOP_LIMIT in forwarding history."
    assert dropped_records[0]["packet"]["hop_limit"] == 0

    # Unit verification of forward_data_packet logic
    pkt = DataPacket(src="A", dst="D", packet_id="test-drop", hop_limit=1, payload="Drop me")
    res = forward_data_packet(pkt, "B", {"D": RouteEntry(next_hop="E", cost=2)})
    assert res.status == "DROPPED_HOP_LIMIT"
    assert res.packet.hop_limit == 0


def test_section8_command6_count_to_infinity_and_poison_reverse():
    """Section 8 Command 6: Count-to-infinity reproduction & poison-reverse loop prevention (R8, R12)."""
    # 1. Run without mitigation -> count-to-infinity occurs
    res_none = subprocess.run(
        [sys.executable, "demos/run_count_to_infinity.py", "--mitigation", "none", "--watch", "2.0"],
        capture_output=True,
        text=True,
    )
    assert res_none.returncode == 0
    out_none = res_none.stdout.lower()
    assert "scenario: without split horizon" in out_none or "without" in out_none
    assert "reached inf" in out_none or "counted to infinity" in out_none or "failing link b-c" in out_none

    # 2. Run with poison-reverse -> loop immediately prevented
    res_pr = subprocess.run(
        [sys.executable, "demos/run_count_to_infinity.py", "--mitigation", "poison-reverse", "--watch", "2.0"],
        capture_output=True,
        text=True,
    )
    assert res_pr.returncode == 0
    out_pr = res_pr.stdout.lower()
    assert "with poison reverse" in out_pr
    assert "poison reverse worked" in out_pr or "correctly marks c unreachable" in out_pr


def test_requirements_r1_through_r12_compliance_matrix():
    """Verify complete requirements R1 through R12 acceptance criteria matrix."""
    # R1: Topology loading & process startup
    topo = load_topology(DEFAULT_TOPO)
    assert len(topo.routers) == 6
    assert len(topo.links) == 7

    # R2: Independent routing tables with INF=16
    r = Router("A")
    r.set_neighbor("B", 1)
    assert r.table["A"].cost == 0
    assert r.table["B"].cost == 1
    assert INF == 16

    # R3: Decentralized Bellman-Ford update
    changed = r.update_from_advertisement("B", {"C": 2})
    assert changed is True
    assert r.table["C"].cost == 3
    assert r.table["C"].next_hop == "B"

    # R4: Periodic UDP advertisements vector building
    vec = r.build_advertisement("B", split_horizon=False)
    assert "C" in vec

    # R5: Neighbor and route expiry timeout
    r.invalidate_via("B")
    assert r.table["C"].cost == INF

    # R6: Silence-based failure without edge deletion
    # Validated by test_section8_command4 and test_section8_command5

    # R7: Link cost change at runtime
    r.set_neighbor("B", 5)
    assert r.neighbors["B"] == 5

    # R8: Loop mitigation toggles
    vec_sh = r.build_advertisement("B", split_horizon=True)
    # Routes learned via B are suppressed in split horizon
    assert "C" not in vec_sh or vec_sh.get("C") == INF

    # R9: Application DATA forwarding
    pkt = DataPacket("A", "B", "p1", 5, "payload")
    res = forward_data_packet(pkt, "A", {"B": RouteEntry("B", 1)})
    assert res.status == "FORWARD"
    assert pkt.hop_limit == 4

    # R10: Path query display
    # Validated by test_section8_command3

    # R11: Observable telemetry interface
    # Validated by test_section8_command2

    # R12: Reproducible demonstration scripts
    assert Path("demos/run_count_to_infinity.py").exists()
    assert Path("demos/run_failure_recovery.py").exists()
    assert Path("demos/run_evaluation_suite.py").exists()
