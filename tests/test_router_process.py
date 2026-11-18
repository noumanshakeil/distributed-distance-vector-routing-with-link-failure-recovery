"""Tests for RouterProcess daemon and silence-based failure detection."""

import json
import socket
import subprocess
import sys
import time
from typing import Callable, Any

import pytest
from dv_routing.router import INF
from dv_routing.router_process import RouterProcess
from dv_routing.udp_transport import MSG_TYPE_CONTROL, UDPTransport


def wait_for(condition: Callable[[], bool], timeout: float = 3.0, poll_interval: float = 0.05) -> bool:
    """Helper to poll for a condition until timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(poll_interval)
    return False


def test_router_process_startup_and_convergence():
    """Two routers exchange advertisements over UDP and converge."""
    rp_a = RouterProcess(
        "A",
        port=19111,
        neighbor_ports={"B": 19112},
        neighbor_costs={"B": 1},
        interval=0.15,
    )
    rp_b = RouterProcess(
        "B",
        port=19112,
        neighbor_ports={"A": 19111},
        neighbor_costs={"A": 1},
        interval=0.15,
    )
    rp_a.start()
    rp_b.start()

    try:
        converged = wait_for(
            lambda: (
                rp_a.get_table_snapshot().get("B", {}).get("cost") == 1
                and rp_b.get_table_snapshot().get("A", {}).get("cost") == 1
            ),
            timeout=2.0,
        )
        assert converged is True
        table_a = rp_a.get_table_snapshot()
        assert table_a["B"]["cost"] == 1
        assert table_a["B"]["next_hop"] == "B"
    finally:
        rp_a.stop()
        rp_b.stop()


def test_router_process_silence_based_failure_detection():
    """Link failure simulated by packet drop expires routes via silence timeout."""
    # interval = 0.15s -> silence timeout = 3 * 0.15 = 0.45s
    rp_a = RouterProcess(
        "A",
        port=19121,
        neighbor_ports={"B": 19122},
        neighbor_costs={"B": 2},
        interval=0.15,
    )
    rp_b = RouterProcess(
        "B",
        port=19122,
        neighbor_ports={"A": 19121},
        neighbor_costs={"A": 2},
        interval=0.15,
    )
    rp_a.start()
    rp_b.start()

    try:
        # Wait for initial convergence
        assert wait_for(
            lambda: rp_a.get_table_snapshot().get("B", {}).get("cost") == 2,
            timeout=2.0,
        )

        # Drop link symmetrically
        rp_a.fail_link_to("B")
        rp_b.fail_link_to("A")

        # Before silence timeout (e.g. after 0.1s), route should still be present
        time.sleep(0.1)
        assert rp_a.get_table_snapshot().get("B", {}).get("cost") == 2

        # After silence timeout (0.45s from failure), route should expire to INF
        expired = wait_for(
            lambda: rp_a.get_table_snapshot().get("B", {}).get("cost") in (INF, "INF"),
            timeout=2.0,
        )
        assert expired is True
        assert rp_a.get_table_snapshot()["B"]["cost"] in (INF, "INF")

        # Restore link delivery
        rp_a.restore_link_to("B")
        rp_b.restore_link_to("A")

        # Route should recover upon receiving next advertisement
        restored = wait_for(
            lambda: rp_a.get_table_snapshot().get("B", {}).get("cost") == 2,
            timeout=2.0,
        )
        assert restored is True
        assert rp_a.get_table_snapshot()["B"]["cost"] == 2
    finally:
        rp_a.stop()
        rp_b.stop()


def test_router_process_data_packet_forwarding_multi_hop():
    """DATA packets are forwarded hop-by-hop across A -> B -> C."""
    # A (19131) --1-- B (19132) --1-- C (19133)
    rp_a = RouterProcess(
        "A",
        port=19131,
        neighbor_ports={"B": 19132},
        neighbor_costs={"B": 1},
        interval=0.15,
    )
    rp_b = RouterProcess(
        "B",
        port=19132,
        neighbor_ports={"A": 19131, "C": 19133},
        neighbor_costs={"A": 1, "C": 1},
        interval=0.15,
    )
    rp_c = RouterProcess(
        "C",
        port=19133,
        neighbor_ports={"B": 19132},
        neighbor_costs={"B": 1},
        interval=0.15,
    )

    rp_a.start()
    rp_b.start()
    rp_c.start()

    try:
        # Wait for full convergence across A, B, C
        converged = wait_for(
            lambda: (
                rp_a.get_table_snapshot().get("C", {}).get("cost") == 2
                and rp_c.get_table_snapshot().get("A", {}).get("cost") == 2
            ),
            timeout=2.5,
        )
        assert converged is True
        assert rp_a.get_table_snapshot()["C"]["next_hop"] == "B"

        # Send DATA packet from A to C
        pkt = rp_a.send_data(dst="C", payload="Hello C from A", hop_limit=10, packet_id="pkt-test-01")
        assert pkt.packet_id == "pkt-test-01"

        # Verify C receives the packet
        delivered = wait_for(
            lambda: any(p.packet_id == "pkt-test-01" for p in rp_c.received_data),
            timeout=2.0,
        )
        assert delivered is True

        delivered_pkt = next(p for p in rp_c.received_data if p.packet_id == "pkt-test-01")
        assert delivered_pkt.src == "A"
        assert delivered_pkt.dst == "C"
        assert delivered_pkt.payload == "Hello C from A"
        assert delivered_pkt.hops == ["A", "B", "C"]
        # Hop limit: started at 10, decremented at A (9) and B (8), not decremented on delivery at C
        assert delivered_pkt.hop_limit == 8
    finally:
        rp_a.stop()
        rp_b.stop()
        rp_c.stop()


def test_router_process_set_link_cost():
    """Updating link cost at runtime propagates new distance to neighbors."""
    rp_a = RouterProcess(
        "A",
        port=19141,
        neighbor_ports={"B": 19142},
        neighbor_costs={"B": 10},
        interval=0.15,
    )
    rp_b = RouterProcess(
        "B",
        port=19142,
        neighbor_ports={"A": 19141},
        neighbor_costs={"A": 10},
        interval=0.15,
    )
    rp_a.start()
    rp_b.start()

    try:
        assert wait_for(
            lambda: rp_a.get_table_snapshot().get("B", {}).get("cost") == 10,
            timeout=2.0,
        )

        # Change link cost symmetrically to 3
        rp_a.set_link_cost("B", 3)
        rp_b.set_link_cost("A", 3)

        updated = wait_for(
            lambda: (
                rp_a.get_table_snapshot().get("B", {}).get("cost") == 3
                and rp_b.get_table_snapshot().get("A", {}).get("cost") == 3
            ),
            timeout=2.0,
        )
        assert updated is True
    finally:
        rp_a.stop()
        rp_b.stop()


def test_router_process_control_messages_over_udp():
    """CONTROL messages over UDP trigger DROP_LINK, RESTORE_LINK, SET_COST, QUERY_TABLE."""
    rp_a = RouterProcess(
        "A",
        port=19151,
        neighbor_ports={"B": 19152},
        neighbor_costs={"B": 1},
        interval=0.15,
    )
    rp_a.start()

    # Client socket to send control messages
    client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_sock.bind(("127.0.0.1", 19159))
    client_sock.settimeout(2.0)

    try:
        # 1. QUERY_TABLE
        client_sock.sendto(
            json.dumps({"type": MSG_TYPE_CONTROL, "action": "QUERY_TABLE"}).encode("utf-8"),
            ("127.0.0.1", 19151),
        )
        data, _ = client_sock.recvfrom(65535)
        resp = json.loads(data.decode("utf-8"))
        assert resp["type"] == "TABLE_RESPONSE"
        assert resp["router"] == "A"
        assert "A" in resp["table"]
        assert resp["dropped_links"] == []

        # 2. DROP_LINK
        client_sock.sendto(
            json.dumps({"type": MSG_TYPE_CONTROL, "action": "DROP_LINK", "neighbor": "B"}).encode("utf-8"),
            ("127.0.0.1", 19151),
        )
        assert wait_for(lambda: "B" in rp_a._dropped_links, timeout=1.0)

        # 3. RESTORE_LINK
        client_sock.sendto(
            json.dumps({"type": MSG_TYPE_CONTROL, "action": "RESTORE_LINK", "neighbor": "B"}).encode("utf-8"),
            ("127.0.0.1", 19151),
        )
        assert wait_for(lambda: "B" not in rp_a._dropped_links, timeout=1.0)

        # 4. SET_COST
        client_sock.sendto(
            json.dumps({"type": MSG_TYPE_CONTROL, "action": "SET_COST", "neighbor": "B", "cost": 7}).encode("utf-8"),
            ("127.0.0.1", 19151),
        )
        assert wait_for(lambda: rp_a.neighbor_costs.get("B") == 7, timeout=1.0)
    finally:
        client_sock.close()
        rp_a.stop()


def test_router_process_cli_help():
    """CLI execution via python -m dv_routing.router_process supports arguments."""
    res = subprocess.run(
        [sys.executable, "-m", "dv_routing.router_process", "--help"],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0
    assert "--name" in res.stdout
    assert "--port" in res.stdout
    assert "--neighbors" in res.stdout
    assert "--costs" in res.stdout
