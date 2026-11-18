"""Tests for application DATA packet forwarding engine (Requirement R9)."""

import pytest
from dv_routing.router import RouteEntry, INF
from dv_routing.forwarding import (
    DataPacket,
    ForwardingResult,
    forward_data_packet,
)


def test_datapacket_serialization():
    pkt = DataPacket(
        src="A",
        dst="C",
        packet_id="pkt-001",
        hop_limit=15,
        payload="Test message payload",
        hops=["A"],
    )
    d = pkt.to_dict()
    assert d == {
        "src": "A",
        "dst": "C",
        "packet_id": "pkt-001",
        "hop_limit": 15,
        "payload": "Test message payload",
        "hops": ["A"],
    }

    restored = DataPacket.from_dict(d)
    assert restored == pkt
    assert restored.hops == ["A"]

    # Test from_dict without hops defaults to empty list
    d_no_hops = {
        "src": "A",
        "dst": "B",
        "packet_id": "pkt-002",
        "hop_limit": 10,
        "payload": "No hops payload",
    }
    restored2 = DataPacket.from_dict(d_no_hops)
    assert restored2.hops == []


def test_forward_packet_valid_next_hop():
    pkt = DataPacket(
        src="A",
        dst="C",
        packet_id="pkt-1",
        hop_limit=15,
        payload="Hello C",
    )
    routing_table = {
        "A": RouteEntry(next_hop="A", cost=0),
        "B": RouteEntry(next_hop="B", cost=1),
        "C": RouteEntry(next_hop="B", cost=2),
    }

    result = forward_data_packet(pkt, current_router="A", routing_table=routing_table)

    assert result.status == "FORWARD"
    assert result.next_hop == "B"
    assert result.packet.hop_limit == 14
    assert result.packet.hops == ["A"]


def test_forward_packet_delivery_at_destination():
    pkt = DataPacket(
        src="A",
        dst="C",
        packet_id="pkt-2",
        hop_limit=10,
        payload="Deliver me",
        hops=["A", "B"],
    )
    routing_table = {
        "C": RouteEntry(next_hop="C", cost=0),
    }

    result = forward_data_packet(pkt, current_router="C", routing_table=routing_table)

    assert result.status == "DELIVERED"
    assert result.next_hop is None
    assert result.packet.hops == ["A", "B", "C"]
    assert result.packet.hop_limit == 10  # Not decremented on delivery


def test_forward_packet_hop_limit_expired():
    # Hop limit 1 reaching intermediate router -> cannot forward further
    pkt = DataPacket(
        src="A",
        dst="C",
        packet_id="pkt-3",
        hop_limit=1,
        payload="Expiring packet",
        hops=["A"],
    )
    routing_table = {
        "C": RouteEntry(next_hop="C", cost=1),
    }

    result = forward_data_packet(pkt, current_router="B", routing_table=routing_table)

    assert result.status == "DROPPED_HOP_LIMIT"
    assert result.packet.hop_limit == 0
    assert result.packet.hops == ["A", "B"]
    assert result.next_hop is None

    # Hop limit 0
    pkt2 = DataPacket(
        src="A",
        dst="C",
        packet_id="pkt-3b",
        hop_limit=0,
        payload="Zero hop limit packet",
        hops=[],
    )
    result2 = forward_data_packet(pkt2, current_router="A", routing_table=routing_table)
    assert result2.status == "DROPPED_HOP_LIMIT"
    assert result2.packet.hop_limit == 0
    assert result2.packet.hops == ["A"]


def test_forward_packet_unreachable_missing_route():
    pkt = DataPacket(
        src="A",
        dst="Z",
        packet_id="pkt-4",
        hop_limit=15,
        payload="Lost packet",
    )
    routing_table = {
        "A": RouteEntry(next_hop="A", cost=0),
        "B": RouteEntry(next_hop="B", cost=1),
    }

    result = forward_data_packet(pkt, current_router="A", routing_table=routing_table)

    assert result.status == "DROPPED_UNREACHABLE"
    assert result.next_hop is None
    assert result.packet.hop_limit == 14
    assert result.packet.hops == ["A"]


def test_forward_packet_unreachable_cost_inf():
    pkt = DataPacket(
        src="A",
        dst="C",
        packet_id="pkt-5",
        hop_limit=15,
        payload="Broken link",
    )
    routing_table = {
        "A": RouteEntry(next_hop="A", cost=0),
        "C": RouteEntry(next_hop="B", cost=INF),
    }

    result = forward_data_packet(pkt, current_router="A", routing_table=routing_table)

    assert result.status == "DROPPED_UNREACHABLE"
    assert result.next_hop is None
    assert result.packet.hop_limit == 14
    assert result.packet.hops == ["A"]


def test_forward_packet_unreachable_next_hop_none():
    pkt = DataPacket(
        src="A",
        dst="C",
        packet_id="pkt-6",
        hop_limit=15,
        payload="No next hop",
    )
    routing_table = {
        "A": RouteEntry(next_hop="A", cost=0),
        "C": RouteEntry(next_hop=None, cost=5),
    }

    result = forward_data_packet(pkt, current_router="A", routing_table=routing_table)

    assert result.status == "DROPPED_UNREACHABLE"
    assert result.next_hop is None
    assert result.packet.hop_limit == 14
    assert result.packet.hops == ["A"]


def test_multi_hop_forwarding_trace():
    # Simulate forwarding across A -> B -> C
    table_a = {
        "A": RouteEntry(next_hop="A", cost=0),
        "B": RouteEntry(next_hop="B", cost=1),
        "C": RouteEntry(next_hop="B", cost=2),
    }
    table_b = {
        "A": RouteEntry(next_hop="A", cost=1),
        "B": RouteEntry(next_hop="B", cost=0),
        "C": RouteEntry(next_hop="C", cost=1),
    }
    table_c = {
        "A": RouteEntry(next_hop="B", cost=2),
        "B": RouteEntry(next_hop="B", cost=1),
        "C": RouteEntry(next_hop="C", cost=0),
    }

    pkt = DataPacket(
        src="A",
        dst="C",
        packet_id="trace-1",
        hop_limit=5,
        payload="End to end test",
    )

    # Step 1: Router A
    r1 = forward_data_packet(pkt, current_router="A", routing_table=table_a)
    assert r1.status == "FORWARD"
    assert r1.next_hop == "B"
    assert pkt.hop_limit == 4
    assert pkt.hops == ["A"]

    # Step 2: Router B receives it
    r2 = forward_data_packet(pkt, current_router="B", routing_table=table_b)
    assert r2.status == "FORWARD"
    assert r2.next_hop == "C"
    assert pkt.hop_limit == 3
    assert pkt.hops == ["A", "B"]

    # Step 3: Router C receives it
    r3 = forward_data_packet(pkt, current_router="C", routing_table=table_c)
    assert r3.status == "DELIVERED"
    assert r3.next_hop is None
    assert pkt.hop_limit == 3
    assert pkt.hops == ["A", "B", "C"]
