"""Application DATA packet forwarding engine for distance-vector routing (Requirement R9)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .router import RouteEntry


@dataclass
class DataPacket:
    """Application DATA packet routed hop-by-hop across routers."""

    src: str
    dst: str
    packet_id: str
    hop_limit: int
    payload: str
    hops: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "src": self.src,
            "dst": self.dst,
            "packet_id": self.packet_id,
            "hop_limit": self.hop_limit,
            "payload": self.payload,
            "hops": list(self.hops),
        }

    @classmethod
    def from_dict(cls, data: dict) -> DataPacket:
        return cls(
            src=data["src"],
            dst=data["dst"],
            packet_id=data["packet_id"],
            hop_limit=data["hop_limit"],
            payload=data["payload"],
            hops=list(data.get("hops", [])),
        )


@dataclass
class ForwardingResult:
    """Outcome of forwarding a DataPacket at a specific router node."""

    status: str  # "DELIVERED", "FORWARD", "DROPPED_HOP_LIMIT", "DROPPED_UNREACHABLE"
    packet: DataPacket
    next_hop: Optional[str] = None
    reason: str = ""


def forward_data_packet(
    packet: DataPacket,
    current_router: str,
    routing_table: Dict[str, RouteEntry],
) -> ForwardingResult:
    """Forward a data packet hop-by-hop using the router's current routing table.

    Steps:
    1. Append current router to packet.hops.
    2. Check if current router is packet destination -> DELIVERED.
    3. Check if hop_limit <= 1 -> set hop_limit to 0 and return DROPPED_HOP_LIMIT.
    4. Decrement hop_limit -= 1.
    5. Lookup dst in routing_table. If absent or unreachable -> DROPPED_UNREACHABLE.
    6. Return FORWARD with next_hop from routing entry.
    """
    packet.hops.append(current_router)

    if current_router == packet.dst:
        return ForwardingResult(
            status="DELIVERED",
            packet=packet,
            next_hop=None,
            reason=f"Delivered to destination {current_router}",
        )

    if packet.hop_limit <= 1:
        packet.hop_limit = 0
        return ForwardingResult(
            status="DROPPED_HOP_LIMIT",
            packet=packet,
            next_hop=None,
            reason="Hop limit reached zero",
        )

    packet.hop_limit -= 1

    entry = routing_table.get(packet.dst)
    if entry is None or not entry.is_reachable():
        return ForwardingResult(
            status="DROPPED_UNREACHABLE",
            packet=packet,
            next_hop=None,
            reason=f"Destination {packet.dst} is unreachable from {current_router}",
        )

    return ForwardingResult(
        status="FORWARD",
        packet=packet,
        next_hop=entry.next_hop,
        reason=f"Forwarding towards {packet.dst} via {entry.next_hop}",
    )
