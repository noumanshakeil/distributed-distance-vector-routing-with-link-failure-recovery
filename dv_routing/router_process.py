"""Autonomous Router Process Daemon with silence-based failure detection and DATA forwarding."""

from __future__ import annotations

import argparse
import json
import socket
import sys
import threading
import time
from typing import Callable, Dict, List, Optional, Set, Tuple

from .forwarding import DataPacket, ForwardingResult, forward_data_packet
from .router import INF, RouteEntry, Router
from .udp_transport import (
    MSG_TYPE_ADVERT,
    MSG_TYPE_CONTROL,
    MSG_TYPE_DATA,
    UDPTransport,
)


class RouterProcess:
    """An autonomous distance-vector router process communicating via UDP datagrams.

    Manages neighbor adjacencies, periodic and triggered distance-vector advertisements,
    silence-based failure detection (3 missed intervals), simulated link failure by
    packet drop without deleting edge state, hop-by-hop application DATA packet forwarding,
    and control query responses.
    """

    def __init__(
        self,
        name: str,
        port: int,
        neighbor_ports: Dict[str, int],
        neighbor_costs: Dict[str, int],
        host: str = "127.0.0.1",
        interval: float = 2.0,
        split_horizon: bool = False,
        poison_reverse: bool = False,
    ) -> None:
        self.name = name
        self.port = port
        self.neighbor_ports: Dict[str, int] = {str(k): int(v) for k, v in neighbor_ports.items()}
        self.neighbor_costs: Dict[str, int] = {str(k): int(v) for k, v in neighbor_costs.items()}
        self.host = host
        self.interval = float(interval)
        self.split_horizon = split_horizon
        self.poison_reverse = poison_reverse

        self.router = Router(name)
        self.transport = UDPTransport(host, port, on_message=self._on_udp_message)

        self._last_heard: Dict[str, float] = {}
        self._dropped_links: Set[str] = set()
        self._running = False
        self._timer_thread: Optional[threading.Thread] = None

        self._stop_event = threading.Event()
        self._state_lock = threading.RLock()
        self._start_time: float = 0.0
        self._pkt_seq: int = 0

        self.received_data: List[DataPacket] = []
        self.forward_history: List[ForwardingResult] = []
        self.on_data_received: Optional[Callable[[DataPacket, ForwardingResult], None]] = None

    def start(self) -> None:
        """Initializes direct neighbor costs on Router, starts UDP transport, and spawns timer thread."""
        with self._state_lock:
            if self._running:
                return
            self._running = True
            self._stop_event.clear()
            self._start_time = time.time()

            # Seed direct neighbor links in Router
            for nbr, cost in self.neighbor_costs.items():
                self.router.set_neighbor(nbr, cost)

            self.transport.start()
            # Capture bound port if dynamic port 0 was passed
            self.port = self.transport.port

            self._timer_thread = threading.Thread(
                target=self._timer_loop,
                daemon=True,
                name=f"RouterProcess-{self.name}-Timer",
            )
            self._timer_thread.start()

            # Transmit immediate initial advertisements to live neighbors
            self._broadcast_advertisement()

    def stop(self) -> None:
        """Stops the timer thread, closes UDP transport, and joins threads."""
        with self._state_lock:
            if not self._running:
                return
            self._running = False

        self._stop_event.set()
        self.transport.close()

        timer_thread = self._timer_thread
        if timer_thread is not None and timer_thread.is_alive():
            if threading.current_thread() != timer_thread:
                timer_thread.join(timeout=2.0)

    def fail_link_to(self, neighbor: str) -> None:
        """Simulates link failure by dropping packets to/from neighbor without deleting edge state."""
        with self._state_lock:
            self._dropped_links.add(neighbor)

    def restore_link_to(self, neighbor: str) -> None:
        """Restores packet delivery for neighbor, allowing periodic advertisements to recover connectivity."""
        with self._state_lock:
            self._dropped_links.discard(neighbor)

    def set_link_cost(self, neighbor: str, cost: int) -> None:
        """Updates direct link cost for neighbor and triggers advertisement broadcast."""
        with self._state_lock:
            self.neighbor_costs[neighbor] = cost
            if neighbor not in self._dropped_links:
                self.router.set_neighbor(neighbor, cost)

        self._broadcast_advertisement()

    def get_table_snapshot(self) -> dict:
        """Returns thread-safe snapshot of router's distance-vector table."""
        return self.router.snapshot()

    def send_data(
        self,
        dst: str,
        payload: str,
        hop_limit: int = 16,
        packet_id: Optional[str] = None,
    ) -> DataPacket:
        """Creates and injects an application DATA packet hop-by-hop."""
        if packet_id is None:
            with self._state_lock:
                self._pkt_seq += 1
                seq = self._pkt_seq
            packet_id = f"pkt-{self.name}-{seq}"

        pkt = DataPacket(
            src=self.name,
            dst=dst,
            packet_id=packet_id,
            hop_limit=hop_limit,
            payload=payload,
            hops=[],
        )

        result = forward_data_packet(
            pkt,
            current_router=self.name,
            routing_table=self.router.table,
        )

        with self._state_lock:
            self.forward_history.append(result)

        if result.status == "FORWARD":
            next_hop = result.next_hop
            with self._state_lock:
                dropped = next_hop in self._dropped_links
            if next_hop is not None and next_hop in self.neighbor_ports and not dropped:
                dest_port = self.neighbor_ports[next_hop]
                msg = {
                    "type": MSG_TYPE_DATA,
                    "sender": self.name,
                    "packet": pkt.to_dict(),
                    **pkt.to_dict(),
                }
                self.transport.send_json(self.host, dest_port, msg)
        elif result.status == "DELIVERED":
            with self._state_lock:
                self.received_data.append(pkt)
            if self.on_data_received:
                try:
                    self.on_data_received(pkt, result)
                except Exception:
                    pass

        return pkt

    def _on_udp_message(self, data: dict, addr: Tuple[str, int]) -> None:
        """Callback invoked by UDPTransport on receiving a datagram."""
        msg_type = data.get("type")

        if msg_type == MSG_TYPE_ADVERT:
            sender = data.get("sender")
            vector = data.get("vector", {})
            if not sender:
                return

            with self._state_lock:
                if sender in self._dropped_links:
                    return  # Dropped link: ignore advertisement

                self._last_heard[sender] = time.time()

                # If neighbor was marked down, restore direct link cost
                was_down = (
                    sender not in self.router.neighbors
                    or self.router.table.get(sender) is None
                    or self.router.table[sender].cost >= INF
                )
                if was_down and sender in self.neighbor_costs:
                    self.router.set_neighbor(sender, self.neighbor_costs[sender])

            changed = self.router.update_from_advertisement(sender, vector)
            if changed or was_down:
                self._broadcast_advertisement(exclude=sender)

        elif msg_type == MSG_TYPE_DATA:
            incoming_sender = data.get("sender")
            if incoming_sender is None:
                port_to_nbr = {p: n for n, p in self.neighbor_ports.items()}
                incoming_sender = port_to_nbr.get(addr[1])

            with self._state_lock:
                if incoming_sender and incoming_sender in self._dropped_links:
                    return

            if "packet" in data and isinstance(data["packet"], dict):
                pkt = DataPacket.from_dict(data["packet"])
            elif "src" in data and "dst" in data:
                pkt = DataPacket.from_dict(data)
            else:
                return

            with self._state_lock:
                if pkt.hops and pkt.hops[-1] in self._dropped_links:
                    return

            result = forward_data_packet(
                pkt,
                current_router=self.name,
                routing_table=self.router.table,
            )

            with self._state_lock:
                self.forward_history.append(result)

            if result.status == "FORWARD":
                next_hop = result.next_hop
                with self._state_lock:
                    dropped = next_hop in self._dropped_links
                if next_hop is not None and next_hop in self.neighbor_ports and not dropped:
                    dest_port = self.neighbor_ports[next_hop]
                    msg = {
                        "type": MSG_TYPE_DATA,
                        "sender": self.name,
                        "packet": pkt.to_dict(),
                        **pkt.to_dict(),
                    }
                    self.transport.send_json(self.host, dest_port, msg)
            elif result.status == "DELIVERED":
                with self._state_lock:
                    self.received_data.append(pkt)
                if self.on_data_received:
                    try:
                        self.on_data_received(pkt, result)
                    except Exception:
                        pass

        elif msg_type == MSG_TYPE_CONTROL:
            action = (data.get("action") or data.get("command") or "").upper()
            if action == "DROP_LINK":
                nbr = data.get("neighbor") or data.get("target") or data.get("endpoint")
                if nbr:
                    self.fail_link_to(nbr)
            elif action == "RESTORE_LINK":
                nbr = data.get("neighbor") or data.get("target") or data.get("endpoint")
                if nbr:
                    self.restore_link_to(nbr)
            elif action == "SET_COST":
                nbr = data.get("neighbor") or data.get("target") or data.get("endpoint")
                cost = int(data.get("cost", 1))
                if nbr:
                    self.set_link_cost(nbr, cost)
            elif action == "QUERY_TABLE":
                with self._state_lock:
                    dropped = list(self._dropped_links)
                resp = {
                    "type": "TABLE_RESPONSE",
                    "router": self.name,
                    "table": self.get_table_snapshot(),
                    "dropped_links": dropped,
                }
                self.transport.send_json(addr[0], addr[1], resp)
            elif action in ("QUERY_DATA", "QUERY_RECEIVED"):
                with self._state_lock:
                    rcvd = [p.to_dict() for p in self.received_data]
                    fwds = [
                        {
                            "status": r.status,
                            "next_hop": r.next_hop,
                            "packet": r.packet.to_dict(),
                        }
                        for r in self.forward_history
                    ]
                resp = {
                    "type": "DATA_RESPONSE",
                    "router": self.name,
                    "received": rcvd,
                    "history": fwds,
                }
                self.transport.send_json(addr[0], addr[1], resp)
            elif action == "STOP":
                threading.Thread(target=self.stop, daemon=True).start()

    def _broadcast_advertisement(self, exclude: Optional[str] = None) -> None:
        """Sends DV advertisement to all non-dropped neighbors."""
        with self._state_lock:
            dropped = set(self._dropped_links)
            neighbors = dict(self.neighbor_ports)

        for nbr, port in neighbors.items():
            if nbr == exclude or nbr in dropped:
                continue
            vector = self.router.build_advertisement(
                nbr,
                split_horizon=self.split_horizon,
                poison_reverse=self.poison_reverse,
            )
            msg = {
                "type": MSG_TYPE_ADVERT,
                "sender": self.name,
                "vector": vector,
            }
            try:
                self.transport.send_json(self.host, port, msg)
            except Exception:
                pass

    def _is_neighbor_active(self, nbr: str) -> bool:
        """Checks if routes through nbr or direct adjacency are currently active."""
        with self.router._lock:
            if nbr in self.router.neighbors:
                return True
            for dest, entry in self.router.table.items():
                if dest != self.name and entry.next_hop == nbr and entry.cost < INF:
                    return True
            if nbr in self.router.table and self.router.table[nbr].cost < INF:
                return True
        return False

    def _timer_loop(self) -> None:
        """Background thread checking silence timeouts and triggering periodic advertisements."""
        last_advert = 0.0
        tick = min(self.interval / 4.0, 0.05)

        while self._running:
            now = time.time()
            silence_timeout = 3.0 * self.interval

            with self._state_lock:
                active_neighbors = list(self.neighbor_ports.keys())

            # Silence check for failure detection
            for nbr in active_neighbors:
                with self._state_lock:
                    last_time = self._last_heard.get(nbr, self._start_time)

                if (now - last_time) >= silence_timeout:
                    if self._is_neighbor_active(nbr):
                        changed = self.router.invalidate_via(nbr)
                        self.router.remove_neighbor(nbr)
                        if changed:
                            self._broadcast_advertisement(exclude=nbr)

            # Periodic advertisement
            if (now - last_advert) >= self.interval:
                self._broadcast_advertisement()
                last_advert = now

            self._stop_event.wait(timeout=tick)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Autonomous Distance-Vector Router Process Daemon")
    parser.add_argument("--name", required=True, help="Router name (e.g. A)")
    parser.add_argument("--port", type=int, required=True, help="UDP listening port")
    parser.add_argument("--neighbors", required=True, help="JSON mapping of neighbor names to UDP ports")
    parser.add_argument("--costs", required=True, help="JSON mapping of neighbor names to link costs")
    parser.add_argument("--host", default="127.0.0.1", help="Host address to bind (default: 127.0.0.1)")
    parser.add_argument("--interval", type=float, default=2.0, help="Periodic advertisement interval in seconds")
    parser.add_argument("--split-horizon", action="store_true", help="Enable split horizon")
    parser.add_argument("--poison-reverse", action="store_true", help="Enable poison reverse")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    neighbor_ports = {str(k): int(v) for k, v in json.loads(args.neighbors).items()}
    neighbor_costs = {str(k): int(v) for k, v in json.loads(args.costs).items()}

    rp = RouterProcess(
        name=args.name,
        port=args.port,
        neighbor_ports=neighbor_ports,
        neighbor_costs=neighbor_costs,
        host=args.host,
        interval=args.interval,
        split_horizon=args.split_horizon,
        poison_reverse=args.poison_reverse,
    )
    rp.start()
    print(f"Router {args.name} daemon running on {args.host}:{args.port}")

    try:
        while rp._running:
            time.sleep(0.5)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        rp.stop()
        print(f"Router {args.name} daemon stopped.")


if __name__ == "__main__":
    main()
