"""Multi-process Network Launcher and Controller for distributed distance-vector routing."""

from __future__ import annotations

import argparse
import json
import multiprocessing
from pathlib import Path
import socket
import sys
import threading
import time
from typing import Dict, List, Optional, Union

from .router_process import RouterProcess
from .topology import Topology, load_topology
from .udp_transport import MSG_TYPE_CONTROL, MSG_TYPE_DATA


def _run_router_process(
    name: str,
    port: int,
    neighbor_ports: Dict[str, int],
    neighbor_costs: Dict[str, int],
    host: str = "127.0.0.1",
    interval: float = 2.0,
    split_horizon: bool = False,
    poison_reverse: bool = False,
) -> None:
    """Entry point for a spawned router process daemon."""
    rp = RouterProcess(
        name=name,
        port=port,
        neighbor_ports=neighbor_ports,
        neighbor_costs=neighbor_costs,
        host=host,
        interval=interval,
        split_horizon=split_horizon,
        poison_reverse=poison_reverse,
    )
    rp.start()
    try:
        while rp._running:
            time.sleep(0.1)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        rp.stop()


class NetworkLauncherController:
    """Controller managing a network of virtual router processes or threads."""

    def __init__(
        self,
        topology: Topology,
        interval: float = 2.0,
        split_horizon: bool = False,
        poison_reverse: bool = False,
        base_port: int = 10001,
        mode: str = "process",
        host: str = "127.0.0.1",
    ) -> None:
        self.topology = topology
        self.interval = float(interval)
        self.split_horizon = split_horizon
        self.poison_reverse = poison_reverse
        self.base_port = int(base_port)
        self.mode = mode
        self.host = host

        # Assign ports deterministically: sorted router names -> base_port + i
        sorted_routers = sorted(self.topology.routers)
        self.router_ports: Dict[str, int] = {
            r: self.base_port + i for i, r in enumerate(sorted_routers)
        }

        self._processes: List[multiprocessing.Process] = []
        self._daemons: List[RouterProcess] = []
        self._running: bool = False
        self._lock = threading.Lock()

    def start(self) -> None:
        """Starts all router processes or threads."""
        with self._lock:
            if self._running:
                return
            self._running = True

            sorted_routers = sorted(self.topology.routers)
            if self.mode == "process":
                ctx = multiprocessing.get_context("spawn")
                for r in sorted_routers:
                    port = self.router_ports[r]
                    nbr_costs = self.topology.neighbors_of(r)
                    nbr_ports = {nbr: self.router_ports[nbr] for nbr in nbr_costs}
                    p = ctx.Process(
                        target=_run_router_process,
                        args=(
                            r,
                            port,
                            nbr_ports,
                            nbr_costs,
                            self.host,
                            self.interval,
                            self.split_horizon,
                            self.poison_reverse,
                        ),
                        name=f"RouterProcess-{r}",
                        daemon=True,
                    )
                    self._processes.append(p)
                    p.start()
            elif self.mode == "thread":
                for r in sorted_routers:
                    port = self.router_ports[r]
                    nbr_costs = self.topology.neighbors_of(r)
                    nbr_ports = {nbr: self.router_ports[nbr] for nbr in nbr_costs}
                    rp = RouterProcess(
                        name=r,
                        port=port,
                        neighbor_ports=nbr_ports,
                        neighbor_costs=nbr_costs,
                        host=self.host,
                        interval=self.interval,
                        split_horizon=self.split_horizon,
                        poison_reverse=self.poison_reverse,
                    )
                    self._daemons.append(rp)
                    rp.start()
            else:
                raise ValueError(f"Unknown mode: {self.mode}")

    def stop(self) -> None:
        """Gracefully stops all router processes, joins them with timeout, and terminates if needed."""
        with self._lock:
            if not self._running and not self._processes and not self._daemons:
                return
            self._running = False

        # Graceful stop: send CONTROL STOP datagram to all router ports
        for port in self.router_ports.values():
            try:
                self._send_udp(port, {"type": MSG_TYPE_CONTROL, "action": "STOP"})
            except Exception:
                pass

        if self.mode == "process":
            for p in self._processes:
                p.join(timeout=1.0)
                if p.is_alive():
                    p.terminate()
                    p.join(timeout=1.0)
            self._processes.clear()
        elif self.mode == "thread":
            for rp in self._daemons:
                rp.stop()
            self._daemons.clear()

    def is_running(self) -> bool:
        """Checks if router processes or daemons are active."""
        if not self._running:
            return False
        if self.mode == "process":
            return any(p.is_alive() for p in self._processes)
        elif self.mode == "thread":
            return any(getattr(rp, "_running", False) for rp in self._daemons)
        return False

    def _send_udp(self, port: int, payload: dict) -> None:
        """Helper to send a UDP datagram to a local port."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except Exception:
                pass
        try:
            data = json.dumps(payload).encode("utf-8")
            sock.sendto(data, (self.host, port))
        finally:
            sock.close()

    def query_table(self, router: str, timeout: float = 1.0) -> Optional[dict]:
        """Sends UDP QUERY_TABLE control message to router's port and receives table snapshot."""
        if router not in self.router_ports:
            return None
        port = self.router_ports[router]
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except Exception:
                pass
        sock.settimeout(timeout)
        try:
            req = json.dumps({"type": MSG_TYPE_CONTROL, "action": "QUERY_TABLE"}).encode("utf-8")
            sock.sendto(req, (self.host, port))
            deadline = time.time() + timeout
            while time.time() < deadline:
                remaining = max(0.01, deadline - time.time())
                sock.settimeout(remaining)
                try:
                    data, _ = sock.recvfrom(65535)
                except (socket.timeout, TimeoutError):
                    break
                except (ConnectionResetError, ConnectionRefusedError, OSError):
                    break
                try:
                    msg = json.loads(data.decode("utf-8"))
                except Exception:
                    continue
                if msg.get("type") == "TABLE_RESPONSE" and msg.get("router") == router:
                    return msg.get("table")
            return None
        finally:
            sock.close()

    def query_all_tables(self, timeout: float = 1.0) -> Dict[str, dict]:
        """Queries all running routers and returns mapping router_name -> table_dict."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except Exception:
                pass
        req = json.dumps({"type": MSG_TYPE_CONTROL, "action": "QUERY_TABLE"}).encode("utf-8")
        results: Dict[str, dict] = {}
        target_routers = set(self.router_ports.keys())
        try:
            for r, port in self.router_ports.items():
                try:
                    sock.sendto(req, (self.host, port))
                except Exception:
                    pass

            deadline = time.time() + timeout
            while len(results) < len(target_routers) and time.time() < deadline:
                remaining = max(0.01, deadline - time.time())
                sock.settimeout(remaining)
                try:
                    data, _ = sock.recvfrom(65535)
                except (socket.timeout, TimeoutError):
                    break
                except (ConnectionResetError, ConnectionRefusedError, OSError):
                    break
                try:
                    msg = json.loads(data.decode("utf-8"))
                except Exception:
                    continue
                if msg.get("type") == "TABLE_RESPONSE":
                    r_name = msg.get("router")
                    if r_name in target_routers:
                        results[r_name] = msg.get("table", {})
        finally:
            sock.close()
        return results

    def query_data(self, router: str, timeout: float = 1.0) -> Optional[dict]:
        """Queries received data packets and forwarding history from router."""
        if router not in self.router_ports:
            return None
        port = self.router_ports[router]
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except Exception:
                pass
        sock.settimeout(timeout)
        try:
            req = json.dumps({"type": MSG_TYPE_CONTROL, "action": "QUERY_DATA"}).encode("utf-8")
            sock.sendto(req, (self.host, port))
            deadline = time.time() + timeout
            while time.time() < deadline:
                remaining = max(0.01, deadline - time.time())
                sock.settimeout(remaining)
                try:
                    data, _ = sock.recvfrom(65535)
                except (socket.timeout, TimeoutError):
                    break
                except (ConnectionResetError, ConnectionRefusedError, OSError):
                    break
                try:
                    msg = json.loads(data.decode("utf-8"))
                except Exception:
                    continue
                if msg.get("type") == "DATA_RESPONSE" and msg.get("router") == router:
                    return msg
            return None
        finally:
            sock.close()

    def fail_link(self, a: str, b: str) -> None:
        """Sends DROP_LINK control command to router A for B, and to router B for A."""
        if a in self.router_ports:
            self._send_udp(
                self.router_ports[a],
                {"type": MSG_TYPE_CONTROL, "action": "DROP_LINK", "neighbor": b, "target": b, "endpoint": b},
            )
        if b in self.router_ports:
            self._send_udp(
                self.router_ports[b],
                {"type": MSG_TYPE_CONTROL, "action": "DROP_LINK", "neighbor": a, "target": a, "endpoint": a},
            )

    def restore_link(self, a: str, b: str) -> None:
        """Sends RESTORE_LINK control command to router A for B, and to router B for A."""
        if a in self.router_ports:
            self._send_udp(
                self.router_ports[a],
                {"type": MSG_TYPE_CONTROL, "action": "RESTORE_LINK", "neighbor": b, "target": b, "endpoint": b},
            )
        if b in self.router_ports:
            self._send_udp(
                self.router_ports[b],
                {"type": MSG_TYPE_CONTROL, "action": "RESTORE_LINK", "neighbor": a, "target": a, "endpoint": a},
            )

    def set_link_cost(self, a: str, b: str, cost: int) -> None:
        """Sends SET_COST control command to both endpoints."""
        if a in self.router_ports:
            self._send_udp(
                self.router_ports[a],
                {"type": MSG_TYPE_CONTROL, "action": "SET_COST", "neighbor": b, "target": b, "endpoint": b, "cost": int(cost)},
            )
        if b in self.router_ports:
            self._send_udp(
                self.router_ports[b],
                {"type": MSG_TYPE_CONTROL, "action": "SET_COST", "neighbor": a, "target": a, "endpoint": a, "cost": int(cost)},
            )

    def send_data(self, src: str, dst: str, payload: str, hop_limit: int = 16) -> None:
        """Injects application DATA packet at src router."""
        if src not in self.router_ports:
            raise ValueError(f"Unknown source router: {src}")
        pkt_dict = {
            "src": src,
            "dst": dst,
            "packet_id": f"pkt-{src}-{int(time.time()*1000)}",
            "hop_limit": hop_limit,
            "payload": payload,
            "hops": [],
        }
        msg = {
            "type": MSG_TYPE_DATA,
            "sender": "CONTROLLER",
            "packet": pkt_dict,
            **pkt_dict,
        }
        self._send_udp(self.router_ports[src], msg)


def launch_network(
    topology_file: Union[str, Path, Topology],
    interval: float = 2.0,
    mitigation: str = "none",
    base_port: int = 10001,
    mode: str = "process",
    host: str = "127.0.0.1",
) -> NetworkLauncherController:
    """Convenience factory: parses topology, configures mitigation, and launches network."""
    if isinstance(topology_file, Topology):
        topo = topology_file
    else:
        topo = load_topology(topology_file)

    split_horizon = mitigation in ("split-horizon", "split_horizon")
    poison_reverse = mitigation in ("poison-reverse", "poison_reverse")
    if poison_reverse:
        split_horizon = True

    controller = NetworkLauncherController(
        topology=topo,
        interval=interval,
        split_horizon=split_horizon,
        poison_reverse=poison_reverse,
        base_port=base_port,
        mode=mode,
        host=host,
    )
    controller.start()
    return controller


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multi-process Network Launcher for Distance-Vector Routing")
    parser.add_argument("--topology", required=True, help="Path to topology configuration file")
    parser.add_argument("--interval", type=float, default=2.0, help="Periodic advertisement interval in seconds")
    parser.add_argument(
        "--mitigation",
        choices=["none", "split-horizon", "poison-reverse"],
        default="none",
        help="Loop mitigation strategy",
    )
    parser.add_argument("--base-port", type=int, default=10001, help="Base UDP port for router assignments")
    parser.add_argument("--mode", choices=["process", "thread"], default="process", help="Launch mode")
    parser.add_argument("--host", default="127.0.0.1", help="Host address to bind")
    return parser.parse_args()


def main() -> None:
    multiprocessing.freeze_support()
    args = parse_args()

    controller = launch_network(
        topology_file=args.topology,
        interval=args.interval,
        mitigation=args.mitigation,
        base_port=args.base_port,
        mode=args.mode,
        host=args.host,
    )

    print(f"Launched {len(controller.router_ports)} routers from {args.topology} (mitigation={args.mitigation}):")
    for r, port in sorted(controller.router_ports.items()):
        print(f"  Router {r} -> {args.host}:{port}")

    print("Network is running. Press Ctrl+C to terminate all router processes.")

    try:
        while controller.is_running():
            time.sleep(0.5)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        controller.stop()
        print("All router processes stopped.")


if __name__ == "__main__":
    main()
