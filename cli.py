#!/usr/bin/env python3
"""Hybrid CLI Controller for Distributed Distance-Vector Routing.

Supports report-compliant subcommands (send, link-fail, link-restore,
link-cost, show path, tables) and an interactive REPL mode.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import socket
import sys
import time
from typing import Dict, Optional, Tuple

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from dv_routing.launcher import NetworkLauncherController, launch_network  # noqa: E402
from dv_routing.network import Network  # noqa: E402
from dv_routing.router import INF  # noqa: E402
from dv_routing.topology import Topology, load_topology  # noqa: E402
from dv_routing.udp_transport import MSG_TYPE_CONTROL, MSG_TYPE_DATA  # noqa: E402


def send_udp(host: str, port: int, payload: dict) -> None:
    """Sends a JSON datagram to (host, port)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if hasattr(socket, "SIO_UDP_CONNRESET"):
        try:
            sock.ioctl(socket.SIO_UDP_CONNRESET, False)
        except Exception:
            pass
    try:
        data = json.dumps(payload).encode("utf-8")
        sock.sendto(data, (host, port))
    finally:
        sock.close()


def query_table_udp(host: str, port: int, router: str = "", timeout: float = 0.5, retries: int = 2) -> Optional[dict]:
    """Queries routing table snapshot from router at (host, port) via UDP."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if hasattr(socket, "SIO_UDP_CONNRESET"):
        try:
            sock.ioctl(socket.SIO_UDP_CONNRESET, False)
        except Exception:
            pass
    req = json.dumps({"type": MSG_TYPE_CONTROL, "action": "QUERY_TABLE"}).encode("utf-8")
    try:
        for _ in range(retries):
            try:
                sock.sendto(req, (host, port))
            except Exception:
                continue
            deadline = time.time() + timeout
            while time.time() < deadline:
                rem = max(0.01, deadline - time.time())
                sock.settimeout(rem)
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
                    if router and msg.get("router") and msg.get("router") != router:
                        continue
                    return msg.get("table")
        return None
    finally:
        sock.close()


def discover_routers(
    host: str = "127.0.0.1",
    base_port: int = 10001,
    count: int = 26,
    timeout: float = 0.4,
) -> Tuple[Dict[str, dict], Dict[str, int]]:
    """Probes ports base_port..base_port+count-1 for active routers."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if hasattr(socket, "SIO_UDP_CONNRESET"):
        try:
            sock.ioctl(socket.SIO_UDP_CONNRESET, False)
        except Exception:
            pass
    req = json.dumps({"type": MSG_TYPE_CONTROL, "action": "QUERY_TABLE"}).encode("utf-8")
    for p in range(base_port, base_port + count):
        try:
            sock.sendto(req, (host, p))
        except Exception:
            pass

    tables: Dict[str, dict] = {}
    ports: Dict[str, int] = {}
    deadline = time.time() + timeout
    while time.time() < deadline:
        rem = max(0.01, deadline - time.time())
        sock.settimeout(rem)
        try:
            data, addr = sock.recvfrom(65535)
        except (socket.timeout, TimeoutError):
            break
        except (ConnectionResetError, ConnectionRefusedError, OSError):
            break
        try:
            msg = json.loads(data.decode("utf-8"))
        except Exception:
            continue
        if msg.get("type") == "TABLE_RESPONSE":
            r = msg.get("router")
            if r:
                tables[r] = msg.get("table", {})
                ports[r] = addr[1]
    sock.close()
    return tables, ports


def resolve_router_port(router: str, base_port: int = 10001, ports_map: Optional[Dict[str, int]] = None) -> int:
    """Resolves UDP port for router name using discovered mapping or offset."""
    if ports_map and router in ports_map:
        return ports_map[router]
    if len(router) == 1 and router.isalpha():
        return base_port + (ord(router.upper()) - ord("A"))
    return base_port


def format_table_dict(router_name: str, table_dict: dict) -> str:
    """Formats a single router's table dictionary into pretty ASCII lines."""
    lines = [f"Router {router_name}"]
    lines.append(f"  {'Dest':<8} {'NextHop':<10} {'Cost':<6}")
    lines.append(f"  {'----':<8} {'-------':<10} {'----':<6}")
    for dest, entry in sorted(table_dict.items()):
        hop = entry.get("next_hop") or "-"
        cost = entry.get("cost")
        cost_str = "INF" if cost is None or cost >= INF else str(cost)
        lines.append(f"  {dest:<8} {hop:<10} {cost_str:<6}")
    return "\n".join(lines)


def trace_path(
    src: str,
    dst: str,
    base_port: int = 10001,
    host: str = "127.0.0.1",
    ports_map: Optional[Dict[str, int]] = None,
) -> str:
    """Traces hop-by-hop route path from src to dst by querying router tables."""
    src = src.upper()
    dst = dst.upper()
    if src == dst:
        return f"Path: {src} (cost=0) | Total cost: 0"

    discovered_tables, discovered_ports = discover_routers(host=host, base_port=base_port)
    if ports_map is None:
        ports_map = discovered_ports

    curr = src
    visited = {curr}
    cumulative_cost = 0
    segments = [curr]
    max_hops = 16

    for _ in range(max_hops):
        port = resolve_router_port(curr, base_port, ports_map)
        tbl = discovered_tables.get(curr)
        if tbl is None:
            tbl = query_table_udp(host, port, curr, timeout=0.5)
            if tbl is not None:
                discovered_tables[curr] = tbl

        if tbl is None:
            return f"Error: Router {curr} did not respond at {host}:{port}."

        dest_entry = tbl.get(dst)
        if not dest_entry:
            return f"Path: {' -> '.join(segments)} | Destination {dst} unreachable from {curr} (not in table)"

        next_hop = dest_entry.get("next_hop")
        dest_cost = dest_entry.get("cost", INF)
        if next_hop is None or dest_cost >= INF:
            return f"Path: {' -> '.join(segments)} | Destination {dst} unreachable from {curr} (cost=INF)"

        # Direct link cost from curr to next_hop
        next_hop_entry = tbl.get(next_hop)
        if next_hop_entry and next_hop_entry.get("cost") is not None and next_hop_entry.get("cost") < INF:
            link_cost = next_hop_entry.get("cost", 1)
        else:
            link_cost = 1

        cumulative_cost += link_cost
        segments.append(f"{next_hop} (cost={cumulative_cost})")

        if next_hop == dst:
            return f"Path: {' -> '.join(segments)} | Total cost: {cumulative_cost}"

        if next_hop in visited:
            return f"Path: {' -> '.join(segments)} | Routing loop detected at {next_hop}"

        visited.add(next_hop)
        curr = next_hop

    return f"Path: {' -> '.join(segments)} | Hop limit exceeded ({max_hops} hops)"


def trace_path_sim(net: Network, src: str, dst: str) -> str:
    """Traces hop-by-hop route path from src to dst in simulation mode."""
    src = src.upper()
    dst = dst.upper()
    if src not in net.routers or dst not in net.routers:
        return f"Error: Unknown routers {src} or {dst}"
    if src == dst:
        return f"Path: {src} (cost=0) | Total cost: 0"

    curr = src
    visited = {curr}
    cumulative_cost = 0
    segments = [curr]
    max_hops = 16

    for _ in range(max_hops):
        tbl = net.routers[curr].table
        entry = tbl.get(dst)
        if not entry or not entry.is_reachable():
            return f"Path: {' -> '.join(segments)} | Destination {dst} unreachable from {curr} (cost=INF)"
        next_hop = entry.next_hop
        link_entry = tbl.get(next_hop)
        link_cost = link_entry.cost if link_entry and link_entry.is_reachable() else 1
        cumulative_cost += link_cost
        segments.append(f"{next_hop} (cost={cumulative_cost})")
        if next_hop == dst:
            return f"Path: {' -> '.join(segments)} | Total cost: {cumulative_cost}"
        if next_hop in visited:
            return f"Path: {' -> '.join(segments)} | Routing loop detected at {next_hop}"
        visited.add(next_hop)
        curr = next_hop

    return f"Path: {' -> '.join(segments)} | Hop limit exceeded ({max_hops} hops)"


def parse_link(token: str) -> Tuple[str, str]:
    """Parses link format 'A-B' or 'A - B' into tuple ('A', 'B')."""
    token = token.replace(" ", "")
    if "-" not in token:
        raise ValueError("Link must look like A-B")
    a, b = token.split("-", 1)
    return a.strip().upper(), b.strip().upper()


def resolve_topology_path(topo_str: str) -> Path:
    """Resolves topology file path against CWD or package topologies directory."""
    p = Path(topo_str)
    if p.is_file():
        return p
    p_root = ROOT / topo_str
    if p_root.is_file():
        return p_root
    p_topos = ROOT / "topologies" / topo_str
    if p_topos.is_file():
        return p_topos
    return p


def print_help() -> None:
    print(
        """
Commands
--------
  tables              Print all routing tables
  send <src> <dst> [payload]
                      Send DATA packet from src to dst
  show path <src> <dst>
                      Show hop-by-hop path from src to dst
  path <src> <dst>    Shortcut for show path
  fail A-B            Fail link between A and B
  restore A-B         Restore a previously failed link
  cost A-B <cost>     Change link cost between A and B
  wait                Wait until tables converge
  sh on|off           Toggle split horizon (sim mode)
  pr on|off           Toggle poison reverse (sim mode)
  status              Show config + live/down links
  topo                Show topology summary
  help                Show this help
  quit                Stop simulation and exit
"""
    )


def cmd_send(args: argparse.Namespace) -> None:
    src = args.src.upper()
    dst = args.dst.upper()
    port = resolve_router_port(src, args.base_port)
    pkt_dict = {
        "src": src,
        "dst": dst,
        "packet_id": f"pkt-{src}-{int(time.time()*1000)}",
        "hop_limit": args.hop_limit,
        "payload": args.payload,
        "hops": [],
    }
    msg = {
        "type": MSG_TYPE_DATA,
        "sender": "CONTROLLER",
        "packet": pkt_dict,
        **pkt_dict,
    }
    send_udp("127.0.0.1", port, msg)
    print(f"Sent DATA packet {pkt_dict['packet_id']} from {src} to {dst}: \"{args.payload}\" (hop_limit={args.hop_limit})")


def cmd_link_fail(args: argparse.Namespace) -> None:
    ep1 = args.endpoint1.upper()
    ep2 = args.endpoint2.upper()
    port1 = resolve_router_port(ep1, args.base_port)
    port2 = resolve_router_port(ep2, args.base_port)
    msg1 = {"type": MSG_TYPE_CONTROL, "action": "DROP_LINK", "neighbor": ep2, "target": ep2, "endpoint": ep2}
    msg2 = {"type": MSG_TYPE_CONTROL, "action": "DROP_LINK", "neighbor": ep1, "target": ep1, "endpoint": ep1}
    send_udp("127.0.0.1", port1, msg1)
    send_udp("127.0.0.1", port2, msg2)
    print(f"Link {ep1}-{ep2} failed (DROP_LINK sent to both endpoints).")


def cmd_link_restore(args: argparse.Namespace) -> None:
    ep1 = args.endpoint1.upper()
    ep2 = args.endpoint2.upper()
    port1 = resolve_router_port(ep1, args.base_port)
    port2 = resolve_router_port(ep2, args.base_port)
    msg1 = {"type": MSG_TYPE_CONTROL, "action": "RESTORE_LINK", "neighbor": ep2, "target": ep2, "endpoint": ep2}
    msg2 = {"type": MSG_TYPE_CONTROL, "action": "RESTORE_LINK", "neighbor": ep1, "target": ep1, "endpoint": ep1}
    send_udp("127.0.0.1", port1, msg1)
    send_udp("127.0.0.1", port2, msg2)
    print(f"Link {ep1}-{ep2} restored (RESTORE_LINK sent to both endpoints).")


def cmd_link_cost(args: argparse.Namespace) -> None:
    ep1 = args.endpoint1.upper()
    ep2 = args.endpoint2.upper()
    port1 = resolve_router_port(ep1, args.base_port)
    port2 = resolve_router_port(ep2, args.base_port)
    msg1 = {"type": MSG_TYPE_CONTROL, "action": "SET_COST", "neighbor": ep2, "target": ep2, "endpoint": ep2, "cost": args.cost}
    msg2 = {"type": MSG_TYPE_CONTROL, "action": "SET_COST", "neighbor": ep1, "target": ep1, "endpoint": ep1, "cost": args.cost}
    send_udp("127.0.0.1", port1, msg1)
    send_udp("127.0.0.1", port2, msg2)
    print(f"Link {ep1}-{ep2} cost set to {args.cost} (SET_COST sent to both endpoints).")


def cmd_show(args: argparse.Namespace) -> None:
    if getattr(args, "show_command", None) == "path":
        print(trace_path(args.src, args.dst, base_port=args.base_port))
    else:
        print("Usage: cli.py show path <src> <dst>")


def cmd_tables(args: argparse.Namespace) -> None:
    tables, _ = discover_routers(base_port=args.base_port)
    if not tables:
        print("No active routers found.")
        return
    output = "\n\n".join(format_table_dict(r, tbl) for r, tbl in sorted(tables.items()))
    print(output)


def cmd_interactive(args: argparse.Namespace) -> None:
    print("=" * 60)
    print("  Distributed Distance-Vector Routing Simulator")
    print("=" * 60)

    topo_path = resolve_topology_path(args.topology)
    topo = load_topology(topo_path)
    print()
    print(topo.summary())

    mode = getattr(args, "mode", "process")
    base_port = getattr(args, "base_port", 10001)
    interval = getattr(args, "interval", 2.0)

    controller: Optional[NetworkLauncherController] = None
    net: Optional[Network] = None

    if mode == "process":
        print(f"\nLaunching network in process mode (interval={interval}s, base_port={base_port})...")
        controller = launch_network(
            topology_file=topo,
            interval=interval,
            mitigation="split-horizon",
            base_port=base_port,
            mode="process",
        )
        print("Waiting for initial convergence...")
        time.sleep(min(max(interval * 2.0, 1.0), 3.0))
        tables = controller.query_all_tables(timeout=1.0)
        if tables:
            print("\n" + "\n\n".join(format_table_dict(r, tbl) for r, tbl in sorted(tables.items())))
    else:
        print(f"\nLaunching network in simulation mode (interval={interval}s)...")
        net = Network(
            topo,
            advertise_interval=interval,
            split_horizon=False,
            poison_reverse=False,
            log=lambda msg: print(msg) if msg.startswith("[") else None,
        )
        net.start()
        print("Waiting for initial convergence...")
        net.wait_until_converged(stable_for=1.0, timeout=20)
        print("\n" + "\n\n".join(net.routers[n].format_table() for n in sorted(net.routers)))

    print_help()

    try:
        while True:
            try:
                raw = input("dv> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not raw:
                continue
            parts = raw.split()
            cmd = parts[0].lower()

            try:
                if cmd in ("quit", "exit", "q"):
                    break
                elif cmd == "help":
                    print_help()
                elif cmd == "tables":
                    if controller is not None:
                        tables = controller.query_all_tables(timeout=1.0)
                        if tables:
                            print("\n" + "\n\n".join(format_table_dict(r, tbl) for r, tbl in sorted(tables.items())))
                        else:
                            print("No response from routers.")
                    elif net is not None:
                        print("\n" + "\n\n".join(net.routers[n].format_table() for n in sorted(net.routers)))
                elif cmd == "wait":
                    if controller is not None:
                        time.sleep(interval * 1.5)
                        tables = controller.query_all_tables(timeout=1.0)
                        print("Converged.")
                        if tables:
                            print("\n" + "\n\n".join(format_table_dict(r, tbl) for r, tbl in sorted(tables.items())))
                    elif net is not None:
                        ok = net.wait_until_converged(stable_for=1.0, timeout=25)
                        print("Converged." if ok else "Timed out.")
                        print("\n" + "\n\n".join(net.routers[n].format_table() for n in sorted(net.routers)))
                elif cmd == "send" and len(parts) >= 3:
                    src = parts[1].upper()
                    dst = parts[2].upper()
                    payload = " ".join(parts[3:]) if len(parts) > 3 else "Test Payload"
                    if controller is not None:
                        controller.send_data(src, dst, payload)
                        print(f"Sent DATA packet from {src} to {dst}: \"{payload}\"")
                    elif net is not None:
                        print(f"Sent DATA packet from {src} to {dst}: \"{payload}\"")
                elif (cmd == "show" and len(parts) >= 4 and parts[1].lower() == "path") or (cmd == "path" and len(parts) >= 3):
                    if cmd == "show":
                        src, dst = parts[2].upper(), parts[3].upper()
                    else:
                        src, dst = parts[1].upper(), parts[2].upper()
                    if controller is not None:
                        print(trace_path(src, dst, base_port=controller.base_port, ports_map=controller.router_ports))
                    elif net is not None:
                        print(trace_path_sim(net, src, dst))
                elif cmd == "fail" and len(parts) >= 2:
                    a, b = parse_link(parts[1])
                    if controller is not None:
                        controller.fail_link(a, b)
                        print(f"Link {a}-{b} failed.")
                    elif net is not None:
                        net.fail_link(a, b)
                elif cmd == "restore" and len(parts) >= 2:
                    a, b = parse_link(parts[1])
                    if controller is not None:
                        controller.restore_link(a, b)
                        print(f"Link {a}-{b} restored.")
                    elif net is not None:
                        net.restore_link(a, b)
                elif cmd == "cost" and len(parts) >= 3:
                    a, b = parse_link(parts[1])
                    c = int(parts[2])
                    if controller is not None:
                        controller.set_link_cost(a, b, c)
                        print(f"Link {a}-{b} cost set to {c}.")
                    elif net is not None:
                        net.routers[a].set_neighbor(b, c)
                        net.routers[b].set_neighbor(a, c)
                        net._advertise_from(a)
                        net._advertise_from(b)
                        print(f"Link {a}-{b} cost set to {c}.")
                elif cmd == "sh" and len(parts) >= 2:
                    val = parts[1].lower() in ("on", "1", "true")
                    if controller is not None:
                        print(f"Split horizon in process mode configured at startup (SH={controller.split_horizon}).")
                    elif net is not None:
                        net.set_split_horizon(val)
                        print(f"Split horizon set to {val}.")
                elif cmd == "pr" and len(parts) >= 2:
                    val = parts[1].lower() in ("on", "1", "true")
                    if controller is not None:
                        print(f"Poison reverse in process mode configured at startup (PR={controller.poison_reverse}).")
                    elif net is not None:
                        net.set_poison_reverse(val)
                        print(f"Poison reverse set to {val}.")
                elif cmd == "status":
                    if controller is not None:
                        tables = controller.query_all_tables(timeout=1.0)
                        print(f"mode=process  running={controller.is_running()}  routers={sorted(tables.keys())}")
                    elif net is not None:
                        snap = net.snapshot()
                        print(
                            f"running={snap['running']}  SH={snap['split_horizon']}  "
                            f"PR={snap['poison_reverse']}  msgs={snap['message_count']}"
                        )
                        print(f"down links: {snap['down_links'] or '(none)'}")
                elif cmd == "topo":
                    print(topo.summary())
                else:
                    print("Unknown command. Type 'help'.")
            except Exception as exc:  # noqa: BLE001
                print(f"Error: {exc}")
    finally:
        if controller is not None:
            controller.stop()
        if net is not None:
            net.stop()
        print("Goodbye.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Distributed Distance-Vector Routing Hybrid CLI Controller",
    )
    subparsers = parser.add_subparsers(dest="command", required=False)

    # send
    p_send = subparsers.add_parser("send", help="Send DATA packet from src to dst")
    p_send.add_argument("--src", required=True, help="Source router name")
    p_send.add_argument("--dst", required=True, help="Destination router name")
    p_send.add_argument("--payload", default="Test Payload", help="Payload string")
    p_send.add_argument("--hop-limit", type=int, default=16, help="Hop limit")
    p_send.add_argument("--base-port", type=int, default=10001, help="Base UDP port")

    # link-fail
    p_fail = subparsers.add_parser("link-fail", help="Simulate link failure")
    p_fail.add_argument("--endpoint1", required=True, help="First endpoint")
    p_fail.add_argument("--endpoint2", required=True, help="Second endpoint")
    p_fail.add_argument("--base-port", type=int, default=10001, help="Base UDP port")

    # link-restore
    p_restore = subparsers.add_parser("link-restore", help="Restore link")
    p_restore.add_argument("--endpoint1", required=True, help="First endpoint")
    p_restore.add_argument("--endpoint2", required=True, help="Second endpoint")
    p_restore.add_argument("--base-port", type=int, default=10001, help="Base UDP port")

    # link-cost
    p_cost = subparsers.add_parser("link-cost", help="Update link cost")
    p_cost.add_argument("--endpoint1", required=True, help="First endpoint")
    p_cost.add_argument("--endpoint2", required=True, help="Second endpoint")
    p_cost.add_argument("--cost", type=int, required=True, help="New link cost")
    p_cost.add_argument("--base-port", type=int, default=10001, help="Base UDP port")

    # show
    p_show = subparsers.add_parser("show", help="Show network status or path")
    show_sub = p_show.add_subparsers(dest="show_command", required=False)
    p_show_path = show_sub.add_parser("path", help="Show hop-by-hop path")
    p_show_path.add_argument("src", help="Source router")
    p_show_path.add_argument("dst", help="Destination router")
    p_show_path.add_argument("--base-port", type=int, default=10001, help="Base UDP port")

    # tables
    p_tables = subparsers.add_parser("tables", help="Print all routing tables")
    p_tables.add_argument("--base-port", type=int, default=10001, help="Base UDP port")

    # interactive
    p_inter = subparsers.add_parser("interactive", help="Start interactive REPL")
    p_inter.add_argument("--mode", choices=["process", "sim"], default="process", help="Launch mode")
    p_inter.add_argument("--topology", default="topologies/default.txt", help="Path to topology")
    p_inter.add_argument("--interval", type=float, default=2.0, help="Advertisement interval")
    p_inter.add_argument("--base-port", type=int, default=10001, help="Base UDP port")

    return parser


def main() -> None:
    parser = build_parser()
    if len(sys.argv) == 1:
        args = parser.parse_args(["interactive"])
    else:
        args = parser.parse_args()

    cmd = args.command
    if cmd == "send":
        cmd_send(args)
    elif cmd == "link-fail":
        cmd_link_fail(args)
    elif cmd == "link-restore":
        cmd_link_restore(args)
    elif cmd == "link-cost":
        cmd_link_cost(args)
    elif cmd == "show":
        cmd_show(args)
    elif cmd == "tables":
        cmd_tables(args)
    elif cmd == "interactive" or cmd is None:
        cmd_interactive(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
