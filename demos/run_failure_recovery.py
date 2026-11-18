#!/usr/bin/env python3
"""Scripted demo: converge → fail a link → reconverge → restore → reconverge."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Dict, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dv_routing import INF, Network, load_topology  # noqa: E402
from dv_routing.launcher import NetworkLauncherController, launch_network  # noqa: E402


def format_table_dict(router_name: str, table_dict: dict) -> str:
    """Formats a router table dictionary into pretty ASCII lines."""
    lines = [f"Router {router_name}"]
    lines.append(f"  {'Dest':<8} {'NextHop':<10} {'Cost':<6}")
    lines.append(f"  {'----':<8} {'-------':<10} {'----':<6}")
    for dest, entry in sorted(table_dict.items()):
        hop = entry.get("next_hop") or "-"
        cost = entry.get("cost")
        cost_str = "INF" if cost is None or cost >= INF else str(cost)
        lines.append(f"  {dest:<8} {hop:<10} {cost_str:<6}")
    return "\n".join(lines)


def wait_converged_process(
    controller: NetworkLauncherController,
    stable_for: float = 0.8,
    timeout: float = 20.0,
) -> Dict[str, dict]:
    """Polls router processes until all routing tables stabilize."""
    deadline = time.monotonic() + timeout
    last_snap = None
    stable_since: Optional[float] = None
    target_count = len(controller.router_ports)

    while time.monotonic() < deadline:
        tables = controller.query_all_tables(timeout=0.4)
        if len(tables) == target_count:
            snap = tuple(
                (r, tuple(sorted((d, e.get("next_hop"), e.get("cost")) for d, e in tbl.items())))
                for r, tbl in sorted(tables.items())
            )
            now = time.monotonic()
            if snap == last_snap:
                if stable_since is not None and (now - stable_since) >= stable_for:
                    return tables
                if stable_since is None:
                    stable_since = now
            else:
                last_snap = snap
                stable_since = now
        time.sleep(0.1)
    return controller.query_all_tables(timeout=0.5)


def run_process_mode(args: argparse.Namespace, a: str, b: str) -> None:
    print(f"Launching network in process mode (interval={args.interval}s, base_port={args.base_port})...")
    controller = launch_network(
        topology_file=args.topology,
        interval=args.interval,
        mitigation="split-horizon",
        base_port=args.base_port,
        mode="process",
    )
    try:
        print("\n=== BEFORE FAILURE (waiting for convergence) ===")
        tables = wait_converged_process(controller, stable_for=0.8, timeout=20)
        print("\n\n".join(format_table_dict(r, tbl) for r, tbl in sorted(tables.items())))

        print(f"\n=== FAILING LINK {a}-{b} ===")
        controller.fail_link(a, b)
        tables = wait_converged_process(controller, stable_for=1.0, timeout=25)
        print("\n=== AFTER FAILURE ===")
        print("\n\n".join(format_table_dict(r, tbl) for r, tbl in sorted(tables.items())))

        print(f"\n=== RESTORING LINK {a}-{b} ===")
        controller.restore_link(a, b)
        tables = wait_converged_process(controller, stable_for=0.8, timeout=20)
        print("\n=== AFTER RECOVERY ===")
        print("\n\n".join(format_table_dict(r, tbl) for r, tbl in sorted(tables.items())))
    finally:
        controller.stop()


def run_sim_mode(args: argparse.Namespace, a: str, b: str) -> None:
    topo = load_topology(args.topology)
    net = Network(topo, advertise_interval=args.interval, log=lambda m: None)
    net.start()
    try:
        print("\n=== BEFORE FAILURE (waiting for convergence) ===")
        net.wait_until_converged(stable_for=0.8, timeout=20)
        print("\n\n".join(net.routers[n].format_table() for n in sorted(net.routers)))

        print(f"\n=== FAILING LINK {a}-{b} ===")
        net.fail_link(a, b)
        net.wait_until_converged(stable_for=1.0, timeout=25)
        print("\n=== AFTER FAILURE ===")
        print("\n\n".join(net.routers[n].format_table() for n in sorted(net.routers)))

        print(f"\n=== RESTORING LINK {a}-{b} ===")
        net.restore_link(a, b)
        net.wait_until_converged(stable_for=0.8, timeout=20)
        print("\n=== AFTER RECOVERY ===")
        print("\n\n".join(net.routers[n].format_table() for n in sorted(net.routers)))
    finally:
        net.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Link failure / recovery demo")
    parser.add_argument(
        "--topology",
        default=str(ROOT / "topologies" / "default.txt"),
        help="Topology file path",
    )
    parser.add_argument("--fail", default="A-B", help="Link to fail, e.g. A-B")
    parser.add_argument(
        "--mode",
        choices=["process", "sim"],
        default="process",
        help="Launch mode: process (default) or sim (in-process threads)",
    )
    parser.add_argument("--base-port", type=int, default=10001, help="Base UDP port for process mode")
    parser.add_argument("--interval", type=float, default=0.25, help="Advertisement interval")
    args = parser.parse_args()

    a, b = args.fail.split("-")
    topo = load_topology(args.topology)
    print(topo.summary())
    print()

    if args.mode == "process":
        run_process_mode(args, a, b)
    else:
        run_sim_mode(args, a, b)

    print("\nDemo complete.")


if __name__ == "__main__":
    main()
