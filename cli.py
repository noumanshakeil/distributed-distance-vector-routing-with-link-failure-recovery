#!/usr/bin/env python3
"""Interactive CLI for the distance-vector routing simulation.

Use this in a viva to load a topology, fail/restore links, toggle
split horizon / poison reverse, and print routing tables on demand.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from dv_routing import Network, load_topology  # noqa: E402

SAMPLES = {
    "1": ("Default 6-router mesh", ROOT / "topologies" / "default.txt"),
    "2": ("Count-to-infinity line (A-B-C)", ROOT / "topologies" / "count_to_infinity.txt"),
    "3": ("Small triangle", ROOT / "topologies" / "triangle.txt"),
}


def pick_topology() -> Path:
    print("\nChoose a topology:")
    for key, (label, path) in SAMPLES.items():
        print(f"  {key}) {label}")
    print("  4) Load custom file path")
    choice = input("Selection [1]: ").strip() or "1"
    if choice == "4":
        path = Path(input("Path to topology file: ").strip())
        return path
    if choice not in SAMPLES:
        print("Unknown choice — using default.")
        choice = "1"
    return SAMPLES[choice][1]


def print_help() -> None:
    print(
        """
Commands
--------
  tables              Print all routing tables
  fail A-B            Fail link between A and B
  restore A-B         Restore a previously failed link
  wait                Wait until tables converge
  sh on|off           Toggle split horizon
  pr on|off           Toggle poison reverse (also enables SH)
  status              Show config + live/down links
  topo                Show topology summary
  help                Show this help
  quit                Stop simulation and exit
"""
    )


def parse_link(token: str) -> tuple[str, str]:
    token = token.replace(" ", "")
    if "-" not in token:
        raise ValueError("Link must look like A-B")
    a, b = token.split("-", 1)
    return a.strip(), b.strip()


def main() -> None:
    print("=" * 60)
    print("  Distributed Distance-Vector Routing Simulator")
    print("=" * 60)

    path = pick_topology()
    topo = load_topology(path)
    print()
    print(topo.summary())

    net = Network(
        topo,
        advertise_interval=0.35,
        split_horizon=False,
        poison_reverse=False,
        log=print,
    )
    net.start()
    print("\nWaiting for initial convergence...")
    net.wait_until_converged(stable_for=1.0, timeout=20)
    print(net.print_all_tables())
    print_help()

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
                print(net.print_all_tables())
            elif cmd == "wait":
                ok = net.wait_until_converged(stable_for=1.0, timeout=25)
                print("Converged." if ok else "Timed out.")
                print(net.print_all_tables())
            elif cmd == "fail" and len(parts) >= 2:
                a, b = parse_link(parts[1])
                net.fail_link(a, b)
            elif cmd == "restore" and len(parts) >= 2:
                a, b = parse_link(parts[1])
                net.restore_link(a, b)
            elif cmd == "sh" and len(parts) >= 2:
                net.set_split_horizon(parts[1].lower() in ("on", "1", "true"))
            elif cmd == "pr" and len(parts) >= 2:
                net.set_poison_reverse(parts[1].lower() in ("on", "1", "true"))
            elif cmd == "status":
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
        except Exception as exc:  # noqa: BLE001 — show errors to the operator
            print(f"Error: {exc}")

    net.stop()
    print("Goodbye.")


if __name__ == "__main__":
    main()
