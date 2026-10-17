#!/usr/bin/env python3
"""Scripted demo: converge → fail a link → reconverge → restore → reconverge."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dv_routing import Network, load_topology  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Link failure / recovery demo")
    parser.add_argument(
        "--topology",
        default=str(ROOT / "topologies" / "default.txt"),
        help="Topology file path",
    )
    parser.add_argument("--fail", default="A-B", help="Link to fail, e.g. A-B")
    args = parser.parse_args()

    a, b = args.fail.split("-")
    topo = load_topology(args.topology)
    print(topo.summary())
    print()

    net = Network(topo, advertise_interval=0.25, log=print)
    net.start()

    print("\n=== BEFORE FAILURE (waiting for convergence) ===")
    net.wait_until_converged(stable_for=1.0, timeout=20)
    print(net.print_all_tables())

    print(f"\n=== FAILING LINK {a}-{b} ===")
    net.fail_link(a, b)
    net.wait_until_converged(stable_for=1.2, timeout=25)
    print("\n=== AFTER FAILURE ===")
    print(net.print_all_tables())

    print(f"\n=== RESTORING LINK {a}-{b} ===")
    net.restore_link(a, b)
    net.wait_until_converged(stable_for=1.0, timeout=20)
    print("\n=== AFTER RECOVERY ===")
    print(net.print_all_tables())

    net.stop()
    print("\nDemo complete.")


if __name__ == "__main__":
    main()
