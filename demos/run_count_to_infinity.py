#!/usr/bin/env python3
"""Count-to-infinity demo: without fix, then with poison reverse or split horizon.

Uses the classic A—B—C line. After B–C fails:
  * Without SH/PR, A and B inflate the cost toward C (count to infinity).
  * With poison reverse, B advertises C=INF back to A and the loop dies.
  * With split horizon, B suppresses route C back to A and the loop dies.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dv_routing import INF, Network, load_topology  # noqa: E402


def watch_cost_to_c(net: Network, duration: float, label: str) -> None:
    """Print A and B's cost to C over time."""
    print(f"\n--- Watching cost-to-C ({label}) for {duration:.1f}s ---")
    end = time.monotonic() + duration
    last = None
    while time.monotonic() < end:
        a_cost = net.route_cost("A", "C")
        b_cost = net.route_cost("B", "C")
        snap = (a_cost, b_cost)
        if snap != last:
            a_s = "INF" if a_cost >= INF else str(a_cost)
            b_s = "INF" if b_cost >= INF else str(b_cost)
            print(f"  t+{duration - (end - time.monotonic()):4.1f}s  A→C={a_s:>3}  B→C={b_s:>3}")
            last = snap
        time.sleep(0.1)
    print(net.print_all_tables())


def run_scenario(*, mitigation: str = "none", watch: float = 6.0) -> None:
    topo = load_topology(ROOT / "topologies" / "count_to_infinity.txt")
    if mitigation == "poison-reverse":
        title = "WITH poison reverse"
        sh = True
        pr = True
    elif mitigation == "split-horizon":
        title = "WITH split horizon"
        sh = True
        pr = False
    else:
        title = "WITHOUT split horizon / poison reverse"
        sh = False
        pr = False

    print("\n" + "=" * 60)
    print(f"  Scenario: {title}")
    print("=" * 60)
    print(topo.summary())

    net = Network(
        topo,
        advertise_interval=0.3,
        split_horizon=sh,
        poison_reverse=pr,
        log=lambda m: None,  # quieter; we print costs ourselves
    )
    net.start()
    net.wait_until_converged(stable_for=0.8, timeout=15)
    print("\nConverged before failure:")
    print(net.print_all_tables())

    print("\n>>> Failing link B-C")
    if pr or sh:
        # Good news travels with triggered updates + loop prevention.
        net.fail_link("B", "C", triggered_advertise=True)
    else:
        # Classic count-to-infinity race: do not flood INF immediately.
        # A still believes C is reachable via B and will advertise that
        # stale route back to B, which re-learns C through A and climbs.
        net.fail_link("B", "C", triggered_advertise=False)
        time.sleep(0.05)
        net._advertise_from("A")  # stale advert seeds the loop
    watch_cost_to_c(net, watch, title)

    a_final = net.route_cost("A", "C")
    if pr:
        if a_final >= INF:
            print("\nResult: A correctly marks C unreachable (poison reverse worked).")
        else:
            print(f"\nResult: unexpected — A still has cost {a_final} to C.")
    elif sh:
        if a_final >= INF:
            print("\nResult: A correctly marks C unreachable (split horizon worked).")
        else:
            print(f"\nResult: unexpected — A still has cost {a_final} to C.")
    else:
        if a_final >= INF:
            print("\nResult: reached INF (counted to infinity).")
        else:
            print(f"\nResult: A→C cost is {a_final} (still climbing or stuck in a loop).")

    net.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Count-to-infinity demo")
    parser.add_argument(
        "--watch",
        type=float,
        default=6.0,
        help="Seconds to watch costs after failure",
    )
    parser.add_argument(
        "--mitigation",
        choices=("none", "poison-reverse", "split-horizon"),
        default=None,
        help="Loop mitigation strategy (Section 8 Command 6)",
    )
    parser.add_argument(
        "--only",
        choices=("without", "with", "both"),
        default=None,
        help="Which scenario(s) to run (backward compatibility)",
    )
    args = parser.parse_args()

    if args.mitigation is not None:
        run_scenario(mitigation=args.mitigation, watch=args.watch)
    else:
        only = args.only or "both"
        if only in ("without", "both"):
            run_scenario(mitigation="none", watch=args.watch)
        if only in ("with", "both"):
            run_scenario(mitigation="poison-reverse", watch=min(args.watch, 4.0))

    print("\nDemo complete.")


if __name__ == "__main__":
    main()
