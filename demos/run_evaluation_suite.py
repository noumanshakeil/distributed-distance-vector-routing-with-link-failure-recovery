#!/usr/bin/env python3
"""Automated Evaluation Suite for Distributed Distance-Vector Routing.

Executes test scenarios T1 through T8 from Section 6.2 of the project report:
  * T1: Baseline convergence (logs/t1_baseline.csv)
  * T2: Single-link failure & recovery (logs/t2_failure.csv)
  * T3: Count-to-infinity without fix (logs/t3_loop.csv)
  * T4: Poison reverse enabled (logs/t4_poison.csv)
  * T5: Split horizon enabled (logs/t5_split.csv)
  * T6: Update interval sweep (logs/t6_sweep.csv)
  * T7: Link cost change at runtime (logs/t7_cost_change.csv)
  * T8: Repeated transient link failure (logs/t8_transient.csv)
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dv_routing import INF, Network, load_topology  # noqa: E402


def _get_path_string(net: Network, src: str, dst: str) -> str:
    """Traces current next-hop path from src to dst."""
    curr = src
    path = [curr]
    for _ in range(16):
        if curr == dst:
            break
        entry = net.routers[curr].table.get(dst)
        if not entry or entry.next_hop is None or entry.next_hop in path or entry.cost >= INF:
            break
        curr = entry.next_hop
        path.append(curr)
    return "->".join(path)


def run_t1_baseline(default_topo, output_dir: Path, trials: int, fast: bool) -> None:
    print("  [T1] Running Baseline Convergence...")
    interval = 0.04 if fast else 0.25
    stable_for = 0.1 if fast else 0.6
    timeout = 5.0 if fast else 15.0

    csv_path = output_dir / "t1_baseline.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["trial", "timestamp", "router", "dest", "next_hop", "cost", "converged_time_sec"])

        for trial in range(1, trials + 1):
            net = Network(default_topo, advertise_interval=interval, split_horizon=True, poison_reverse=True, log=lambda m: None)
            t0 = time.monotonic()
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            net.start()
            net.wait_until_converged(stable_for=stable_for, timeout=timeout)
            conv_time = round(time.monotonic() - t0, 4)

            for r in sorted(net.routers):
                for dest, entry in sorted(net.routers[r].table.items()):
                    cost_val = entry.cost if entry.cost < INF else "INF"
                    writer.writerow([trial, now_iso, r, dest, entry.next_hop, cost_val, conv_time])
            net.stop()


def run_t2_failure(default_topo, output_dir: Path, trials: int, fast: bool) -> None:
    print("  [T2] Running Single-Link Failure & Recovery (A-B)...")
    interval = 0.04 if fast else 0.25
    stable_for = 0.1 if fast else 0.6
    timeout = 5.0 if fast else 15.0

    csv_path = output_dir / "t2_failure.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["trial", "timestamp", "event", "route_a_to_b_cost", "route_a_to_b_next_hop", "reroute_time_sec"])

        for trial in range(1, trials + 1):
            net = Network(default_topo, advertise_interval=interval, split_horizon=True, poison_reverse=True, log=lambda m: None)
            net.start()
            net.wait_until_converged(stable_for=stable_for, timeout=timeout)

            # Baseline
            entry_b = net.routers["A"].table.get("B")
            cost_b = entry_b.cost if entry_b and entry_b.cost < INF else "INF"
            hop_b = entry_b.next_hop if entry_b else "-"
            writer.writerow([trial, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "baseline", cost_b, hop_b, 0.0])

            # Fail A-B
            t_f0 = time.monotonic()
            net.fail_link("A", "B", triggered_advertise=True)
            net.wait_until_converged(stable_for=stable_for, timeout=timeout)
            reroute_time = round(time.monotonic() - t_f0, 4)
            entry_f = net.routers["A"].table.get("B")
            cost_f = entry_f.cost if entry_f and entry_f.cost < INF else "INF"
            hop_f = entry_f.next_hop if entry_f else "-"
            writer.writerow([trial, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "link_fail", cost_f, hop_f, reroute_time])

            # Restore A-B
            t_r0 = time.monotonic()
            net.restore_link("A", "B")
            net.wait_until_converged(stable_for=stable_for, timeout=timeout)
            restore_time = round(time.monotonic() - t_r0, 4)
            entry_r = net.routers["A"].table.get("B")
            cost_r = entry_r.cost if entry_r and entry_r.cost < INF else "INF"
            hop_r = entry_r.next_hop if entry_r else "-"
            writer.writerow([trial, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "link_restore", cost_r, hop_r, restore_time])

            net.stop()


def run_t3_loop(cti_topo, output_dir: Path, trials: int, fast: bool) -> None:
    print("  [T3] Running Count-to-Infinity (No Mitigation)...")
    interval = 0.03 if fast else 0.2
    stable_for = 0.08 if fast else 0.5
    timeout = 4.0 if fast else 10.0

    csv_path = output_dir / "t3_loop.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["trial", "step", "time_offset_sec", "cost_a_to_c", "cost_b_to_c"])

        for trial in range(1, trials + 1):
            net = Network(cti_topo, advertise_interval=interval, split_horizon=False, poison_reverse=False, log=lambda m: None)
            net.start()
            net.wait_until_converged(stable_for=stable_for, timeout=timeout)

            # Record initial step
            writer.writerow([trial, 0, 0.0, net.route_cost("A", "C"), net.route_cost("B", "C")])

            t0 = time.monotonic()
            net.fail_link("B", "C", triggered_advertise=False)
            time.sleep(interval / 2.0 if fast else 0.04)
            net._advertise_from("A")

            step = 1
            last_snap = None
            deadline = t0 + (1.2 if fast else 3.5)
            poll_interval = 0.02 if fast else 0.08

            while time.monotonic() < deadline:
                now_offset = round(time.monotonic() - t0, 3)
                cost_a = net.route_cost("A", "C")
                cost_b = net.route_cost("B", "C")
                snap = (cost_a, cost_b)
                if snap != last_snap:
                    last_snap = snap
                    c_a_str = "INF" if cost_a >= INF else cost_a
                    c_b_str = "INF" if cost_b >= INF else cost_b
                    writer.writerow([trial, step, now_offset, c_a_str, c_b_str])
                    step += 1
                    if cost_a >= INF and cost_b >= INF:
                        break
                time.sleep(poll_interval)
            net.stop()


def run_t4_poison(cti_topo, output_dir: Path, trials: int, fast: bool) -> None:
    print("  [T4] Running Poison Reverse Mitigation...")
    interval = 0.03 if fast else 0.2
    stable_for = 0.08 if fast else 0.5
    timeout = 4.0 if fast else 10.0

    csv_path = output_dir / "t4_poison.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["trial", "step", "time_offset_sec", "cost_a_to_c", "cost_b_to_c", "loop_stopped"])

        for trial in range(1, trials + 1):
            net = Network(cti_topo, advertise_interval=interval, split_horizon=True, poison_reverse=True, log=lambda m: None)
            net.start()
            net.wait_until_converged(stable_for=stable_for, timeout=timeout)

            writer.writerow([trial, 0, 0.0, net.route_cost("A", "C"), net.route_cost("B", "C"), False])

            t0 = time.monotonic()
            net.fail_link("B", "C", triggered_advertise=True)

            step = 1
            last_snap = None
            deadline = t0 + (0.8 if fast else 2.0)
            poll_interval = 0.02 if fast else 0.05

            while time.monotonic() < deadline:
                now_offset = round(time.monotonic() - t0, 3)
                cost_a = net.route_cost("A", "C")
                cost_b = net.route_cost("B", "C")
                stopped = cost_a >= INF and cost_b >= INF
                snap = (cost_a, cost_b)
                if snap != last_snap:
                    last_snap = snap
                    c_a_str = "INF" if cost_a >= INF else cost_a
                    c_b_str = "INF" if cost_b >= INF else cost_b
                    writer.writerow([trial, step, now_offset, c_a_str, c_b_str, stopped])
                    step += 1
                    if stopped:
                        break
                time.sleep(poll_interval)
            net.stop()


def run_t5_split(cti_topo, output_dir: Path, trials: int, fast: bool) -> None:
    print("  [T5] Running Split Horizon Mitigation...")
    interval = 0.03 if fast else 0.2
    stable_for = 0.08 if fast else 0.5
    timeout = 4.0 if fast else 10.0

    csv_path = output_dir / "t5_split.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["trial", "step", "time_offset_sec", "cost_a_to_c", "cost_b_to_c", "loop_stopped"])

        for trial in range(1, trials + 1):
            net = Network(cti_topo, advertise_interval=interval, split_horizon=True, poison_reverse=False, log=lambda m: None)
            net.start()
            net.wait_until_converged(stable_for=stable_for, timeout=timeout)

            writer.writerow([trial, 0, 0.0, net.route_cost("A", "C"), net.route_cost("B", "C"), False])

            t0 = time.monotonic()
            net.fail_link("B", "C", triggered_advertise=True)

            step = 1
            last_snap = None
            deadline = t0 + (0.8 if fast else 2.0)
            poll_interval = 0.02 if fast else 0.05

            while time.monotonic() < deadline:
                now_offset = round(time.monotonic() - t0, 3)
                cost_a = net.route_cost("A", "C")
                cost_b = net.route_cost("B", "C")
                stopped = cost_a >= INF and cost_b >= INF
                snap = (cost_a, cost_b)
                if snap != last_snap:
                    last_snap = snap
                    c_a_str = "INF" if cost_a >= INF else cost_a
                    c_b_str = "INF" if cost_b >= INF else cost_b
                    writer.writerow([trial, step, now_offset, c_a_str, c_b_str, stopped])
                    step += 1
                    if stopped:
                        break
                time.sleep(poll_interval)
            net.stop()


def run_t6_sweep(default_topo, output_dir: Path, trials: int, fast: bool) -> None:
    print("  [T6] Running Update Interval Sweep...")
    intervals = [0.03, 0.06, 0.1] if fast else [1.0, 2.0, 5.0]

    csv_path = output_dir / "t6_sweep.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["trial", "interval_sec", "convergence_time_sec", "message_count"])

        for trial in range(1, trials + 1):
            for iv in intervals:
                stable_for = 0.06 if fast else min(iv * 1.0, 2.0)
                timeout = 4.0 if fast else max(15.0, iv * 4)
                net = Network(default_topo, advertise_interval=iv, split_horizon=True, poison_reverse=True, log=lambda m: None)
                t0 = time.monotonic()
                net.start()
                net.wait_until_converged(stable_for=stable_for, timeout=timeout)
                conv_time = round(time.monotonic() - t0, 4)
                msg_count = net.message_count
                writer.writerow([trial, iv, conv_time, msg_count])
                net.stop()


def run_t7_cost_change(default_topo, output_dir: Path, trials: int, fast: bool) -> None:
    print("  [T7] Running Link Cost Change at Runtime (B-C: 1 -> 5)...")
    interval = 0.04 if fast else 0.25
    stable_for = 0.1 if fast else 0.6
    timeout = 5.0 if fast else 15.0

    csv_path = output_dir / "t7_cost_change.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["trial", "old_cost", "new_cost", "old_route", "new_route", "adaptation_time_sec"])

        for trial in range(1, trials + 1):
            net = Network(default_topo, advertise_interval=interval, split_horizon=True, poison_reverse=True, log=lambda m: None)
            net.start()
            net.wait_until_converged(stable_for=stable_for, timeout=timeout)

            old_cost = net.route_cost("A", "C")
            old_route = _get_path_string(net, "A", "C")

            t0 = time.monotonic()
            net.set_link_cost("B", "C", 5)
            net.wait_until_converged(stable_for=stable_for, timeout=timeout)
            adapt_time = round(time.monotonic() - t0, 4)

            new_cost = net.route_cost("A", "C")
            new_route = _get_path_string(net, "A", "C")

            writer.writerow([trial, old_cost, new_cost, old_route, new_route, adapt_time])
            net.stop()


def run_t8_transient(default_topo, output_dir: Path, trials: int, fast: bool) -> None:
    print("  [T8] Running Repeated Transient Link Failure (5 Cycles)...")
    interval = 0.03 if fast else 0.2
    stable_for = 0.06 if fast else 0.4
    timeout = 4.0 if fast else 12.0

    csv_path = output_dir / "t8_transient.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["trial", "cycle", "fail_reconverged", "restore_reconverged", "time_sec"])

        for trial in range(1, trials + 1):
            net = Network(default_topo, advertise_interval=interval, split_horizon=True, poison_reverse=True, log=lambda m: None)
            net.start()
            net.wait_until_converged(stable_for=stable_for, timeout=timeout)

            cycles = 5
            for cycle in range(1, cycles + 1):
                t_c0 = time.monotonic()
                net.fail_link("A", "B", triggered_advertise=True)
                fail_ok = net.wait_until_converged(stable_for=stable_for, timeout=timeout)

                net.restore_link("A", "B")
                rest_ok = net.wait_until_converged(stable_for=stable_for, timeout=timeout)
                cyc_time = round(time.monotonic() - t_c0, 4)

                writer.writerow([trial, cycle, fail_ok, rest_ok, cyc_time])
            net.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Automated Evaluation Suite (T1-T8)")
    parser.add_argument("--output-dir", default=str(ROOT / "logs"), help="Output directory for CSV files")
    parser.add_argument("--trials", type=int, default=5, help="Number of trials per scenario (default: 5)")
    parser.add_argument("--fast", action="store_true", help="Accelerated timing mode for rapid testing")
    args = parser.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    print("=" * 65)
    print("  Automated Evaluation Suite (Section 6.2 Benchmarks)")
    print(f"  Trials: {args.trials} | Mode: {'FAST' if args.fast else 'STANDARD'}")
    print(f"  Output Directory: {out.resolve()}")
    print("=" * 65)

    default_topo = load_topology(ROOT / "topologies" / "default.txt")
    cti_topo = load_topology(ROOT / "topologies" / "count_to_infinity.txt")

    t_start = time.monotonic()

    run_t1_baseline(default_topo, out, args.trials, args.fast)
    run_t2_failure(default_topo, out, args.trials, args.fast)
    run_t3_loop(cti_topo, out, args.trials, args.fast)
    run_t4_poison(cti_topo, out, args.trials, args.fast)
    run_t5_split(cti_topo, out, args.trials, args.fast)
    run_t6_sweep(default_topo, out, args.trials, args.fast)
    run_t7_cost_change(default_topo, out, args.trials, args.fast)
    run_t8_transient(default_topo, out, args.trials, args.fast)

    total_sec = round(time.monotonic() - t_start, 2)
    print("=" * 65)
    print(f"  All scenarios complete in {total_sec}s.")
    print(f"  Benchmark logs saved to: {out.resolve()}")
    print("=" * 65)


if __name__ == "__main__":
    main()
