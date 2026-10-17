"""Threaded distance-vector network simulation.

Each router runs a background worker thread that:
  1. Drains a per-router inbox of advertisements from neighbours.
  2. Periodically pushes its own distance vector to live neighbours.

Link failure / restore and split-horizon toggles are applied under a
network-wide lock so the UI and CLI can mutate the live graph safely.
"""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Tuple

from .router import INF, Router
from .topology import Topology


LogFn = Callable[[str], None]


@dataclass
class Advertisement:
    """One DV message: sender → receiver with a distance vector payload."""

    sender: str
    receiver: str
    vector: Dict[str, int]


@dataclass
class NetworkEvent:
    """Structured event for the live visualization / logs."""

    kind: str
    message: str
    ts: float = field(default_factory=time.time)


class Network:
    """In-process multi-router DV network using threads + queues."""

    def __init__(
        self,
        topology: Topology,
        advertise_interval: float = 0.4,
        split_horizon: bool = False,
        poison_reverse: bool = False,
        log: Optional[LogFn] = None,
    ) -> None:
        self.topology = topology
        self.advertise_interval = advertise_interval
        self.split_horizon = split_horizon
        self.poison_reverse = poison_reverse
        self._log_fn = log or (lambda msg: None)

        self.routers: Dict[str, Router] = {
            name: Router(name) for name in topology.routers
        }
        # Live link set: frozenset({a,b}) → cost. Starts as full topology.
        self.live_links: Dict[frozenset, int] = dict(topology.links)

        self._inboxes: Dict[str, queue.Queue] = {
            name: queue.Queue() for name in self.routers
        }
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        self._running = False

        self.events: List[NetworkEvent] = []
        self._event_listeners: List[Callable[[NetworkEvent], None]] = []
        self.message_count = 0

        self._wire_neighbors()

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def _wire_neighbors(self) -> None:
        """Install direct neighbour costs from currently live links."""
        for router in self.routers.values():
            router.neighbors.clear()
        for edge, cost in self.live_links.items():
            a, b = tuple(edge)
            self.routers[a].set_neighbor(b, cost)
            self.routers[b].set_neighbor(a, cost)

    def on_event(self, callback: Callable[[NetworkEvent], None]) -> None:
        self._event_listeners.append(callback)

    def log(self, kind: str, message: str) -> None:
        ev = NetworkEvent(kind=kind, message=message)
        with self._lock:
            self.events.append(ev)
            # Keep a bounded history for the UI.
            if len(self.events) > 500:
                self.events = self.events[-400:]
        self._log_fn(f"[{kind}] {message}")
        for cb in list(self._event_listeners):
            try:
                cb(ev)
            except Exception:  # noqa: BLE001 — UI listeners must not kill sim
                pass

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._running:
            return
        self._stop.clear()
        self._running = True
        for name in self.routers:
            t = threading.Thread(
                target=self._router_loop,
                args=(name,),
                name=f"router-{name}",
                daemon=True,
            )
            self._threads.append(t)
            t.start()
        self.log("info", "Network started — routers advertising.")
        # Kick an immediate full advertise so convergence begins without
        # waiting for the first interval tick.
        self._advertise_all()

    def stop(self) -> None:
        self._stop.set()
        self._running = False
        for t in self._threads:
            t.join(timeout=2.0)
        self._threads.clear()
        self.log("info", "Network stopped.")

    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Router worker thread
    # ------------------------------------------------------------------

    def _router_loop(self, name: str) -> None:
        inbox = self._inboxes[name]
        router = self.routers[name]
        next_adv = time.monotonic()

        while not self._stop.is_set():
            # Process pending advertisements (non-blocking drain).
            processed = 0
            while processed < 64:
                try:
                    msg: Advertisement = inbox.get_nowait()
                except queue.Empty:
                    break
                changed = router.update_from_advertisement(msg.sender, msg.vector)
                processed += 1
                if changed:
                    self.log(
                        "update",
                        f"{name} updated table from advert by {msg.sender}",
                    )
                    # Triggered update: re-advertise promptly.
                    self._advertise_from(name)

            now = time.monotonic()
            if now >= next_adv:
                self._advertise_from(name)
                next_adv = now + self.advertise_interval

            time.sleep(0.05)

    def _advertise_from(self, name: str) -> None:
        with self._lock:
            router = self.routers[name]
            neighbors = list(router.neighbors.items())
            sh = self.split_horizon
            pr = self.poison_reverse

        for neighbor, _cost in neighbors:
            vector = router.build_advertisement(
                neighbor, split_horizon=sh, poison_reverse=pr
            )
            self._deliver(Advertisement(sender=name, receiver=neighbor, vector=vector))

    def _advertise_all(self) -> None:
        for name in list(self.routers):
            self._advertise_from(name)

    def _deliver(self, msg: Advertisement) -> None:
        # Drop if the link is currently down.
        edge = frozenset((msg.sender, msg.receiver))
        with self._lock:
            if edge not in self.live_links:
                return
            self.message_count += 1
            inbox = self._inboxes[msg.receiver]
        inbox.put(msg)

    # ------------------------------------------------------------------
    # Link failure / recovery
    # ------------------------------------------------------------------

    def fail_link(self, a: str, b: str, *, triggered_advertise: bool = True) -> None:
        """Take link a–b down.

        Parameters
        ----------
        triggered_advertise:
            If True (default), endpoints immediately re-advertise after
            invalidating routes. Set False for count-to-infinity demos so a
            neighbour can still push a *stale* route before INF propagates —
            that is the classic race that creates the loop.
        """
        edge = frozenset((a, b))
        with self._lock:
            if edge not in self.live_links:
                self.log("warn", f"Link {a}-{b} is already down or unknown.")
                return
            del self.live_links[edge]
            self.routers[a].remove_neighbor(b)
            self.routers[b].remove_neighbor(a)
            changed_a = self.routers[a].invalidate_via(b)
            changed_b = self.routers[b].invalidate_via(a)

        self.log("fail", f"Link {a}-{b} FAILED")
        if triggered_advertise:
            if changed_a:
                self._advertise_from(a)
            if changed_b:
                self._advertise_from(b)

    def restore_link(self, a: str, b: str) -> None:
        edge = frozenset((a, b))
        with self._lock:
            if edge not in self.topology.links:
                self.log("warn", f"Link {a}-{b} is not in the topology.")
                return
            if edge in self.live_links:
                self.log("warn", f"Link {a}-{b} is already up.")
                return
            cost = self.topology.links[edge]
            self.live_links[edge] = cost
            self.routers[a].set_neighbor(b, cost)
            self.routers[b].set_neighbor(a, cost)

        self.log("restore", f"Link {a}-{b} RESTORED (cost={cost})")
        self._advertise_from(a)
        self._advertise_from(b)

    def set_split_horizon(self, enabled: bool) -> None:
        with self._lock:
            self.split_horizon = enabled
            if enabled:
                # Poison reverse implies split horizon semantics.
                pass
        self.log("config", f"Split horizon = {enabled}")

    def set_poison_reverse(self, enabled: bool) -> None:
        with self._lock:
            self.poison_reverse = enabled
            if enabled:
                self.split_horizon = True  # PR is a form of SH
        self.log("config", f"Poison reverse = {enabled} (split_horizon={self.split_horizon})")

    # ------------------------------------------------------------------
    # Convergence / snapshots
    # ------------------------------------------------------------------

    def wait_until_converged(
        self,
        stable_for: float = 1.0,
        timeout: float = 30.0,
        poll: float = 0.15,
    ) -> bool:
        """Block until routing tables stop changing for *stable_for* seconds.

        Returns True on convergence, False on timeout.
        """
        deadline = time.monotonic() + timeout
        last_fp: Optional[tuple] = None
        stable_since: Optional[float] = None

        while time.monotonic() < deadline:
            fp = self.tables_fingerprint()
            now = time.monotonic()
            if fp == last_fp:
                if stable_since is not None and (now - stable_since) >= stable_for:
                    self.log("info", "Network converged (tables stable).")
                    return True
                if stable_since is None:
                    stable_since = now
            else:
                last_fp = fp
                stable_since = now
            time.sleep(poll)

        self.log("warn", "Convergence wait timed out.")
        return False

    def tables_fingerprint(self) -> tuple:
        return tuple(
            (name, self.routers[name].fingerprint())
            for name in sorted(self.routers)
        )

    def print_all_tables(self) -> str:
        blocks = [self.routers[n].format_table() for n in sorted(self.routers)]
        text = "\n\n".join(blocks)
        self._log_fn(text)
        return text

    def snapshot(self) -> dict:
        """Full JSON-serialisable state for the web UI."""
        with self._lock:
            links = []
            down = []
            for edge, cost in self.topology.links.items():
                a, b = sorted(edge)
                item = {"a": a, "b": b, "cost": cost, "up": edge in self.live_links}
                links.append(item)
                if edge not in self.live_links:
                    down.append(f"{a}-{b}")
            return {
                "running": self._running,
                "split_horizon": self.split_horizon,
                "poison_reverse": self.poison_reverse,
                "message_count": self.message_count,
                "routers": {
                    name: self.routers[name].snapshot()
                    for name in sorted(self.routers)
                },
                "links": links,
                "down_links": down,
                "events": [
                    {"kind": e.kind, "message": e.message, "ts": e.ts}
                    for e in self.events[-40:]
                ],
            }

    def route_cost(self, src: str, dest: str) -> int:
        entry = self.routers[src].table.get(dest)
        if entry is None:
            return INF
        return entry.cost
