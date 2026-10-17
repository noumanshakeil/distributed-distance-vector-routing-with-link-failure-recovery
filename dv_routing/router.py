"""Router with a distance-vector table and Bellman-Ford updates.

The update rule (classic DV / Bellman-Ford)::

    For each destination D advertised by neighbour N at cost adv[D]:
        candidate = link_cost(self, N) + adv[D]
        if candidate < current_cost(D):
            install route: next_hop = N, cost = candidate

Direct neighbours are also seeded as one-hop routes so the network
can bootstrap without waiting for advertisements.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict, Optional


# Convention: cost >= INF means "unreachable".
INF = 16  # RIP-style infinity; small so count-to-infinity demos finish quickly.


@dataclass
class RouteEntry:
    """One row of a routing table: how to reach a destination."""

    next_hop: Optional[str]
    cost: int

    def is_reachable(self) -> bool:
        return self.cost < INF and self.next_hop is not None


class Router:
    """A single distance-vector router.

    Attributes
    ----------
    name:
        Router identifier (must match topology).
    neighbors:
        Live neighbour → direct link cost (only *up* links).
    table:
        Destination → RouteEntry (next_hop, cost). Always includes self at cost 0.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.neighbors: Dict[str, int] = {}
        self.table: Dict[str, RouteEntry] = {
            name: RouteEntry(next_hop=name, cost=0),
        }
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Neighbour / link management
    # ------------------------------------------------------------------

    def set_neighbor(self, neighbor: str, cost: int) -> None:
        """Add or update a direct neighbour link (link is up)."""
        with self._lock:
            self.neighbors[neighbor] = cost
            # Seed / refresh the one-hop route.
            current = self.table.get(neighbor)
            if current is None or cost < current.cost or current.next_hop == neighbor:
                self.table[neighbor] = RouteEntry(next_hop=neighbor, cost=cost)

    def remove_neighbor(self, neighbor: str) -> None:
        """Neighbour link went down — drop direct adjacency."""
        with self._lock:
            self.neighbors.pop(neighbor, None)

    def invalidate_via(self, dead_neighbor: str) -> bool:
        """Mark every route that used *dead_neighbor* as next hop unreachable.

        Returns True if any row changed (caller should trigger re-advertise).
        """
        changed = False
        with self._lock:
            for dest, entry in list(self.table.items()):
                if dest == self.name:
                    continue
                if entry.next_hop == dead_neighbor:
                    self.table[dest] = RouteEntry(next_hop=None, cost=INF)
                    changed = True
            # Also clear a direct route to the neighbour itself.
            if dead_neighbor in self.table:
                entry = self.table[dead_neighbor]
                if entry.next_hop == dead_neighbor or entry.cost >= INF:
                    self.table[dead_neighbor] = RouteEntry(next_hop=None, cost=INF)
                    changed = True
        return changed

    # ------------------------------------------------------------------
    # Bellman-Ford update from one neighbour advertisement
    # ------------------------------------------------------------------

    def update_from_advertisement(
        self,
        from_neighbor: str,
        advertisement: Dict[str, int],
    ) -> bool:
        """Apply one neighbour's distance vector (Bellman-Ford).

        Parameters
        ----------
        from_neighbor:
            Who sent this advertisement (must be a live neighbour).
        advertisement:
            Mapping destination → advertised cost from that neighbour's view.
            The neighbour may have already applied split horizon / poison
            reverse before sending.

        Returns
        -------
        bool
            True if this router's table changed.
        """
        with self._lock:
            if from_neighbor not in self.neighbors:
                return False  # link is down; ignore stale ads

            link_cost = self.neighbors[from_neighbor]
            changed = False

            for dest, adv_cost in advertisement.items():
                if dest == self.name:
                    continue  # never overwrite the self route

                # --- Bellman-Ford candidate ---
                if adv_cost >= INF:
                    candidate = INF
                else:
                    candidate = link_cost + adv_cost
                    if candidate > INF:
                        candidate = INF

                current = self.table.get(dest)

                if current is None:
                    # New destination learned via this neighbour.
                    if candidate < INF:
                        self.table[dest] = RouteEntry(
                            next_hop=from_neighbor, cost=candidate
                        )
                        changed = True
                    continue

                # Better path → take it.
                if candidate < current.cost:
                    self.table[dest] = RouteEntry(
                        next_hop=from_neighbor, cost=candidate
                    )
                    changed = True
                # Same next-hop refreshed its cost (could go up after a failure).
                elif current.next_hop == from_neighbor and candidate != current.cost:
                    new_hop = from_neighbor if candidate < INF else None
                    self.table[dest] = RouteEntry(next_hop=new_hop, cost=candidate)
                    changed = True
                # Equal cost from another neighbour: keep existing (stable).

            return changed

    # ------------------------------------------------------------------
    # Build an advertisement for a specific neighbour
    # ------------------------------------------------------------------

    def build_advertisement(
        self,
        to_neighbor: str,
        split_horizon: bool = False,
        poison_reverse: bool = False,
    ) -> Dict[str, int]:
        """Build the distance vector we send to *to_neighbor*.

        Split horizon
            Omit routes learned *via* that neighbour (do not advertise
            back the way we learned them).
        Poison reverse
            Still send those routes, but with cost = INF so the neighbour
            immediately drops the loop-forming path.
        """
        with self._lock:
            adv: Dict[str, int] = {}
            for dest, entry in self.table.items():
                cost = entry.cost if entry.cost < INF else INF

                if entry.next_hop == to_neighbor and dest != self.name:
                    if poison_reverse:
                        adv[dest] = INF
                        continue
                    if split_horizon:
                        continue  # omit entirely

                adv[dest] = cost
            return adv

    # ------------------------------------------------------------------
    # Snapshots for printing / UI
    # ------------------------------------------------------------------

    def snapshot(self) -> Dict[str, dict]:
        """Thread-safe copy of the table for display."""
        with self._lock:
            return {
                dest: {
                    "next_hop": entry.next_hop,
                    "cost": entry.cost if entry.cost < INF else "INF",
                }
                for dest, entry in sorted(self.table.items())
            }

    def fingerprint(self) -> tuple:
        """Hashable table state used for convergence detection."""
        with self._lock:
            return tuple(
                (dest, entry.next_hop, entry.cost)
                for dest, entry in sorted(self.table.items())
            )

    def format_table(self) -> str:
        """Pretty ASCII routing table for CLI / logs."""
        with self._lock:
            lines = [f"Router {self.name}"]
            lines.append(f"  {'Dest':<8} {'NextHop':<10} {'Cost':<6}")
            lines.append(f"  {'----':<8} {'-------':<10} {'----':<6}")
            for dest, entry in sorted(self.table.items()):
                hop = entry.next_hop if entry.next_hop is not None else "-"
                cost = str(entry.cost) if entry.cost < INF else "INF"
                lines.append(f"  {dest:<8} {hop:<10} {cost:<6}")
            return "\n".join(lines)
