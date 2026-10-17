"""Topology file format and loader.

File format (lines starting with # are comments)::

    ROUTER <id>
    LINK <router_a> <router_b> <cost>

Undirected links are stored once and applied in both directions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Set, Tuple


@dataclass
class Topology:
    """In-memory undirected graph of routers and link costs."""

    routers: Set[str] = field(default_factory=set)
    # Canonical edge key is a frozenset of the two endpoints.
    links: Dict[frozenset, int] = field(default_factory=dict)

    def add_router(self, name: str) -> None:
        name = name.strip()
        if not name:
            raise ValueError("Router name cannot be empty")
        self.routers.add(name)

    def add_link(self, a: str, b: str, cost: int) -> None:
        if a == b:
            raise ValueError(f"Self-loop not allowed: {a}")
        if cost <= 0:
            raise ValueError(f"Link cost must be positive, got {cost}")
        if a not in self.routers or b not in self.routers:
            missing = {a, b} - self.routers
            raise ValueError(f"Unknown router(s) in LINK: {missing}")
        key = frozenset((a, b))
        self.links[key] = cost

    def neighbors_of(self, router: str) -> Dict[str, int]:
        """Return {neighbor: cost} for an active view of this topology."""
        result: Dict[str, int] = {}
        for edge, cost in self.links.items():
            if router in edge:
                other = next(iter(edge - {router}))
                result[other] = cost
        return result

    def link_list(self) -> List[Tuple[str, str, int]]:
        """Stable sorted list of (a, b, cost) with a < b lexicographically."""
        out: List[Tuple[str, str, int]] = []
        for edge, cost in self.links.items():
            a, b = sorted(edge)
            out.append((a, b, cost))
        return sorted(out)

    def summary(self) -> str:
        lines = [
            f"Routers ({len(self.routers)}): {', '.join(sorted(self.routers))}",
            f"Links ({len(self.links)}):",
        ]
        for a, b, cost in self.link_list():
            lines.append(f"  {a} -- {b}  cost={cost}")
        return "\n".join(lines)


def load_topology(path: str | Path) -> Topology:
    """Parse a topology text file into a Topology object."""
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Topology file not found: {path}")

    topo = Topology()
    with path.open(encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            cmd = parts[0].upper()
            try:
                if cmd == "ROUTER":
                    if len(parts) != 2:
                        raise ValueError("ROUTER expects: ROUTER <id>")
                    topo.add_router(parts[1])
                elif cmd == "LINK":
                    if len(parts) != 4:
                        raise ValueError("LINK expects: LINK <a> <b> <cost>")
                    topo.add_link(parts[1], parts[2], int(parts[3]))
                else:
                    raise ValueError(f"Unknown directive '{parts[0]}'")
            except (ValueError, IndexError) as exc:
                raise ValueError(f"{path}:{lineno}: {exc}") from exc

    if not topo.routers:
        raise ValueError(f"No routers defined in {path}")
    return topo
