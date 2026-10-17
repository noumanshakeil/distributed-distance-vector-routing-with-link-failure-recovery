"""Distributed distance-vector routing simulation.

Modules
-------
topology
    Load routers and links from a simple text file.
router
    Per-router distance vector table and Bellman-Ford update rule.
network
    Threaded message-passing network: advertise, fail/restore links,
    split horizon / poison reverse, convergence detection.
"""

from .topology import Topology, load_topology
from .router import Router, RouteEntry, INF
from .network import Network

__all__ = [
    "Topology",
    "load_topology",
    "Router",
    "RouteEntry",
    "INF",
    "Network",
]
