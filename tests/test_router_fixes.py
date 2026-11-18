import time
from dv_routing.router import Router, RouteEntry, INF


def test_direct_link_fallback_when_indirect_path_fails():
    r = Router("A")
    r.set_neighbor("B", 10)  # Direct link cost 10
    r.set_neighbor("C", 1)   # Direct link cost 1

    # Learn cheaper path to B via C: cost 1 + 1 = 2
    r.update_from_advertisement("C", {"B": 1})
    assert r.table["B"].next_hop == "C"
    assert r.table["B"].cost == 2

    # C advertises B is down (INF)
    changed = r.update_from_advertisement("C", {"B": INF})
    assert changed is True
    # Router A must fall back to its direct link to B (cost 10) instead of being unreachable
    assert r.table["B"].cost == 10
    assert r.table["B"].next_hop == "B"


def test_route_entry_stores_timestamp():
    r = Router("A")
    before = time.time()
    r.set_neighbor("B", 2)
    after = time.time()
    assert hasattr(r.table["B"], "last_updated")
    assert before <= r.table["B"].last_updated <= after


def test_split_horizon_with_unreachable_routes():
    r = Router("B")
    r.set_neighbor("C", 1)
    r.set_neighbor("A", 1)
    r.table["C"] = RouteEntry(next_hop="C", cost=INF)
    # When split horizon is True and poison reverse is False,
    # routes learned via C should be omitted from advertisements to C
    adv_to_c = r.build_advertisement("C", split_horizon=True, poison_reverse=False)
    assert "C" not in adv_to_c

    # Also test via invalidate_via
    r2 = Router("B")
    r2.set_neighbor("C", 1)
    r2.set_neighbor("A", 1)
    r2.invalidate_via("C")
    assert r2.table["C"].next_hop == "C"
    assert r2.table["C"].cost == INF
    adv_to_c2 = r2.build_advertisement("C", split_horizon=True, poison_reverse=False)
    assert "C" not in adv_to_c2
    adv_to_c_pr = r2.build_advertisement("C", split_horizon=True, poison_reverse=True)
    assert adv_to_c_pr["C"] == INF


def test_route_entry_is_reachable():
    entry_ok = RouteEntry(next_hop="B", cost=5)
    assert entry_ok.is_reachable() is True

    entry_no_hop = RouteEntry(next_hop=None, cost=5)
    assert entry_no_hop.is_reachable() is False

    entry_inf = RouteEntry(next_hop="B", cost=INF)
    assert entry_inf.is_reachable() is False
