# Distributed Distance-Vector Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align the Distance-Vector Routing codebase with the Study and Detailed Scope Report PDF by implementing a distributed multi-process UDP architecture (ports 10001–10006), silence-based failure detection, application DATA packet forwarding, report-compliant CLI and evaluation scripts, while retaining backward compatibility with the existing simulator and fixing identified bugs.

**Architecture:** A dual-mode system where independent router processes (`dv_routing/router_process.py`) communicate over loopback UDP datagram sockets (`dv_routing/udp_transport.py`) on ports 10001–10006, governed by Bellman-Ford path relaxation and silence-based route expiry. A multi-process launcher (`dv_routing/launcher.py`) starts the network; an application DATA forwarding engine (`dv_routing/forwarding.py`) forwards packets hop-by-hop with hop limits; a hybrid CLI (`cli.py`) supports both Section 8 one-shot commands and interactive REPL; and the existing in-process threaded simulator (`dv_routing/network.py`) remains available as a `--sim` fallback.

**Tech Stack:** Python 3.10+, `socket`, `multiprocessing`, `threading`, `dataclasses`, `argparse`, `fastapi`, `uvicorn`, `websockets`, `pytest`.

## Global Constraints

- **Platform:** Windows and Linux compatible; all loopback networking binds to `127.0.0.1`.
- **Port Allocation:** Designated UDP ports 10001–10006 for routers A through F.
- **Metric Limit:** Infinity ($\text{INF}$) is strictly 16; path costs are non-negative integers; self-routes are cost 0.
- **Default Timers:** Periodic advertisement interval = 2.0s; silence detection timeout = 6.0s (3 consecutive missed intervals).
- **Control & Data Schemas:** JSON-encoded UDP datagrams tagged with `type: "ADVERT" | "DATA" | "CONTROL"`.
- **Frontend Guidelines:** Relative paths only (`fetch('/api/...')`); no hardcoded localhost in client JS.
- **Surgical Edits:** Preserve existing functional tests and do not break the current visualizer.

---

### Task 1: Fix Core Router Logic & Update Table Model

**Files:**
- Modify: `dv_routing/router.py`
- Test: `tests/test_router_fixes.py`

**Interfaces:**
- Consumes: `RouteEntry(next_hop: Optional[str], cost: int, last_updated: float = 0.0)`
- Produces: `Router.update_from_advertisement(from_neighbor: str, advertisement: Dict[str, int]) -> bool` with direct link fallback and timestamp updates; `Router.invalidate_via(dead_neighbor: str) -> bool` with next-hop retention for poison reverse / split horizon tracking.

- [ ] **Step 1: Write the failing unit tests for direct-link fallback and Split Horizon next-hop retention**

Create `tests/test_router_fixes.py`:
```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_router_fixes.py -v`
Expected: FAIL due to missing `last_updated` attribute and lack of direct link fallback.

- [ ] **Step 3: Update `RouteEntry` and `Router` in `dv_routing/router.py`**

In `dv_routing/router.py`:
1. Add `last_updated: float = 0.0` to `RouteEntry`:
```python
import time

@dataclass
class RouteEntry:
    next_hop: Optional[str]
    cost: int
    last_updated: float = 0.0

    def is_reachable(self) -> bool:
        return self.cost < INF and self.next_hop is not None
```
2. Update `__init__`, `set_neighbor`, `invalidate_via`, and `update_from_advertisement` to record `time.time()`.
3. In `update_from_advertisement()`, implement direct-link fallback when an indirect candidate becomes `INF`:
```python
if current.next_hop == from_neighbor and candidate != current.cost:
    if candidate >= INF and dest in self.neighbors:
        fallback_cost = self.neighbors[dest]
        self.table[dest] = RouteEntry(next_hop=dest, cost=fallback_cost, last_updated=now)
    else:
        new_hop = from_neighbor if candidate < INF else None
        self.table[dest] = RouteEntry(next_hop=new_hop, cost=candidate, last_updated=now)
    changed = True
```
4. In `build_advertisement()`, handle routes with `next_hop == to_neighbor` properly even when `cost == INF`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_router_fixes.py -v`
Expected: 3 passed.

- [ ] **Step 5: Verify existing tests continue to pass**

Run: `python demos/run_failure_recovery.py` and `python demos/run_count_to_infinity.py`
Expected: Both exit 0 with expected output.

---

### Task 2: Implement UDP Transport & Wire Protocol Abstraction

**Files:**
- Create: `dv_routing/udp_transport.py`
- Test: `tests/test_udp_transport.py`

**Interfaces:**
- Produces:
  - `UDPTransport(host: str, port: int, on_message: Callable[[dict, tuple], None])`
  - `send_json(dest_host: str, dest_port: int, payload: dict) -> None`
  - `close() -> None`
  - Message schema types: `MSG_TYPE_ADVERT = "ADVERT"`, `MSG_TYPE_DATA = "DATA"`, `MSG_TYPE_CONTROL = "CONTROL"`

- [ ] **Step 1: Write failing unit test for UDPTransport**

Create `tests/test_udp_transport.py`:
```python
import time
from dv_routing.udp_transport import UDPTransport

def test_udp_transport_send_receive():
    received = []
    def handler(data, addr):
        received.append((data, addr))

    t1 = UDPTransport(host="127.0.0.1", port=19001, on_message=handler)
    t2 = UDPTransport(host="127.0.0.1", port=19002, on_message=lambda d, a: None)
    t1.start()
    t2.start()

    try:
        t2.send_json("127.0.0.1", 19001, {"type": "ADVERT", "sender": "B", "vector": {"A": 1}})
        deadline = time.time() + 2.0
        while not received and time.time() < deadline:
            time.sleep(0.05)

        assert len(received) == 1
        payload, addr = received[0]
        assert payload["type"] == "ADVERT"
        assert payload["sender"] == "B"
        assert payload["vector"]["A"] == 1
    finally:
        t1.close()
        t2.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_udp_transport.py -v`
Expected: FAIL (ModuleNotFoundError: No module named 'dv_routing.udp_transport').

- [ ] **Step 3: Implement `dv_routing/udp_transport.py`**

Create `dv_routing/udp_transport.py`:
```python
from __future__ import annotations
import json
import socket
import threading
from typing import Callable, Optional, Tuple

MSG_TYPE_ADVERT = "ADVERT"
MSG_TYPE_DATA = "DATA"
MSG_TYPE_CONTROL = "CONTROL"

class UDPTransport:
    def __init__(
        self,
        host: str,
        port: int,
        on_message: Callable[[dict, Tuple[str, int]], None],
    ) -> None:
        self.host = host
        self.port = port
        self.on_message = on_message
        self._sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._running = True
        self._thread = threading.Thread(target=self._listen_loop, daemon=True, name=f"udp-{self.port}")
        self._thread.start()

    def _listen_loop(self) -> None:
        assert self._sock is not None
        while self._running:
            try:
                data, addr = self._sock.recvfrom(65535)
                if not data:
                    continue
                payload = json.loads(data.decode("utf-8"))
                self.on_message(payload, addr)
            except (socket.error, json.JSONDecodeError, UnicodeDecodeError):
                if not self._running:
                    break

    def send_json(self, dest_host: str, dest_port: int, payload: dict) -> None:
        if self._sock is None:
            raise RuntimeError("Transport not started")
        data = json.dumps(payload).encode("utf-8")
        self._sock.sendto(data, (dest_host, dest_port))

    def close(self) -> None:
        self._running = False
        if self._sock is not None:
            try:
                self._sock.close()
            except socket.error:
                pass
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=1.0)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_udp_transport.py -v`
Expected: 1 passed.

---

### Task 3: Implement Application DATA Forwarding Engine

**Files:**
- Create: `dv_routing/forwarding.py`
- Test: `tests/test_forwarding.py`

**Interfaces:**
- Produces:
  - `DataPacket(src: str, dst: str, packet_id: str, hop_limit: int, payload: str, hops: List[str] = field(default_factory=list))`
  - `ForwardingResult(status: str, packet: DataPacket, next_hop: Optional[str], reason: str = "")`
  - `forward_data_packet(packet: DataPacket, current_router: str, routing_table: Dict[str, RouteEntry]) -> ForwardingResult`

- [ ] **Step 1: Write failing unit test for DATA forwarding**

Create `tests/test_forwarding.py`:
```python
from dv_routing.forwarding import DataPacket, forward_data_packet
from dv_routing.router import RouteEntry, INF

def test_forward_packet_success():
    table = {
        "A": RouteEntry("A", 0),
        "D": RouteEntry("B", 3),
    }
    pkt = DataPacket(src="A", dst="D", packet_id="pkt-1", hop_limit=5, payload="Hello")
    res = forward_data_packet(pkt, current_router="A", routing_table=table)
    assert res.status == "FORWARD"
    assert res.next_hop == "B"
    assert res.packet.hop_limit == 4
    assert res.packet.hops == ["A"]

def test_forward_packet_delivered():
    table = {
        "D": RouteEntry("D", 0),
    }
    pkt = DataPacket(src="A", dst="D", packet_id="pkt-2", hop_limit=3, payload="Hello", hops=["A", "B"])
    res = forward_data_packet(pkt, current_router="D", routing_table=table)
    assert res.status == "DELIVERED"
    assert res.packet.hops == ["A", "B", "D"]

def test_forward_packet_hop_limit_expired():
    table = {
        "D": RouteEntry("B", 3),
    }
    pkt = DataPacket(src="A", dst="D", packet_id="pkt-3", hop_limit=1, payload="Hello")
    res = forward_data_packet(pkt, current_router="A", routing_table=table)
    assert res.status == "DROPPED_HOP_LIMIT"
    assert res.packet.hop_limit == 0

def test_forward_packet_unreachable_destination():
    table = {
        "D": RouteEntry(None, INF),
    }
    pkt = DataPacket(src="A", dst="D", packet_id="pkt-4", hop_limit=5, payload="Hello")
    res = forward_data_packet(pkt, current_router="A", routing_table=table)
    assert res.status == "DROPPED_UNREACHABLE"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_forwarding.py -v`
Expected: FAIL (ModuleNotFoundError: No module named 'dv_routing.forwarding').

- [ ] **Step 3: Implement `dv_routing/forwarding.py`**

Create `dv_routing/forwarding.py`:
```python
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from .router import INF, RouteEntry

@dataclass
class DataPacket:
    src: str
    dst: str
    packet_id: str
    hop_limit: int
    payload: str
    hops: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "type": "DATA",
            "src": self.src,
            "dst": self.dst,
            "packet_id": self.packet_id,
            "hop_limit": self.hop_limit,
            "payload": self.payload,
            "hops": list(self.hops),
        }

    @classmethod
    def from_dict(cls, data: dict) -> DataPacket:
        return cls(
            src=data["src"],
            dst=data["dst"],
            packet_id=data["packet_id"],
            hop_limit=int(data["hop_limit"]),
            payload=str(data["payload"]),
            hops=list(data.get("hops", [])),
        )

@dataclass
class ForwardingResult:
    status: str  # "DELIVERED", "FORWARD", "DROPPED_HOP_LIMIT", "DROPPED_UNREACHABLE"
    packet: DataPacket
    next_hop: Optional[str] = None
    reason: str = ""

def forward_data_packet(
    packet: DataPacket,
    current_router: str,
    routing_table: Dict[str, RouteEntry],
) -> ForwardingResult:
    packet.hops.append(current_router)
    if current_router == packet.dst:
        return ForwardingResult(status="DELIVERED", packet=packet, next_hop=None)

    if packet.hop_limit <= 1:
        packet.hop_limit = 0
        return ForwardingResult(
            status="DROPPED_HOP_LIMIT",
            packet=packet,
            reason=f"Hop limit exceeded at router {current_router}",
        )

    packet.hop_limit -= 1
    entry = routing_table.get(packet.dst)
    if entry is None or not entry.is_reachable():
        return ForwardingResult(
            status="DROPPED_UNREACHABLE",
            packet=packet,
            reason=f"Destination {packet.dst} unreachable at router {current_router}",
        )

    return ForwardingResult(
        status="FORWARD",
        packet=packet,
        next_hop=entry.next_hop,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_forwarding.py -v`
Expected: 4 passed.

---

### Task 4: Implement Router Daemon Process & Silence-Based Failure Detection

**Files:**
- Create: `dv_routing/router_process.py`
- Test: `tests/test_router_process.py`

**Interfaces:**
- Produces:
  - `RouterProcess(name: str, port: int, neighbor_ports: Dict[str, int], neighbor_costs: Dict[str, int], interval: float = 2.0, split_horizon: bool = False, poison_reverse: bool = False)`
  - Methods: `start()`, `stop()`, `send_data(...)`, `fail_link_to(neighbor: str)`, `restore_link_to(neighbor: str)`, `set_link_cost(neighbor: str, cost: int)`, `get_table_snapshot() -> dict`
  - Runnable via CLI/subprocess: `python -m dv_routing.router_process --name A --port 10001 ...`

- [ ] **Step 1: Write failing unit test for RouterProcess silence timeout and control operations**

Create `tests/test_router_process.py`:
```python
import time
from dv_routing.router_process import RouterProcess
from dv_routing.router import INF

def test_router_process_startup_and_advertisement():
    # Two routers A and B with fast intervals (0.2s) for testing
    rp_a = RouterProcess("A", port=19011, neighbor_ports={"B": 19012}, neighbor_costs={"B": 1}, interval=0.2)
    rp_b = RouterProcess("B", port=19012, neighbor_ports={"A": 19011}, neighbor_costs={"A": 1}, interval=0.2)
    rp_a.start()
    rp_b.start()

    try:
        # Wait for mutual advertisement
        time.sleep(0.6)
        table_a = rp_a.get_table_snapshot()
        assert table_a["B"]["cost"] == 1
        assert table_a["B"]["next_hop"] == "B"
    finally:
        rp_a.stop()
        rp_b.stop()

def test_router_process_silence_based_failure_detection():
    # 0.2s interval -> 3x interval = 0.6s silence timeout
    rp_a = RouterProcess("A", port=19013, neighbor_ports={"B": 19014}, neighbor_costs={"B": 1}, interval=0.2)
    rp_b = RouterProcess("B", port=19014, neighbor_ports={"A": 19013}, neighbor_costs={"A": 1}, interval=0.2)
    rp_a.start()
    rp_b.start()

    try:
        time.sleep(0.5)
        assert rp_a.get_table_snapshot()["B"]["cost"] == 1

        # Simulate link cut: drop packets from B on A and vice versa without deleting memory edges directly
        rp_a.fail_link_to("B")
        rp_b.fail_link_to("A")

        # Before silence timeout, table still has old route
        time.sleep(0.2)

        # Wait past silence timeout (3 * 0.2 = 0.6s)
        time.sleep(0.6)
        table_a = rp_a.get_table_snapshot()
        assert table_a["B"]["cost"] == INF or table_a["B"]["cost"] == "INF"

        # Restore link delivery
        rp_a.restore_link_to("B")
        rp_b.restore_link_to("A")
        time.sleep(0.6)
        table_a_restored = rp_a.get_table_snapshot()
        assert table_a_restored["B"]["cost"] == 1
    finally:
        rp_a.stop()
        rp_b.stop()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_router_process.py -v`
Expected: FAIL (ModuleNotFoundError: No module named 'dv_routing.router_process').

- [ ] **Step 3: Implement `dv_routing/router_process.py`**

Create `dv_routing/router_process.py`:
- Use `UDPTransport` for network communication on designated port.
- Handle `MSG_TYPE_ADVERT`: if neighbor is dropped in local filter, discard packet. Otherwise, update neighbor's `last_heard` timestamp and invoke `router.update_from_advertisement()`.
- Handle `MSG_TYPE_DATA`: pass to `forward_data_packet()`. If forwarding to neighbor, forward via `UDPTransport.send_json()`. If delivered, log delivery.
- Handle `MSG_TYPE_CONTROL`:
  - `DROP_LINK`: add neighbor to `_dropped_links` set.
  - `RESTORE_LINK`: remove neighbor from `_dropped_links` set.
  - `SET_COST`: update neighbor link cost and trigger Bellman-Ford recomputation.
  - `QUERY_TABLE`: reply with JSON table snapshot to sender address.
- Maintain background timer thread:
  - Check `last_heard` timestamps: if `now - last_heard[n] >= 3 * interval`, mark neighbor unreachable, call `router.invalidate_via(n)`, and trigger updated advertisement broadcast.
  - Every `interval` seconds, transmit DV advertisements to all non-dropped neighbors.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_router_process.py -v`
Expected: 2 passed.

---

### Task 5: Implement Multi-Process Launcher (`launcher.py`)

**Files:**
- Create: `dv_routing/launcher.py`
- Test: `tests/test_launcher.py`

**Interfaces:**
- Produces:
  - CLI execution: `python -m dv_routing.launcher --topology topologies/default.txt --interval 2.0 [--mitigation none|split-horizon|poison-reverse]`
  - Programmatic: `launch_network(topology_file: str, interval: float, mitigation: str) -> NetworkLauncherController`

- [ ] **Step 1: Write failing unit test for `launcher.py`**

Create `tests/test_launcher.py`:
```python
import time
from dv_routing.launcher import launch_network

def test_launcher_starts_and_stops_processes():
    controller = launch_network(
        topology_file="topologies/triangle.txt",
        interval=0.3,
        mitigation="split-horizon",
        base_port=19030,
    )
    try:
        assert controller.is_running()
        time.sleep(1.0)
        tables = controller.query_all_tables()
        assert len(tables) == 3
        assert "A" in tables and "B" in tables and "C" in tables
    finally:
        controller.stop()
        assert not controller.is_running()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_launcher.py -v`
Expected: FAIL (ModuleNotFoundError: No module named 'dv_routing.launcher').

- [ ] **Step 3: Implement `dv_routing/launcher.py`**

Create `dv_routing/launcher.py`:
- Parses `--topology`, `--interval`, `--mitigation`, `--base-port`.
- Uses default port mapping 10001–10006 for routers A through F (or configurable base port).
- Starts each router process using Python `multiprocessing.Process`.
- Provides `NetworkLauncherController` with `query_all_tables()`, `fail_link(a, b)`, `restore_link(a, b)`, `send_data(...)`, `stop()`.
- Catches `SIGINT` / `KeyboardInterrupt` for graceful cleanup.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_launcher.py -v`
Expected: 1 passed.

---

### Task 6: Align Hybrid CLI Controller (`cli.py`)

**Files:**
- Modify: `cli.py`
- Test: `tests/test_cli_commands.py`

**Interfaces:**
- Consumes: `launcher.py`, `udp_transport.py`, `forwarding.py`
- Produces: CLI commands matching Section 8:
  - `python cli.py send --src A --dst D --payload "..."`
  - `python cli.py link-fail --endpoint1 A --endpoint2 B`
  - `python cli.py link-restore --endpoint1 A --endpoint2 B`
  - `python cli.py show path <src> <dst>`
  - `python cli.py link-cost --endpoint1 B --endpoint2 C --cost 5`
  - `python cli.py tables`
  - `python cli.py` / `python cli.py interactive` (REPL)

- [ ] **Step 1: Write failing test for one-shot CLI commands**

Create `tests/test_cli_commands.py`:
```python
import subprocess
import sys
import time

def test_cli_help():
    res = subprocess.run([sys.executable, "cli.py", "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "send" in res.stdout
    assert "link-fail" in res.stdout
    assert "link-restore" in res.stdout

def test_cli_link_fail_args_parsing():
    res = subprocess.run([sys.executable, "cli.py", "link-fail", "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    assert "--endpoint1" in res.stdout
    assert "--endpoint2" in res.stdout
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli_commands.py -v`
Expected: FAIL (unrecognized arguments).

- [ ] **Step 3: Update `cli.py` with `argparse` subcommands and interactive fallback**

Refactor `cli.py` to:
1. Support subcommands when arguments are provided:
   - `send`: `--src`, `--dst`, `--payload`, `--hop-limit`
   - `link-fail`: `--endpoint1`, `--endpoint2`
   - `link-restore`: `--endpoint1`, `--endpoint2`
   - `link-cost`: `--endpoint1`, `--endpoint2`, `--cost`
   - `show`: `subparser` for `path` with `src`, `dst`
   - `tables`: queries and prints all tables
   - `interactive`: launches the interactive REPL
2. When invoked without subcommands (e.g. `python cli.py`), default to interactive REPL while supporting all new commands (`send`, `show path`, `link-cost`, `fail`, `restore`, `tables`, `quit`).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_cli_commands.py -v`
Expected: 2 passed.

---

### Task 7: Fix Web Visualizer Observer & Live Push Bug

**Files:**
- Modify: `web/server.py`
- Test: `tests/test_web_observer.py`

**Interfaces:**
- Produces:
  - Fixed `_on_sim_event` thread-safe scheduling using captured main event loop.
  - Dual-mode support: web server monitors running UDP router processes via UDP `QUERY_TABLE` or wraps in-memory `Network`.

- [ ] **Step 1: Write failing unit test reproducing asyncio loop issue in worker thread**

Create `tests/test_web_observer.py`:
```python
import asyncio
from web.server import _on_sim_event, broadcast_state

def test_sim_event_from_thread_does_not_raise():
    # Calling from worker thread should schedule broadcast_state onto main event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        # Event should succeed without raising or silently dropping
        _on_sim_event({"kind": "update", "message": "test event"})
    finally:
        loop.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_web_observer.py -v`
Expected: FAIL or verify thread failure condition.

- [ ] **Step 3: Fix `web/server.py`**

In `web/server.py`:
1. Store running loop reference during startup:
```python
_main_loop: Optional[asyncio.AbstractEventLoop] = None

@app.on_event("startup")
async def startup_event():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    ...
```
2. In `_on_sim_event`:
```python
def _on_sim_event(_ev) -> None:
    global _main_loop
    if _main_loop is not None and _main_loop.is_running():
        asyncio.run_coroutine_threadsafe(broadcast_state(), _main_loop)
```
3. Add ability to query UDP ports 10001–10006 for live status if running in UDP mode.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_web_observer.py -v`
Expected: 1 passed.

---

### Task 8: Demos & Automated Evaluation Suite (T1–T8 CSV Logs)

**Files:**
- Modify: `demos/run_count_to_infinity.py`
- Modify: `demos/run_failure_recovery.py`
- Create: `demos/run_evaluation_suite.py`
- Create: `docs/captures/README_WIRESHARK.md`
- Test: `tests/test_evaluation_suite.py`

**Interfaces:**
- Produces:
  - `demos/run_count_to_infinity.py --mitigation none|poison-reverse|split-horizon`
  - `demos/run_evaluation_suite.py` generating `logs/t1_baseline.csv` through `logs/t8_transient.csv`
  - `docs/captures/README_WIRESHARK.md` (60-second teammate guide)

- [ ] **Step 1: Write test for demo CLI flags and CSV log generation**

Create `tests/test_evaluation_suite.py`:
```python
import subprocess
import sys
from pathlib import Path

def test_count_to_infinity_mitigation_flag():
    res = subprocess.run(
        [sys.executable, "demos/run_count_to_infinity.py", "--mitigation", "poison-reverse"],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0
    assert "poison reverse" in res.stdout.lower()

def test_evaluation_suite_generates_csv_logs(tmp_path):
    # Run evaluation suite with fast intervals in test mode
    res = subprocess.run(
        [sys.executable, "demos/run_evaluation_suite.py", "--output-dir", str(tmp_path), "--trials", "1", "--fast"],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0
    assert (tmp_path / "t1_baseline.csv").exists()
    assert (tmp_path / "t2_failure.csv").exists()
    assert (tmp_path / "t3_loop.csv").exists()
    assert (tmp_path / "t4_poison.csv").exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_evaluation_suite.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement updates to demos and create `demos/run_evaluation_suite.py`**

1. Update `demos/run_count_to_infinity.py` to accept `--mitigation {none, poison-reverse, split-horizon}` and `--only {without, with, both}` for backward compatibility.
2. Create `demos/run_evaluation_suite.py`:
   - Runs T1 (Baseline convergence), T2 (Single-link failure/recovery via F, cost 7), T3 (Count-to-infinity without fix), T4 (Poison reverse), T5 (Split horizon), T6 (Update interval sweep), T7 (Link cost change at runtime), T8 (Repeated transient link failure).
   - Writes structured data to CSV files in `logs/` directory.
3. Create `docs/captures/README_WIRESHARK.md` detailing:
   - Filter: `udp.port >= 10001 and udp.port <= 10006`
   - Capture steps and save location: `docs/captures/control_overhead.pcapng`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_evaluation_suite.py -v`
Expected: 2 passed.

---

### Task 9: End-to-End System Verification & Regression Suite

**Files:**
- Test: `tests/test_e2e_report_compliance.py`

- [ ] **Step 1: Write end-to-end verification test suite covering Section 8 Demonstration Plan**

Create `tests/test_e2e_report_compliance.py`:
- Test Command 1: `python -m dv_routing.launcher --topology topologies/default.txt --interval 2.0`
- Test Command 2: HTTP connect to `http://127.0.0.1:8765`
- Test Command 3: `python cli.py send --src A --dst D --payload "Test Payload"`
- Test Command 4: `python cli.py link-fail --endpoint1 A --endpoint2 B` (verify silence timeout triggers reroute via F at cost 7)
- Test Command 5: `python cli.py link-restore --endpoint1 A --endpoint2 B` (verify direct route restored at cost 1)
- Test Command 6: `python demos/run_count_to_infinity.py --mitigation none` and `--mitigation poison-reverse`
- Test Requirement R7: runtime link cost change
- Test Requirement R10: `show path A D` prints sequential hop-by-hop path

- [ ] **Step 2: Run full test suite and verify 100% pass rate**

Run: `pytest tests/ -v`
Expected: All tests pass with 0 failures, 0 warnings.

- [ ] **Step 3: Run evaluation suite to generate final CSV logs**

Run: `python demos/run_evaluation_suite.py`
Expected: `logs/t1_baseline.csv` through `logs/t8_transient.csv` generated.
