#!/usr/bin/env python3
"""FastAPI + WebSocket front-end for the DV routing simulation.

Bind to an uncommon port (default 8765). Serves a single-page UI that
shows the live topology graph, per-router tables, and interactive
fail / restore / split-horizon controls.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import socket
import sys
import time
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dv_routing import Network, NetworkLauncherController, load_topology  # noqa: E402

STATIC_DIR = Path(__file__).resolve().parent / "static"
TOPOLOGIES_DIR = ROOT / "topologies"

SAMPLE_TOPOLOGIES = {
    "default": {
        "label": "Default 6-router mesh",
        "path": TOPOLOGIES_DIR / "default.txt",
    },
    "count_to_infinity": {
        "label": "Count-to-infinity line (A–B–C)",
        "path": TOPOLOGIES_DIR / "count_to_infinity.txt",
    },
    "triangle": {
        "label": "Small triangle",
        "path": TOPOLOGIES_DIR / "triangle.txt",
    },
}

app = FastAPI(title="DV Routing Visualizer")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Module-level event loop reference captured at startup
_main_loop: Optional[asyncio.AbstractEventLoop] = None

# Global simulation handles (single session — fine for a viva demo).
_net: Optional[Network] = None
_controller: Optional[NetworkLauncherController] = None
_net_lock = asyncio.Lock()
_ws_clients: set[WebSocket] = set()
_current_topo_key = "default"


def set_main_loop(loop: Optional[asyncio.AbstractEventLoop]) -> None:
    """Explicitly sets main event loop (useful in test setups)."""
    global _main_loop
    _main_loop = loop


def get_net() -> Network:
    """Returns active in-memory network instance or raises RuntimeError."""
    if _net is None:
        raise RuntimeError("Simulation not started")
    return _net


def set_controller(controller: Optional[NetworkLauncherController]) -> None:
    """Sets an active UDP network controller."""
    global _controller
    _controller = controller


def get_controller() -> Optional[NetworkLauncherController]:
    """Returns active UDP network controller if set."""
    return _controller


def query_router_tables_udp(
    router_ports: Dict[str, int],
    host: str = "127.0.0.1",
    timeout: float = 0.4,
) -> Tuple[Dict[str, dict], Dict[str, list[str]]]:
    """Queries router processes via UDP QUERY_TABLE."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if hasattr(socket, "SIO_UDP_CONNRESET"):
        try:
            sock.ioctl(socket.SIO_UDP_CONNRESET, False)
        except Exception:
            pass
    req = json.dumps({"type": "CONTROL", "action": "QUERY_TABLE"}).encode("utf-8")
    for port in router_ports.values():
        try:
            sock.sendto(req, (host, port))
        except Exception:
            pass

    tables: Dict[str, dict] = {}
    dropped_links_map: Dict[str, list[str]] = {}
    deadline = time.time() + timeout
    while len(tables) < len(router_ports) and time.time() < deadline:
        rem = max(0.01, deadline - time.time())
        sock.settimeout(rem)
        try:
            data, _ = sock.recvfrom(65535)
        except (socket.timeout, TimeoutError):
            break
        except (ConnectionResetError, ConnectionRefusedError, OSError):
            break
        try:
            msg = json.loads(data.decode("utf-8"))
        except Exception:
            continue
        if msg.get("type") == "TABLE_RESPONSE":
            r_name = msg.get("router")
            if r_name:
                tables[r_name] = msg.get("table", {})
                dropped_links_map[r_name] = msg.get("dropped_links", [])
    sock.close()
    return tables, dropped_links_map


def discover_external_udp_routers(
    host: str = "127.0.0.1",
    base_port: int = 10001,
    count: int = 6,
    timeout: float = 0.2,
) -> Tuple[Dict[str, dict], Dict[str, list[str]]]:
    """Probes default UDP ports (10001..10006) for active router processes."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if hasattr(socket, "SIO_UDP_CONNRESET"):
        try:
            sock.ioctl(socket.SIO_UDP_CONNRESET, False)
        except Exception:
            pass
    req = json.dumps({"type": "CONTROL", "action": "QUERY_TABLE"}).encode("utf-8")
    for p in range(base_port, base_port + count):
        try:
            sock.sendto(req, (host, p))
        except Exception:
            pass
    tables: Dict[str, dict] = {}
    dropped_links_map: Dict[str, list[str]] = {}
    deadline = time.time() + timeout
    while time.time() < deadline:
        rem = max(0.01, deadline - time.time())
        sock.settimeout(rem)
        try:
            data, _ = sock.recvfrom(65535)
        except (socket.timeout, TimeoutError):
            break
        except (ConnectionResetError, ConnectionRefusedError, OSError):
            break
        try:
            msg = json.loads(data.decode("utf-8"))
        except Exception:
            continue
        if msg.get("type") == "TABLE_RESPONSE":
            r_name = msg.get("router")
            if r_name:
                tables[r_name] = msg.get("table", {})
                dropped_links_map[r_name] = msg.get("dropped_links", [])
    sock.close()
    return tables, dropped_links_map


def assemble_udp_state(
    tables: Dict[str, dict],
    dropped_links_map: Dict[str, list[str]],
    topology_links: Optional[Dict[Tuple[str, str], int]] = None,
    split_horizon: bool = False,
    poison_reverse: bool = False,
    running: bool = True,
) -> dict:
    """Assembles a UI-compatible state snapshot from UDP router responses."""
    links = []
    down_links = []
    if topology_links:
        for edge, cost in topology_links.items():
            a, b = sorted(edge)
            is_down = (b in dropped_links_map.get(a, [])) or (a in dropped_links_map.get(b, []))
            links.append({"a": a, "b": b, "cost": cost, "up": not is_down})
            if is_down:
                down_links.append(f"{a}-{b}")
    else:
        seen_edges = set()
        for r_name, tbl in tables.items():
            for dest, row in tbl.items():
                if dest != r_name:
                    edge = tuple(sorted([r_name, dest]))
                    if edge not in seen_edges:
                        seen_edges.add(edge)
                        a, b = edge
                        is_down = (b in dropped_links_map.get(a, [])) or (a in dropped_links_map.get(b, []))
                        cost = row.get("cost", 1)
                        if isinstance(cost, str) or cost >= 16:
                            cost = 1
                        links.append({"a": a, "b": b, "cost": cost, "up": not is_down})
                        if is_down:
                            down_links.append(f"{a}-{b}")

    return {
        "running": running,
        "split_horizon": split_horizon,
        "poison_reverse": poison_reverse,
        "message_count": 0,
        "routers": tables,
        "links": links,
        "down_links": down_links,
        "events": [],
    }


def get_state() -> dict:
    """Returns the current state snapshot in either simulation or UDP controller mode."""
    if _controller is not None and _controller.is_running():
        tables, dropped = query_router_tables_udp(
            _controller.router_ports,
            host=_controller.host,
            timeout=0.4,
        )
        return assemble_udp_state(
            tables=tables,
            dropped_links_map=dropped,
            topology_links=_controller.topology.links if hasattr(_controller, "topology") else None,
            split_horizon=getattr(_controller, "split_horizon", False),
            poison_reverse=getattr(_controller, "poison_reverse", False),
            running=True,
        )
    if _net is not None and _net.is_running():
        return _net.snapshot()

    # Check external UDP router processes on default ports
    tables, dropped = discover_external_udp_routers()
    if tables:
        topo_links = None
        if _current_topo_key in SAMPLE_TOPOLOGIES:
            try:
                topo = load_topology(SAMPLE_TOPOLOGIES[_current_topo_key]["path"])
                topo_links = topo.links
            except Exception:
                pass
        return assemble_udp_state(
            tables=tables,
            dropped_links_map=dropped,
            topology_links=topo_links,
            running=True,
        )

    if _net is not None:
        return _net.snapshot()

    raise RuntimeError("Simulation not started")


async def broadcast_state() -> None:
    try:
        state = get_state()
    except RuntimeError:
        return
    payload = json.dumps({"type": "state", "data": state})
    dead: list[WebSocket] = []
    for ws in list(_ws_clients):
        try:
            await ws.send_text(payload)
        except Exception:  # noqa: BLE001
            dead.append(ws)
    for ws in dead:
        _ws_clients.discard(ws)


def _on_sim_event(_ev) -> None:
    """Schedule a broadcast from the sim thread onto the captured asyncio loop."""
    global _main_loop
    if _main_loop is not None and _main_loop.is_running():
        asyncio.run_coroutine_threadsafe(broadcast_state(), _main_loop)


async def stop_network() -> None:
    """Safely stop in-memory simulation network without blocking the event loop."""
    global _net
    if _net is not None:
        net = _net
        _net = None
        await asyncio.to_thread(net.stop)


async def start_network(topo_key: str = "default", custom_path: Optional[str] = None) -> dict:
    global _net, _current_topo_key

    async with _net_lock:
        if _net is not None:
            await stop_network()

        if custom_path:
            path = Path(custom_path)
            _current_topo_key = "custom"
        else:
            if topo_key not in SAMPLE_TOPOLOGIES:
                raise ValueError(f"Unknown topology key: {topo_key}")
            path = SAMPLE_TOPOLOGIES[topo_key]["path"]
            _current_topo_key = topo_key

        topo = load_topology(path)
        net = Network(
            topo,
            advertise_interval=0.4,
            split_horizon=False,
            poison_reverse=False,
            log=lambda _m: None,
        )
        net.on_event(lambda ev: _on_sim_event(ev))
        net.start()
        _net = net

    # Give routers a moment, then push state.
    await asyncio.sleep(0.3)
    await broadcast_state()
    return {"ok": True, "topology": _current_topo_key, "summary": topo.summary()}


@app.on_event("startup")
async def _startup() -> None:
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    await start_network("default")


@app.on_event("shutdown")
async def _shutdown() -> None:
    await stop_network()
    global _controller
    if _controller is not None:
        controller = _controller
        _controller = None
        await asyncio.to_thread(controller.stop)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/state")
async def api_state() -> JSONResponse:
    try:
        return JSONResponse(get_state())
    except RuntimeError as exc:
        return JSONResponse({"error": str(exc)}, status_code=503)


@app.get("/api/topologies")
async def api_topologies() -> JSONResponse:
    return JSONResponse(
        {
            "current": _current_topo_key,
            "samples": {
                k: {"label": v["label"]} for k, v in SAMPLE_TOPOLOGIES.items()
            },
        }
    )


@app.post("/api/load")
async def api_load(body: dict) -> JSONResponse:
    key = body.get("topology", "default")
    custom = body.get("path")
    try:
        result = await start_network(key, custom)
        return JSONResponse(result)
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/api/fail")
async def api_fail(body: dict) -> JSONResponse:
    a, b = body.get("a"), body.get("b")
    if not a or not b:
        return JSONResponse({"error": "Need a and b"}, status_code=400)
    if _controller is not None and _controller.is_running():
        _controller.fail_link(a, b)
    elif _net is not None:
        _net.fail_link(a, b)
    else:
        return JSONResponse({"error": "No simulation or controller running"}, status_code=503)
    await broadcast_state()
    return JSONResponse({"ok": True})


@app.post("/api/restore")
async def api_restore(body: dict) -> JSONResponse:
    a, b = body.get("a"), body.get("b")
    if not a or not b:
        return JSONResponse({"error": "Need a and b"}, status_code=400)
    if _controller is not None and _controller.is_running():
        _controller.restore_link(a, b)
    elif _net is not None:
        _net.restore_link(a, b)
    else:
        return JSONResponse({"error": "No simulation or controller running"}, status_code=503)
    await broadcast_state()
    return JSONResponse({"ok": True})


@app.post("/api/config")
async def api_config(body: dict) -> JSONResponse:
    if _controller is not None and _controller.is_running():
        if "split_horizon" in body:
            _controller.split_horizon = bool(body["split_horizon"])
        if "poison_reverse" in body:
            _controller.poison_reverse = bool(body["poison_reverse"])
    elif _net is not None:
        if "split_horizon" in body:
            _net.set_split_horizon(bool(body["split_horizon"]))
        if "poison_reverse" in body:
            _net.set_poison_reverse(bool(body["poison_reverse"]))
    else:
        return JSONResponse({"error": "No simulation running"}, status_code=503)
    await broadcast_state()
    return JSONResponse({"ok": True, "state": get_state()})


@app.post("/api/demo/count-to-infinity")
async def api_demo_cti(body: dict) -> JSONResponse:
    """One-click: load A-B-C, converge, fail B-C, with or without PR."""
    with_fix = bool(body.get("with_fix", False))
    await start_network("count_to_infinity")
    net = get_net()
    net.set_poison_reverse(with_fix)
    # Wait for converge in a thread so we don't block the event loop badly.
    await asyncio.to_thread(net.wait_until_converged, 0.8, 15)
    await broadcast_state()
    await asyncio.sleep(0.4)
    if with_fix:
        net.fail_link("B", "C", triggered_advertise=True)
    else:
        # Seed the classic loop: suppress immediate INF flood, then let A
        # push its stale route to B.
        net.fail_link("B", "C", triggered_advertise=False)
        await asyncio.sleep(0.05)
        net._advertise_from("A")
    await broadcast_state()
    return JSONResponse(
        {
            "ok": True,
            "with_fix": with_fix,
            "message": (
                "Failed B–C with poison reverse ON"
                if with_fix
                else "Failed B–C with poison reverse OFF — watch costs climb"
            ),
        }
    )


@app.post("/api/demo/failure-recovery")
async def api_demo_failure() -> JSONResponse:
    """One-click: default mesh, fail A-B, wait, restore A-B."""
    await start_network("default")
    net = get_net()
    await asyncio.to_thread(net.wait_until_converged, 1.0, 20)
    await broadcast_state()
    net.fail_link("A", "B")
    await broadcast_state()
    return JSONResponse(
        {
            "ok": True,
            "message": "Failed A–B on default mesh. Watch tables, then Restore A–B.",
        }
    )


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    _ws_clients.add(ws)
    try:
        try:
            state = get_state()
            await ws.send_text(json.dumps({"type": "state", "data": state}))
        except RuntimeError:
            pass
        while True:
            # Client may send ping / commands; we also push on events.
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "ping":
                await ws.send_text(json.dumps({"type": "pong"}))
            elif msg.get("type") == "refresh":
                try:
                    state = get_state()
                    await ws.send_text(json.dumps({"type": "state", "data": state}))
                except RuntimeError:
                    pass
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(ws)


# Periodic state push so tables stay fresh even if no events fire.
async def _ticker() -> None:
    while True:
        await asyncio.sleep(0.7)
        if _ws_clients:
            await broadcast_state()


@app.on_event("startup")
async def _start_ticker() -> None:
    asyncio.create_task(_ticker())


def main() -> None:
    parser = argparse.ArgumentParser(description="DV routing live visualizer")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    import uvicorn

    uvicorn.run(
        "web.server:app",
        host=args.host,
        port=args.port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
