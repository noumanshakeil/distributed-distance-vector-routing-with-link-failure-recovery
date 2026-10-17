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
import sys
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dv_routing import Network, load_topology  # noqa: E402

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

# Global simulation handle (single session — fine for a viva demo).
_net: Optional[Network] = None
_net_lock = asyncio.Lock()
_ws_clients: set[WebSocket] = set()
_current_topo_key = "default"


def get_net() -> Network:
    if _net is None:
        raise RuntimeError("Simulation not started")
    return _net


async def broadcast_state() -> None:
    if _net is None:
        return
    payload = json.dumps({"type": "state", "data": _net.snapshot()})
    dead: list[WebSocket] = []
    for ws in list(_ws_clients):
        try:
            await ws.send_text(payload)
        except Exception:  # noqa: BLE001
            dead.append(ws)
    for ws in dead:
        _ws_clients.discard(ws)


def _on_sim_event(_ev) -> None:
    """Schedule a broadcast from the sim thread onto the asyncio loop."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.run_coroutine_threadsafe(broadcast_state(), loop)
    except RuntimeError:
        pass


async def start_network(topo_key: str = "default", custom_path: Optional[str] = None) -> dict:
    global _net, _current_topo_key

    async with _net_lock:
        if _net is not None:
            _net.stop()
            _net = None

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
    await start_network("default")


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _net
    if _net is not None:
        _net.stop()
        _net = None


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/state")
async def api_state() -> JSONResponse:
    try:
        return JSONResponse(get_net().snapshot())
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
    get_net().fail_link(a, b)
    await broadcast_state()
    return JSONResponse({"ok": True})


@app.post("/api/restore")
async def api_restore(body: dict) -> JSONResponse:
    a, b = body.get("a"), body.get("b")
    if not a or not b:
        return JSONResponse({"error": "Need a and b"}, status_code=400)
    get_net().restore_link(a, b)
    await broadcast_state()
    return JSONResponse({"ok": True})


@app.post("/api/config")
async def api_config(body: dict) -> JSONResponse:
    net = get_net()
    if "split_horizon" in body:
        net.set_split_horizon(bool(body["split_horizon"]))
    if "poison_reverse" in body:
        net.set_poison_reverse(bool(body["poison_reverse"]))
    await broadcast_state()
    return JSONResponse({"ok": True, "state": net.snapshot()})


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
        if _net is not None:
            await ws.send_text(json.dumps({"type": "state", "data": _net.snapshot()}))
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
                if _net is not None:
                    await ws.send_text(
                        json.dumps({"type": "state", "data": _net.snapshot()})
                    )
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(ws)


# Periodic state push so tables stay fresh even if no events fire.
async def _ticker() -> None:
    while True:
        await asyncio.sleep(0.7)
        if _net is not None and _ws_clients:
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
