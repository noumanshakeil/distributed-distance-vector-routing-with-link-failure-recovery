"""Tests for web visualizer observer, background event loop scheduling, REST API, and WebSocket."""

from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path
from typing import Optional

import pytest
from starlette.testclient import TestClient

import web.server as server
from web.server import app, _on_sim_event, get_state, stop_network


def test_sim_event_from_worker_thread_schedules_broadcast():
    """Verify that calling _on_sim_event from a worker thread schedules broadcast_state onto _main_loop."""
    assert hasattr(server, "_main_loop"), "server module must define _main_loop"

    loop = asyncio.new_event_loop()
    called_event = threading.Event()

    async def fake_broadcast():
        called_event.set()

    orig_broadcast = server.broadcast_state
    orig_loop = getattr(server, "_main_loop", None)
    server.broadcast_state = fake_broadcast
    server._main_loop = loop

    def run_loop():
        asyncio.set_event_loop(loop)
        loop.run_forever()

    th_loop = threading.Thread(target=run_loop, daemon=True)
    th_loop.start()

    worker_error: Optional[Exception] = None

    def worker_thread():
        nonlocal worker_error
        try:
            _on_sim_event({"kind": "update", "message": "sim event from worker thread"})
        except Exception as e:
            worker_error = e

    th_worker = threading.Thread(target=worker_thread)
    th_worker.start()
    th_worker.join()

    try:
        assert worker_error is None
        # Must have been scheduled onto loop and executed
        assert called_event.wait(timeout=2.0) is True
    finally:
        loop.call_soon_threadsafe(loop.stop)
        th_loop.join(timeout=1.0)
        server.broadcast_state = orig_broadcast
        server._main_loop = orig_loop


def test_api_topologies_endpoint():
    """Verify /api/topologies returns current topology key and samples."""
    with TestClient(app) as client:
        res = client.get("/api/topologies")
        assert res.status_code == 200
        data = res.json()
        assert "current" in data
        assert "samples" in data
        assert "default" in data["samples"]
        assert "count_to_infinity" in data["samples"]
        assert "triangle" in data["samples"]


def test_api_state_endpoint_sim_mode():
    """Verify /api/state returns full JSON-serializable network state in simulation mode."""
    with TestClient(app) as client:
        res = client.get("/api/state")
        assert res.status_code == 200
        data = res.json()
        assert data.get("running") is True
        assert "routers" in data
        assert "links" in data
        assert "A" in data["routers"]
        assert "B" in data["routers"]


def test_api_fail_restore_and_config_endpoints():
    """Verify /api/fail, /api/restore, and /api/config REST endpoints."""
    with TestClient(app) as client:
        # Test fail
        res_fail = client.post("/api/fail", json={"a": "A", "b": "B"})
        assert res_fail.status_code == 200
        assert res_fail.json() == {"ok": True}

        res_state = client.get("/api/state")
        assert "A-B" in res_state.json().get("down_links", [])

        # Test restore
        res_restore = client.post("/api/restore", json={"a": "A", "b": "B"})
        assert res_restore.status_code == 200
        assert res_restore.json() == {"ok": True}

        res_state2 = client.get("/api/state")
        assert "A-B" not in res_state2.json().get("down_links", [])

        # Test config
        res_cfg = client.post("/api/config", json={"split_horizon": True, "poison_reverse": True})
        assert res_cfg.status_code == 200
        cfg_data = res_cfg.json()
        assert cfg_data.get("ok") is True
        assert cfg_data["state"]["split_horizon"] is True
        assert cfg_data["state"]["poison_reverse"] is True


def test_websocket_connection_and_messages():
    """Verify /ws WebSocket connection, initial state delivery, ping/pong, and refresh."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            # 1. On connect, initial state is pushed
            msg = ws.receive_json()
            assert msg.get("type") == "state"
            assert "routers" in msg.get("data", {})

            # 2. Ping / Pong
            ws.send_json({"type": "ping"})
            reply = ws.receive_json()
            assert reply == {"type": "pong"}

            # 3. Refresh request returns state
            ws.send_json({"type": "refresh"})
            refresh_reply = ws.receive_json()
            assert refresh_reply.get("type") == "state"
            assert "routers" in refresh_reply.get("data", {})


def test_dual_mode_udp_controller_observation():
    """Verify get_state returns assembled snapshot from UDP controller when active."""
    assert hasattr(server, "get_state"), "server module must provide get_state()"
    get_state_fn = getattr(server, "get_state")

    from dv_routing.launcher import launch_network
    topologies_dir = Path(__file__).resolve().parent.parent / "topologies"
    triangle_topo = str(topologies_dir / "triangle.txt")

    base_port = 19410
    controller = launch_network(
        topology_file=triangle_topo,
        interval=0.15,
        mitigation="split-horizon",
        base_port=base_port,
        mode="thread",
    )
    orig_controller = getattr(server, "_controller", None)
    orig_net = server._net
    try:
        # Switch server to observe controller
        server._controller = controller
        server._net = None

        deadline = time.time() + 4.0
        state = None
        while time.time() < deadline:
            state = get_state_fn()
            if (
                state
                and "X" in state.get("routers", {})
                and "Y" in state.get("routers", {})
                and "Z" in state.get("routers", {})
            ):
                break
            time.sleep(0.05)

        assert state is not None
        assert state["running"] is True
        assert "X" in state["routers"]
        assert "Y" in state["routers"]
        assert "Z" in state["routers"]
        assert len(state["links"]) == 3
    finally:
        controller.stop()
        server._controller = orig_controller
        server._net = orig_net


@pytest.mark.asyncio
async def test_stop_network_safe_execution():
    """Verify stop_network safely stops network via threadpool without raising."""
    await stop_network()
    assert server._net is None
