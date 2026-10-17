# Distributed Distance Vector Routing with Link Failure Recovery

Python simulation of **distributed distance-vector (DV) routing** with runtime link failure / recovery, optional **split horizon** and **poison reverse**, plus a **live browser topology visualizer**.

Semester project — Computer Networks.

**Team:** Muhammad Nouman Shakeel · Aarish Muhammad · Ebrahim Imran

---

## Features

1. Topology text files + loader (sample 6-router mesh, A–B–C count-to-infinity line, triangle)
2. `Router` class with distance-vector table and a clear **Bellman-Ford** update rule
3. Neighbour advertisements via **in-process threads** and per-router queues
4. Convergence detection — wait until all tables are stable, then print them
5. Runtime `fail` / `restore` of links with before / after / recovered views
6. Toggle **split horizon** / **poison reverse**; scripted demos of count-to-infinity
7. Interactive CLI for arbitrary viva scenarios
8. Live web UI: graph + routing tables + fail/restore + one-click demos
9. Full Markdown report with viva Q&A — see [`REPORT.md`](REPORT.md)

---

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Requires Python 3.10+.

---

## Run the live visualizer

```bash
python -m web.server --host 127.0.0.1 --port 8765
```

Open **http://127.0.0.1:8765**

In the UI you can:

- Load a sample topology
- Click any link to fail / restore it
- Toggle split horizon and poison reverse
- Run one-click demos (failure recovery, count-to-infinity with/without fix)
- Watch routing tables update live

---

## Run the interactive CLI

```bash
python cli.py
```

Useful commands:

| Command | Meaning |
| --- | --- |
| `tables` | Print all routing tables |
| `fail A-B` | Fail link A–B |
| `restore A-B` | Restore link A–B |
| `wait` | Wait for convergence, then print tables |
| `sh on` / `sh off` | Split horizon |
| `pr on` / `pr off` | Poison reverse |
| `status` | Config + down links |
| `quit` | Exit |

---

## Scripted demos

**Link failure and recovery** (default 6-router mesh):

```bash
python demos/run_failure_recovery.py
python demos/run_failure_recovery.py --fail B-E
```

**Count-to-infinity** (A–B–C line — without fix, then with poison reverse):

```bash
python demos/run_count_to_infinity.py
python demos/run_count_to_infinity.py --only without
python demos/run_count_to_infinity.py --only with
```

---

## Topology file format

```text
# Comments start with #
ROUTER A
ROUTER B
LINK A B 1
```

Samples live in [`topologies/`](topologies/).

---

## Project layout

```text
dv_routing/          Core library (topology, router, network)
web/                 FastAPI + WebSocket visualizer
demos/               Scripted failure & count-to-infinity demos
topologies/          Sample networks
cli.py               Interactive viva CLI
REPORT.md            Full project report + viva Q&A
```

---

## Report

See **[REPORT.md](REPORT.md)** for the diagram, how DV works, failure recovery, count-to-infinity, sample logs, and viva questions.
