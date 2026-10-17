# Distributed Distance Vector Routing with Link Failure Recovery

**Course:** Computer Networks  
**Type:** Simulation project (no physical hardware)

## Team

| Name | Role |
| --- | --- |
| Muhammad Nouman Shakeel | Implementation & report |
| Aarish Muhammad | Implementation & report |
| Ebrahim Imran | Implementation & report |

---

## 1. Introduction

Distance-vector routing is how classic protocols such as **RIP** let each router learn paths without a global map. Every router keeps a table of *(destination → next hop, cost)* and periodically tells its neighbours what it knows. Neighbours combine those advertisements with the cost of the direct link — the **Bellman-Ford** step — and the network converges on usable routes.

This project simulates that process in Python:

- Routers exchange advertisements over **in-process threads** (async neighbour chatter without real sockets).
- Links can **fail and recover at runtime**.
- **Split horizon** and **poison reverse** can be toggled to show how they stop **count-to-infinity**.
- A **browser UI** shows the live topology and every routing table for viva demos.

---

## 2. Network diagram (default topology)

Six routers with redundant paths so a single link failure still leaves a route:

```text
        A ----1---- B ----1---- C
        |           |           |
        4           2           3
        |           |           |
        F ----1---- E ----1---- D
```

File: `topologies/default.txt`

For count-to-infinity we use the classic line:

```text
        A ----1---- B ----1---- C
```

File: `topologies/count_to_infinity.txt`

---

## 3. How distance-vector routing works

### 3.1 Routing table

Each router stores, for every destination it knows:

| Field | Meaning |
| --- | --- |
| Destination | Who we want to reach |
| Next hop | Immediate neighbour to forward toward |
| Cost | Total distance (sum of link costs along the chosen path) |

Every router always has a row for itself with cost `0`.

### 3.2 Bellman-Ford update (the viva rule)

When router **R** receives an advertisement from neighbour **N**:

```text
for each destination D in N's advertisement:
    candidate = link_cost(R, N) + advertised_cost(N → D)

    if candidate < R's current cost to D:
        install route: next_hop = N, cost = candidate

    else if R already reaches D via N and the cost changed:
        refresh that row (cost may have gone up after a failure)
```

This is exactly the code path in `dv_routing/router.py` → `update_from_advertisement()`.

### 3.3 Periodic advertisements

Each router runs a **daemon thread** that:

1. Drains a private inbox of neighbour advertisements and applies Bellman-Ford.
2. Periodically builds a distance vector and enqueues it to each live neighbour.

So message passing is concurrent and ordered only by timing — close to how real DV protocols behave, without needing UDP sockets.

### 3.4 Convergence

The network is **converged** when every router's table fingerprint stays unchanged for a short stability window (about one second). The simulator then prints all tables.

---

## 4. Link failure and recovery

### Failure (`fail A-B`)

1. Remove the link from the live link set (packets / ads no longer cross it).
2. Both endpoints drop the neighbour relationship.
3. Any route whose **next hop** was the dead neighbour is set to **unreachable** (`cost = INF`).
4. Both routers immediately re-advertise so others learn the bad news.

### Recovery (`restore A-B`)

1. Put the link back with its original cost.
2. Re-seed the one-hop neighbour routes.
3. Re-advertise; Bellman-Ford picks better paths again as updates flood.

### Demo flow (viva)

```bash
python demos/run_failure_recovery.py
```

Typical story on the default mesh when **A–B** fails:

- **Before:** A reaches B at cost 1 (direct).
- **After failure:** A still reaches B via the long way (A–F–E–B), cost rises.
- **After restore:** Direct A–B cost 1 returns.

The web UI one-click button **“Failure recovery”** runs the same idea live.

---

## 5. Count-to-infinity

### The problem

On the line `A — B — C`, after **B–C** fails:

- B correctly marks C unreachable… for a moment.
- But A still advertises “I can reach C with cost 2” (the old path via B).
- B believes A and installs `C via A, cost 3`.
- A then learns from B and installs `C via B, cost 4`.
- Costs climb toward infinity — a **routing loop**.

### Split horizon

Do **not** advertise a route back to the neighbour you learned it from.  
A will not tell B “I can reach C” if A’s next hop for C is B.

### Poison reverse (stronger)

Still send the route, but advertise cost **INF** toward the neighbour you learned it from.  
B immediately learns that A’s path is poisoned and does not bounce the route.

### Demo

```bash
python demos/run_count_to_infinity.py
```

Or in the UI:

1. **Count-to-∞ without fix** — watch A→C and B→C costs climb.
2. **Count-to-∞ with poison reverse** — C becomes INF quickly on A and B.

Infinity in this sim is **16** (RIP-style), so demos finish in a few seconds.

---

## 6. How to run

### Install

```bash
pip install -r requirements.txt
```

### Live visualizer (recommended for viva)

```bash
python -m web.server --port 8765
```

Open http://127.0.0.1:8765

### Interactive CLI

```bash
python cli.py
```

### Scripted demos

```bash
python demos/run_failure_recovery.py
python demos/run_count_to_infinity.py
```

---

## 7. Sample logs

### Failure recovery (excerpt)

```text
Routers (6): A, B, C, D, E, F
Links (7):
  A -- B  cost=1
  ...

[info] Network started — routers advertising.
[info] Network converged (tables stable).

=== BEFORE FAILURE ===
Router A
  Dest     NextHop    Cost
  A        A          0
  B        B          1
  ...

[fail] Link A-B FAILED
[info] Network converged (tables stable).

=== AFTER FAILURE ===
Router A
  Dest     NextHop    Cost
  B        F          7      # via F–E–B after direct link died
  ...

[restore] Link A-B RESTORED (cost=1)
=== AFTER RECOVERY ===
Router A
  B        B          1      # direct path back
```

### Count-to-infinity without fix (excerpt)

```text
>>> Failing link B-C
  t+0.0s  A→C=  2  B→C=  3
  t+0.1s  A→C=  6  B→C=  7
  t+0.2s  A→C= 10  B→C= 11
  t+0.3s  A→C= 14  B→C= 13
  t+0.4s  A→C=INF  B→C=INF
Result: reached INF (counted to infinity).
```

### With poison reverse

```text
>>> Failing link B-C
  t+0.0s  A→C=  2  B→C=INF
  t+0.1s  A→C=INF  B→C=INF
Result: A correctly marks C unreachable (poison reverse worked).
```

---

## 8. Module map (for code walkthrough)

| Module | Responsibility |
| --- | --- |
| `dv_routing/topology.py` | Parse `ROUTER` / `LINK` files |
| `dv_routing/router.py` | Table + Bellman-Ford + SH/PR advertisement build |
| `dv_routing/network.py` | Threads, queues, fail/restore, convergence |
| `cli.py` | Interactive viva control |
| `web/server.py` | FastAPI + WebSocket API |
| `web/static/` | Live graph + tables UI |
| `demos/` | Scripted failure & CTI scenarios |

---

## 9. Viva Q&A

**Q1. What is a distance vector?**  
A: A list each router maintains of distances (costs) to all known destinations, plus the next hop for each. It does **not** store the full path — only “how far” and “which neighbour first”.

**Q2. Why is the update called Bellman-Ford?**  
A: Because the candidate cost is `link_to_neighbour + neighbour’s_cost_to_dest`, which is the distributed form of the Bellman-Ford relaxation used in shortest-path algorithms.

**Q3. How do routers talk in this project?**  
A: Each router has a background thread and an inbox queue. Advertisements are Python objects passed between threads — an in-process model of asynchronous neighbour exchange.

**Q4. What happens when a link fails?**  
A: The link is removed from the live set, both ends drop the neighbour, routes that used that neighbour as next hop become INF, and fresh advertisements spread the bad news until tables reconverge on alternate paths (if any).

**Q5. What is count-to-infinity?**  
A: After a failure, two routers may keep advertising a destination through each other, so the reported cost increases step by step instead of immediately becoming unreachable.

**Q6. Split horizon vs poison reverse?**  
A: Split horizon **omits** routes learned via a neighbour when advertising back to that neighbour. Poison reverse still sends them but with cost **INF**, so the neighbour drops the loop faster.

**Q7. Why set infinity to 16?**  
A: Same idea as RIP: a small infinity makes “counting to infinity” finish quickly in demos, while still being larger than any legitimate path cost in our sample topologies.

**Q8. Does this simulate real RIP packets?**  
A: No — it is an educational simulation of the **algorithm and failure behaviour**, not a wire-compatible RIP stack. That keeps the update rule readable for viva.

**Q9. How would you show recovery in the viva?**  
A: Load the default mesh → show tables → fail A–B → wait / show new next hops → restore A–B → show direct costs return. Same flow works in CLI or the browser UI.

**Q10. What if there is no alternate path?**  
A: After failure and convergence, destinations beyond the cut become INF on the isolated side (as in the A–B–C demo when B–C is cut and poison reverse is on).

---

## 10. Conclusion

We built a complete, readable distance-vector simulator with threaded advertisements, runtime link failure/recovery, split horizon / poison reverse, scripted demos, an interactive CLI, and a live browser visualization. The Bellman-Ford update rule is isolated in `Router.update_from_advertisement` so it can be explained line-by-line in a viva.
