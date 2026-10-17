/* Live DV routing visualizer client */

const $ = (id) => document.getElementById(id);

const els = {
  conn: $("conn-pill"),
  msgs: $("msg-pill"),
  graph: $("graph"),
  graphEmpty: $("graph-empty"),
  graphLoading: $("graph-loading"),
  tables: $("tables"),
  log: $("event-log"),
  topo: $("topo-select"),
  hint: $("action-hint"),
  err: $("action-error"),
  chkSh: $("chk-sh"),
  chkPr: $("chk-pr"),
};

let state = null;
let ws = null;
let layoutCache = {};

function showError(msg) {
  els.err.hidden = !msg;
  els.err.textContent = msg || "";
}

function setHint(msg) {
  els.hint.textContent = msg || "";
}

async function api(path, body) {
  showError("");
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    showError(data.error || `Request failed (${res.status})`);
    throw new Error(data.error || "request failed");
  }
  return data;
}

function connectWs() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
    els.conn.textContent = "Live";
    els.conn.classList.remove("offline");
  };
  ws.onclose = () => {
    els.conn.textContent = "Reconnecting…";
    els.conn.classList.add("offline");
    setTimeout(connectWs, 1200);
  };
  ws.onerror = () => {
    els.conn.textContent = "Offline";
    els.conn.classList.add("offline");
  };
  ws.onmessage = (ev) => {
    try {
      const msg = JSON.parse(ev.data);
      if (msg.type === "state") applyState(msg.data);
    } catch (_) {
      /* ignore */
    }
  };
}

/** Place nodes on an ellipse / circle inside the SVG viewBox. */
function layoutNodes(names) {
  const key = names.slice().sort().join(",");
  if (layoutCache[key]) return layoutCache[key];

  const cx = 320;
  const cy = 210;
  const rx = names.length <= 3 ? 140 : 220;
  const ry = names.length <= 3 ? 110 : 150;
  const pos = {};
  names.forEach((n, i) => {
    const angle = -Math.PI / 2 + (2 * Math.PI * i) / names.length;
    pos[n] = {
      x: cx + rx * Math.cos(angle),
      y: cy + ry * Math.sin(angle),
    };
  });
  layoutCache[key] = pos;
  return pos;
}

function renderGraph(snap) {
  const svg = els.graph;
  const names = Object.keys(snap.routers || {});
  els.graphLoading.hidden = names.length > 0;
  els.graphEmpty.hidden = names.length > 0;
  if (!names.length) {
    svg.innerHTML = "";
    return;
  }

  const pos = layoutNodes(names);
  const parts = [];

  // Links first (under nodes).
  for (const link of snap.links || []) {
    const a = pos[link.a];
    const b = pos[link.b];
    if (!a || !b) continue;
    const midX = (a.x + b.x) / 2;
    const midY = (a.y + b.y) / 2;
    const cls = link.up ? "link-line" : "link-line down";
    parts.push(
      `<line class="${cls}" data-a="${link.a}" data-b="${link.b}" x1="${a.x}" y1="${a.y}" x2="${b.x}" y2="${b.y}" />`
    );
    parts.push(
      `<text class="link-label" x="${midX + 6}" y="${midY - 6}">${link.up ? link.cost : "DOWN"}</text>`
    );
  }

  for (const name of names) {
    const p = pos[name];
    parts.push(
      `<g class="node" data-name="${name}">
        <circle class="node-circle" cx="${p.x}" cy="${p.y}" r="22" />
        <text class="node-label" x="${p.x}" y="${p.y}">${name}</text>
      </g>`
    );
  }

  svg.innerHTML = parts.join("");

  svg.querySelectorAll(".link-line").forEach((el) => {
    el.addEventListener("click", async () => {
      const a = el.dataset.a;
      const b = el.dataset.b;
      const isDown = el.classList.contains("down");
      try {
        if (isDown) {
          await api("/api/restore", { a, b });
          setHint(`Restored link ${a}–${b}`);
        } else {
          await api("/api/fail", { a, b });
          setHint(`Failed link ${a}–${b}`);
        }
      } catch (_) {
        /* shown via showError */
      }
    });
  });
}

function renderTables(snap) {
  const routers = snap.routers || {};
  const names = Object.keys(routers);
  if (!names.length) {
    els.tables.innerHTML = `<p class="muted">No routers yet.</p>`;
    return;
  }
  els.tables.innerHTML = names
    .map((name) => {
      const rows = Object.entries(routers[name])
        .map(([dest, row]) => {
          const cost =
            row.cost === "INF" || row.cost === 16
              ? `<span class="cost-inf">INF</span>`
              : row.cost;
          const hop = row.next_hop == null ? "—" : row.next_hop;
          return `<tr><td>${dest}</td><td>${hop}</td><td>${cost}</td></tr>`;
        })
        .join("");
      return `<div class="table-card">
        <h3>Router ${name}</h3>
        <table>
          <thead><tr><th>Dest</th><th>Next</th><th>Cost</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;
    })
    .join("");
}

function renderLog(snap) {
  const events = (snap.events || []).slice().reverse();
  els.log.innerHTML = events
    .map(
      (e) =>
        `<li><span class="k">${e.kind}</span> ${escapeHtml(e.message)}</li>`
    )
    .join("");
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function applyState(snap) {
  state = snap;
  els.graphLoading.hidden = true;
  els.msgs.textContent = `msgs ${snap.message_count ?? 0}`;
  els.chkSh.checked = !!snap.split_horizon;
  els.chkPr.checked = !!snap.poison_reverse;
  renderGraph(snap);
  renderTables(snap);
  renderLog(snap);
}

async function loadTopology() {
  els.graphLoading.hidden = false;
  setHint("Loading topology…");
  try {
    const data = await api("/api/load", { topology: els.topo.value });
    setHint(`Loaded: ${data.topology}`);
  } catch (_) {
    els.graphLoading.hidden = true;
  }
}

function wireControls() {
  $("btn-load").addEventListener("click", loadTopology);

  els.chkSh.addEventListener("change", async () => {
    try {
      await api("/api/config", { split_horizon: els.chkSh.checked });
      setHint(`Split horizon ${els.chkSh.checked ? "ON" : "OFF"}`);
    } catch (_) {}
  });
  els.chkPr.addEventListener("change", async () => {
    try {
      await api("/api/config", { poison_reverse: els.chkPr.checked });
      setHint(`Poison reverse ${els.chkPr.checked ? "ON" : "OFF"}`);
    } catch (_) {}
  });

  $("btn-fail-ab").addEventListener("click", () =>
    api("/api/fail", { a: "A", b: "B" }).then(() => setHint("Failed A–B")).catch(() => {})
  );
  $("btn-restore-ab").addEventListener("click", () =>
    api("/api/restore", { a: "A", b: "B" }).then(() => setHint("Restored A–B")).catch(() => {})
  );
  $("btn-fail-bc").addEventListener("click", () =>
    api("/api/fail", { a: "B", b: "C" }).then(() => setHint("Failed B–C")).catch(() => {})
  );
  $("btn-restore-bc").addEventListener("click", () =>
    api("/api/restore", { a: "B", b: "C" }).then(() => setHint("Restored B–C")).catch(() => {})
  );

  $("btn-demo-fail").addEventListener("click", async () => {
    els.graphLoading.hidden = false;
    try {
      const data = await api("/api/demo/failure-recovery", {});
      els.topo.value = "default";
      setHint(data.message);
    } catch (_) {
      els.graphLoading.hidden = true;
    }
  });

  $("btn-demo-cti-off").addEventListener("click", async () => {
    els.graphLoading.hidden = false;
    try {
      const data = await api("/api/demo/count-to-infinity", { with_fix: false });
      els.topo.value = "count_to_infinity";
      setHint(data.message);
    } catch (_) {
      els.graphLoading.hidden = true;
    }
  });

  $("btn-demo-cti-on").addEventListener("click", async () => {
    els.graphLoading.hidden = false;
    try {
      const data = await api("/api/demo/count-to-infinity", { with_fix: true });
      els.topo.value = "count_to_infinity";
      setHint(data.message);
    } catch (_) {
      els.graphLoading.hidden = true;
    }
  });
}

async function bootstrap() {
  wireControls();
  connectWs();
  try {
    const res = await fetch("/api/state");
    if (res.ok) {
      applyState(await res.json());
    }
  } catch (_) {
    showError("Could not reach the simulation API.");
  }
}

bootstrap();
