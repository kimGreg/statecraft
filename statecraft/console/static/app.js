// The statecraft console: one page, no build step. Views read the JSON API; the run views
// follow the engine's event log (server-sent events) and redraw what changed.
"use strict";

const $ = (sel, el = document) => el.querySelector(sel);
const main = $("#main");
const state = { registry: null, run: null, agent: null, filesPrefix: "", source: null, feed: null };

function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === "class") el.className = v;
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else if (v !== undefined && v !== null && v !== false) el.setAttribute(k, v);
  }
  for (const c of children.flat()) {
    if (c === null || c === undefined || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}

// replaceChildren with what h() accepts: nested arrays flattened, nulls and false left out
function fill(el, ...children) {
  el.replaceChildren(...children.flat(Infinity).filter((c) => c !== null && c !== undefined && c !== false)
    .map((c) => (c instanceof Node ? c : document.createTextNode(String(c)))));
  return el;
}

async function api(path, options) {
  const r = await fetch(path, options);
  if (r.status === 204) return null;
  const body = await r.json();
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body;
}

const post = (path, body) =>
  api(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });

const pretty = (v) => JSON.stringify(v, null, 2);
const viewTag = (v) => h("span", { class: `pill v-${v}` }, v);
const kindTag = (k) => h("span", { class: `tag ${k === "agentic" ? "agentic" : k === "skill" ? "skill" : ""}` }, k);

function setLive(on, text) {
  $("#live-dot").classList.toggle("on", on);
  $("#live-text").textContent = text;
}

// ---- layered graph layout ------------------------------------------------------------------
// Longest-path layering from the sources (cycles broken by DFS order), then nodes ordered in
// each layer by the mean position of their parents. Returns an <svg>.
function layeredGraph(nodes, edges, opts = {}) {
  const ids = nodes.map((n) => n.id);
  const out = new Map(ids.map((i) => [i, []]));
  const inc = new Map(ids.map((i) => [i, []]));
  for (const e of edges) {
    if (!out.has(e.from) || !out.has(e.to) || e.from === e.to) continue;
    out.get(e.from).push(e.to);
    inc.get(e.to).push(e.from);
  }
  const layer = new Map();
  const visiting = new Set();
  const depth = (id) => {
    if (layer.has(id)) return layer.get(id);
    if (visiting.has(id)) return 0;
    visiting.add(id);
    const parents = inc.get(id).filter((p) => !visiting.has(p) || layer.has(p));
    const d = parents.length ? Math.max(...parents.map(depth)) + 1 : 0;
    visiting.delete(id);
    layer.set(id, d);
    return d;
  };
  const starts = opts.start ? [opts.start] : [];
  starts.forEach((s) => layer.set(s, 0));
  ids.forEach(depth);
  const layers = [];
  for (const id of ids) (layers[layer.get(id)] ||= []).push(id);
  const pos = new Map();
  layers.forEach((col, li) => {
    if (li > 0) {
      const score = (id) => {
        const ps = inc.get(id).filter((p) => pos.has(p));
        return ps.length ? ps.reduce((a, p) => a + pos.get(p).i, 0) / ps.length : 1e9;
      };
      col.sort((a, b) => score(a) - score(b));
    }
    col.forEach((id, i) => pos.set(id, { li, i }));
  });
  const W = opts.nodeWidth || 170, H = 30, GX = opts.gapX || 70, GY = 14;
  const horizontal = opts.horizontal !== false;
  const maxRows = Math.max(1, ...layers.map((c) => (c ? c.length : 0)));
  const width = horizontal ? layers.length * (W + GX) + 20 : maxRows * (W + 20) + 20;
  const height = horizontal ? maxRows * (H + GY) + 20 : layers.length * (H + 50) + 20;
  const xy = (id) => {
    const p = pos.get(id);
    return horizontal ? { x: 10 + p.li * (W + GX), y: 10 + p.i * (H + GY) }
                      : { x: 10 + p.i * (W + 20), y: 10 + p.li * (H + 50) };
  };
  const NS = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(NS, "svg");
  svg.setAttribute("width", width);
  svg.setAttribute("height", height);
  const defs = document.createElementNS(NS, "defs");
  defs.innerHTML = '<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="currentColor" opacity=".6"/></marker>';
  svg.append(defs);
  for (const e of edges) {
    if (!pos.has(e.from) || !pos.has(e.to)) continue;
    const a = xy(e.from), b = xy(e.to);
    const path = document.createElementNS(NS, "path");
    let d;
    if (horizontal) {
      const x1 = a.x + W, y1 = a.y + H / 2, x2 = b.x, y2 = b.y + H / 2;
      const back = x2 <= x1;
      d = back ? `M${a.x + W / 2},${a.y + H} C${a.x + W / 2},${a.y + H + 40} ${b.x + W / 2},${b.y + H + 40} ${b.x + W / 2},${b.y + H}`
               : `M${x1},${y1} C${x1 + GX / 2},${y1} ${x2 - GX / 2},${y2} ${x2},${y2}`;
    } else {
      const x1 = a.x + W / 2, y1 = a.y + H, x2 = b.x + W / 2, y2 = b.y;
      d = `M${x1},${y1} C${x1},${y1 + 25} ${x2},${y2 - 25} ${x2},${y2}`;
    }
    path.setAttribute("d", d);
    path.setAttribute("class", `edge ${e.kind || ""} ${e.walked ? "walked" : ""}`);
    path.setAttribute("marker-end", "url(#arrow)");
    const title = document.createElementNS(NS, "title");
    title.textContent = `${e.from} → ${e.to} (${e.kind || ""})`;
    path.append(title);
    svg.append(path);
  }
  for (const n of nodes) {
    const p = xy(n.id);
    const g = document.createElementNS(NS, "g");
    g.setAttribute("class", `node ${n.type || ""} ${n.cls || ""}`);
    g.setAttribute("transform", `translate(${p.x},${p.y})`);
    const rect = document.createElementNS(NS, "rect");
    rect.setAttribute("width", W);
    rect.setAttribute("height", H);
    if (n.color) rect.style.stroke = n.color;
    const text = document.createElementNS(NS, "text");
    text.setAttribute("x", 9);
    text.setAttribute("y", 19);
    const label = n.label.length > 24 ? n.label.slice(0, 23) + "…" : n.label;
    text.textContent = (n.icon ? n.icon + " " : "") + label;
    const title = document.createElementNS(NS, "title");
    title.textContent = n.title || n.id;
    g.append(rect, text, title);
    if (opts.onClick) g.addEventListener("click", () => opts.onClick(n));
    svg.append(g);
  }
  return svg;
}

const VIEW_COLOR = { succeeded: "#1f9d55", running: "#3559e0", suspended: "#c98a06", pending: "#c98a06",
                     terminated: "#d03b3b", runnable: "#3559e0" };

// ---- overview ------------------------------------------------------------------------------
async function showOverview() {
  const [o, runs] = await Promise.all([api("/api/overview"), api("/api/runs")]);
  $("#title").textContent = o.title;
  const card = (n, label) => h("div", { class: "card" }, h("div", { class: "n" }, n), h("div", { class: "dim" }, label));
  const feed = h("div", { class: "events", id: "feed" });
  fill(main, 
    h("div", { class: "grid cards" }, card(o.features, "features"), card(o.agents, "agents"),
      card(o.servers.length, "tool servers"), card(o.runs, "runs"), card(o.active, "active runs"),
      card(o.questions, "open questions")),
    h("div", { class: "grid", style: "grid-template-columns: 1fr 1fr; margin-top: 14px" },
      h("div", { class: "panel" }, h("h2", {}, "Recent runs"), runsTable(runs.slice(-12).reverse())),
      h("div", { class: "panel" }, h("h2", {}, "Live events (all runs)"), feed)));
  followAll(feed);
}

function runsTable(runs) {
  if (!runs.length) return h("div", { class: "empty" }, "No runs yet.");
  return h("table", {},
    h("tr", {}, h("th", {}, "run"), h("th", {}, "agent"), h("th", {}, "status"), h("th", {}, "agents"), h("th", {}, "questions")),
    runs.map((r) => h("tr", { class: "clickable", onclick: () => (location.hash = `#/runs/${encodeURIComponent(r.id)}`) },
      h("td", {}, h("code", {}, r.id)), h("td", {}, r.agent), h("td", {}, viewTag(r.view)),
      h("td", {}, `${r.agents}  `, h("span", { class: "dim" }, Object.entries(r.views).map(([k, v]) => `${k} ${v}`).join(" · "))),
      h("td", {}, r.questions || ""))));
}

function followAll(feed) {
  closeStreams();
  const src = new EventSource("/api/events");
  state.feed = src;
  src.onopen = () => setLive(true, "live");
  src.onerror = () => setLive(false, "reconnecting");
  const add = (e) => {
    const ev = JSON.parse(e.data);
    feed.prepend(h("div", {}, h("span", { class: "dim" }, `#${ev.seq} `), h("b", {}, ev.kind), " ",
      h("code", {}, ev.agent || ""), ev.state ? ` → ${ev.state}` : "", ev.status ? ` (${ev.status})` : ""));
    while (feed.childElementCount > 200) feed.lastChild.remove();
  };
  src.onmessage = add;
}

function closeStreams() {
  for (const k of ["source", "feed"]) if (state[k]) { state[k].close(); state[k] = null; }
  setLive(false, "idle");
}

// ---- registry ------------------------------------------------------------------------------
async function loadRegistry() {
  state.registry ||= await api("/api/registry");
  return state.registry;
}

function allAgents(reg) {
  return [...reg.features.flatMap((f) => f.agents.map((a) => ({ ...a, feature: f.name }))),
          ...reg.extra.map((a) => ({ ...a, feature: "(extra)" })),
          ...reg.builtins.map((a) => ({ ...a, feature: "(built-in)" }))];
}

async function showRegistry(selected) {
  const reg = await loadRegistry();
  const agents = allAgents(reg);
  const filter = h("input", { placeholder: "filter agents…", style: "width:100%" });
  const list = h("div", { class: "list" });
  const detail = h("div", { class: "panel" });
  const draw = () => {
    const q = filter.value.toLowerCase();
    fill(list, );
    let feature = null;
    for (const a of agents.filter((x) => !q || x.ref.toLowerCase().includes(q) || (x.description || "").toLowerCase().includes(q))) {
      if (a.feature !== feature) { feature = a.feature; list.append(h("h3", {}, feature)); }
      list.append(h("div", { class: `item ${a.ref === selected ? "on" : ""}`, onclick: () => (location.hash = `#/registry/${encodeURIComponent(a.ref)}`) },
        h("span", {}, a.id), kindTag(a.kind), a.load_error ? h("span", { class: "tag v-terminated" }, "load error") : null));
    }
  };
  filter.addEventListener("input", draw);
  draw();
  const problems = reg.features.flatMap((f) => f.problems.map((p) => `${f.name}: ${p}`));
  const a = agents.find((x) => x.ref === selected);
  if (a) detail.append(agentDefinition(a));
  else detail.append(h("h2", {}, "Registry"),
    h("table", {}, h("tr", {}, h("th", {}, "feature"), h("th", {}, "package"), h("th", {}, "agents"), h("th", {}, "skills")),
      reg.features.map((f) => h("tr", {}, h("td", {}, f.name), h("td", {}, h("code", {}, f.package)), h("td", {}, f.agents.length), h("td", {}, f.skills.length)))),
    h("h3", {}, "Problems the registry reports"),
    problems.length ? h("pre", {}, problems.join("\n")) : h("div", { class: "dim" }, "none"));
  fill(main, h("div", { class: "split" }, h("div", { class: "panel" }, filter, list), detail));
}

function chips(items, cls = "tag") {
  return items && items.length ? h("div", { class: "chips" }, items.map((x) => h("span", { class: cls }, x))) : h("span", { class: "dim" }, "—");
}

function agentDefinition(a, walked) {
  const box = h("div", {});
  box.append(h("h2", {}, a.ref, " ", kindTag(a.kind)));
  if (a.description) box.append(h("p", {}, a.description));
  if (a.load_error) box.append(h("pre", { class: "v-terminated" }, a.load_error));
  box.append(h("table", {},
    h("tr", {}, h("th", {}, "reads"), h("td", {}, chips(a.reads)), h("th", {}, "writes"), h("td", {}, chips(a.writes))),
    h("tr", {}, h("th", {}, "spawns"), h("td", {}, chips(a.calls)), h("th", {}, "tools"), h("td", {}, chips(a.tools, "tag server"))),
    h("tr", {}, h("th", {}, "skills"), h("td", {}, chips(a.skills)), h("th", {}, "modes"), h("td", {}, chips(a.modes))),
    a.meta && Object.keys(a.meta).length ? h("tr", {}, h("th", {}, "meta"), h("td", { colspan: 3 }, h("code", {}, JSON.stringify(a.meta)))) : null,
    a.class ? h("tr", {}, h("th", {}, "class"), h("td", { colspan: 3 }, h("code", {}, a.class))) : null));
  if (a.machine) {
    box.append(h("h3", {}, "State machine"), stateMachine(a.machine, walked));
  }
  return box;
}

function stateMachine(m, walked) {
  const visited = new Set((walked && walked.states) || []);
  const pairs = new Set((walked && walked.pairs) || []);
  const nodes = m.states.map((s) => ({
    id: s, label: s, type: "agent", icon: s === m.start ? "▶" : s === m.on_term ? "■" : "",
    cls: walked && s === walked.current ? "current" : visited.has(s) ? "visited" : "",
  }));
  const done = m.transitions.some((t) => t.to === "(done)");
  if (done) nodes.push({ id: "(done)", label: "done", type: "artifact" });
  const edges = m.transitions.map((t) => ({ ...t, walked: pairs.has(`${t.from}>${t.to}`) }));
  const svg = layeredGraph(nodes, edges, { start: m.start, nodeWidth: 150, gapX: 46 });
  return h("div", { class: "graphbox", style: "max-height:360px" }, svg,
    h("div", { class: "dim", style: "padding:6px 10px;font-size:12px" },
      "edges: the states the code names (return, then=, on_fault=). Green: the path this agent walked."));
}

// ---- static graph --------------------------------------------------------------------------
async function showGraph() {
  const g = await api("/api/graph");
  const reg = await loadRegistry();
  const features = reg.features.map((f) => f.name);
  const pick = h("select", {}, h("option", { value: "" }, "all features"), features.map((f) => h("option", { value: f }, f)));
  const show = { spawns: true, uses: true, data: false };
  const box = h("div", { class: "graphbox" });
  const toggles = Object.keys(show).map((k) => h("label", {}, h("input", { type: "checkbox", ...(show[k] ? { checked: "" } : {}), onchange: (e) => { show[k] = e.target.checked; draw(); } }), " ", k));
  const draw = () => {
    const f = pick.value;
    const inFeature = (id) => !f || id.startsWith(f + ":") || id.startsWith("server:") || id.startsWith("type:");
    const edges = g.edges.filter((e) => (e.kind === "spawns" && show.spawns) || (e.kind === "uses" && show.uses) ||
      ((e.kind === "writes" || e.kind === "reads") && show.data)).filter((e) => inFeature(e.from) && inFeature(e.to));
    const used = new Set(edges.flatMap((e) => [e.from, e.to]));
    const nodes = g.nodes.filter((n) => used.has(n.id) || (n.type === "agent" && inFeature(n.id) && f))
      .map((n) => ({ ...n, icon: n.type === "server" ? "⛁" : n.type === "artifact" ? "▤" : n.kind === "agentic" ? "✦" : "●" }));
    fill(box, nodes.length ? layeredGraph(nodes, edges, {
      onClick: (n) => { if (n.type === "agent") location.hash = `#/registry/${encodeURIComponent(n.id)}`; else if (n.type === "server") location.hash = "#/servers"; },
    }) : h("div", { class: "empty" }, "Nothing to draw."));
  };
  pick.addEventListener("change", draw);
  fill(main, h("div", { class: "toolbar" }, pick, toggles,
    h("span", { class: "dim" }, "● state machine  ✦ agentic  ⛁ tool server  ▤ artifact type")), box);
  draw();
}

// ---- servers -------------------------------------------------------------------------------
async function showServers() {
  const servers = await api("/api/servers");
  fill(main, h("div", { class: "grid", style: "grid-template-columns: repeat(auto-fill, minmax(420px, 1fr))" },
    servers.map((s) => h("div", { class: "panel" },
      h("h2", {}, "⛁ ", s.name, " ", h("span", { class: `pill ${/^(up|connected|ok)/.test(s.status) ? "v-succeeded" : /unreach|down|error/.test(s.status) ? "v-terminated" : ""}` }, s.status)),
      s.url ? h("div", { class: "dim" }, h("code", {}, s.url)) : null,
      h("table", {}, h("tr", {}, h("th", {}, "tool"), h("th", {}, "used by")),
        (s.tools || []).map((t) => h("tr", {}, h("td", {}, h("code", {}, t.name), t.description ? h("div", { class: "dim" }, t.description.slice(0, 140)) : null),
          h("td", {}, (t.used_by || []).length ? h("div", { class: "chips" }, t.used_by.map((u) => h("a", { class: "tag", href: `#/registry/${encodeURIComponent(u)}` }, u.split(":")[1]))) : h("span", { class: "dim" }, "—")))))))));
}

// ---- runs ----------------------------------------------------------------------------------
async function showRuns() {
  const reg = await loadRegistry();
  const runs = await api("/api/runs");
  const agentPick = h("select", {}, allAgents(reg).filter((a) => !a.feature.startsWith("(")).map((a) => h("option", { value: a.ref }, a.ref)));
  const input = h("input", { placeholder: '{"key": "value"}', style: "width:260px" });
  const start = h("button", { onclick: async () => {
    try {
      const made = await post("/api/runs", { agent: agentPick.value, input: input.value ? JSON.parse(input.value) : {} });
      location.hash = `#/runs/${encodeURIComponent(made.root)}`;
    } catch (e) { alertBox(e.message); }
  } }, "Spawn a run");
  fill(main, h("div", { class: "toolbar" }, agentPick, input, start),
    h("div", { class: "panel" }, runsTable(runs.slice().reverse())));
}

function alertBox(text) {
  const box = h("div", { class: "panel v-terminated", style: "position:fixed;bottom:16px;right:16px;max-width:420px;z-index:9" }, text);
  document.body.append(box);
  setTimeout(() => box.remove(), 6000);
}

async function showRun(root, selected) {
  closeStreams();
  const tree = h("div", { class: "tree" });
  const graph = h("div", { class: "graphbox", style: "max-height:420px" });
  const side = h("div", { class: "panel" });
  const events = h("div", { class: "events" });
  const questions = h("div", {});
  const header = h("div", { class: "toolbar" });
  fill(main, header,
    h("div", { class: "split" },
      h("div", { class: "grid" }, h("div", { class: "panel" }, h("h2", {}, "Agents"), tree),
        h("div", { class: "panel" }, h("h2", {}, "Open questions"), questions),
        h("div", { class: "panel" }, h("h2", {}, "Events"), events)),
      h("div", { class: "grid" }, h("div", { class: "panel" }, h("h2", {}, "Run tree"), graph), side)));
  state.run = { root, selected: selected || root, header, tree, graph, questions, side };
  let pending = false;
  const refresh = async () => {
    if (pending) return;
    pending = true;
    try {
      const data = await api(`/api/runs/${encodeURIComponent(root)}`);
      drawRun(data, header, tree, graph, questions);
    } finally { pending = false; }
  };
  await refresh();
  await drawAgent(side);
  const src = new EventSource(`/api/runs/${encodeURIComponent(root)}/events`);
  state.source = src;
  src.onopen = () => setLive(true, `live · ${root}`);
  src.onerror = () => setLive(false, "reconnecting");
  let timer = null;
  const onEvent = (e) => {
    const ev = JSON.parse(e.data);
    events.prepend(h("div", {}, h("span", { class: "dim" }, `#${ev.seq} `), h("b", {}, ev.kind), " ",
      h("code", {}, (ev.agent || "").replace(root + "/", "")), ev.state ? ` → ${ev.state}` : "", ev.status ? ` (${ev.status})` : ""));
    clearTimeout(timer);
    timer = setTimeout(async () => { await refresh(); if (ev.agent === state.run.selected) await drawAgent(side); }, 150);
  };
  src.onmessage = onEvent;
}

function drawRun(data, header, tree, graph, questions) {
  const root = data.root;
  const rootRow = data.agents.find((a) => a.id === root);
  fill(header, h("h2", { style: "margin:0" }, h("code", {}, root)), viewTag(rootRow.view),
    h("span", { class: "dim" }, rootRow.agent),
    rootRow.workspace ? h("span", { class: "tag" }, `attached to ${rootRow.workspace}`) : null,
    rootRow.inherits ? h("span", { class: "tag" }, `fork of ${rootRow.inherits.root}`) : null,
    h("span", { style: "flex:1" }),
    ["STOP", "CONT", "TERM"].map((s) => h("button", { class: s === "TERM" ? "danger" : "", onclick: () => signal(root, s) }, s)),
    h("button", { onclick: () => (location.hash = `#/runs/${encodeURIComponent(root)}/files`) }, "Workspace"));
  fill(tree, ...data.agents.map((a) => {
    const depth = a.id.split("/").length - 1;
    return h("div", { class: `row ${a.id === state.run.selected ? "on" : ""}`, style: `padding-left:${6 + depth * 16}px`,
      onclick: () => { location.hash = `#/runs/${encodeURIComponent(root)}/${encodeURIComponent(a.id)}`; } },
      h("span", { class: `v-${a.view}` }, "●"), h("span", {}, a.name || a.id), h("span", { class: "dim" }, a.state),
      a.reasons.length ? h("span", { class: "tag" }, a.reasons.map((r) => r.kind).join(",")) : null);
  }));
  const nodes = data.agents.map((a) => ({ id: a.id, label: `${a.name || a.id} · ${a.state}`, type: "agent",
    color: VIEW_COLOR[a.view], cls: a.id === state.run.selected ? "on" : "", title: `${a.id}\n${a.agent}\n${a.view}${a.error ? "\n" + a.error : ""}` }));
  const edges = data.agents.filter((a) => a.parent).map((a) => ({ from: a.parent, to: a.id, kind: "spawns" }));
  fill(graph, layeredGraph(nodes, edges, { nodeWidth: 200, gapX: 40,
    onClick: (n) => { location.hash = `#/runs/${encodeURIComponent(root)}/${encodeURIComponent(n.id)}`; } }));
  fill(questions, ...(data.questions.length ? data.questions.map(questionForm) : [h("div", { class: "dim" }, "none")]));
}

function questionForm(q) {
  const schema = q.schema || {};
  let field;
  if (schema.enum) field = h("select", {}, schema.enum.map((v) => h("option", { value: JSON.stringify(v) }, String(v))));
  else field = h("input", { placeholder: schema.type === "object" ? '{"...": "..."}' : String(q.default ?? ""), style: "width:100%" });
  const send = async () => {
    let value = field.value;
    try {
      if (schema.enum) value = JSON.parse(value);
      else if (schema.type === "object" || schema.type === "array" || schema.type === "integer" || schema.type === "number" || schema.type === "boolean") value = JSON.parse(value);
    } catch (e) { return alertBox(`not valid: ${e.message}`); }
    try { await post("/api/answers", { question_id: q.question_id, answer: value }); } catch (e) { alertBox(e.message); }
  };
  return h("div", { style: "margin-bottom:10px" }, h("div", {}, h("code", {}, q.agent), " ", h("b", {}, q.name || "")),
    q.question ? h("p", { style: "white-space:pre-wrap" }, q.question) : null,
    h("div", { class: "toolbar" }, field, h("button", { onclick: send }, "Answer")),
    h("div", { class: "dim" }, `schema ${JSON.stringify(schema)}${q.default !== undefined ? " · default " + JSON.stringify(q.default) : ""}`));
}

async function signal(agent, sig) {
  try { await post("/api/signals", { agent, signal: sig }); } catch (e) { alertBox(e.message); }
}

async function drawAgent(side) {
  const id = state.run.selected;
  let a;
  try { a = await api(`/api/agents/${id.split("/").map(encodeURIComponent).join("/")}`); }
  catch (e) { fill(side, h("div", { class: "dim" }, e.message)); return; }
  const r = a.record;
  const pairs = [];
  for (let i = 1; i < a.path.length; i++) pairs.push(`${a.path[i - 1].state}>${a.path[i].state}`);
  if (r.status === "succeeded") pairs.push(`${r.state}>(done)`);
  const walked = { states: a.path.map((p) => p.state), pairs, current: r.state };
  const tabs = ["overview", "data", "conversation", "history", "record"];
  const body = h("div", {});
  const bar = h("div", { class: "tabs2" });
  const pick = (t) => {
    [...bar.children].forEach((b) => b.classList.toggle("on", b.textContent === t));
    fill(body, );
    if (t === "overview") {
      body.append(h("table", {},
        h("tr", {}, h("th", {}, "state"), h("td", {}, h("code", {}, r.state)), h("th", {}, "step"), h("td", {}, r.step)),
        h("tr", {}, h("th", {}, "status"), h("td", {}, r.status), h("th", {}, "lease"), h("td", {}, a.lease ? `${a.lease.owner} · epoch ${a.lease.epoch}` : "—")),
        h("tr", {}, h("th", {}, "waits"), h("td", { colspan: 3 }, r.reasons.length ? h("pre", {}, pretty(r.reasons)) : "—")),
        r.error ? h("tr", {}, h("th", {}, "error"), h("td", { colspan: 3 }, h("pre", { class: "v-terminated" }, r.error))) : null,
        h("tr", {}, h("th", {}, "published"), h("td", { colspan: 3 }, chips(Object.keys(r.published || {}))))),
        h("div", { class: "toolbar", style: "margin-top:8px" },
          ["STOP", "CONT", "TERM", "KILL"].map((s) => h("button", { class: s === "TERM" || s === "KILL" ? "danger" : "", onclick: () => signal(r.id, s) }, s))),
        agentDefinition(a.definition, walked));
    } else if (t === "data") body.append(h("pre", {}, pretty(a.data)));
    else if (t === "conversation") {
      body.append(...(a.messages.length ? a.messages.map((m) => h("div", { class: `msg ${m.role}` },
        h("div", { class: "dim" }, m.role, m.name ? ` · ${m.name}` : ""),
        m.content ? h("div", { style: "white-space:pre-wrap" }, typeof m.content === "string" ? m.content : pretty(m.content)) : null,
        m.tool_calls ? h("pre", {}, pretty(m.tool_calls)) : null)) : [h("div", { class: "dim" }, "no model conversation")]));
    } else if (t === "history") {
      body.append(h("table", {}, h("tr", {}, h("th", {}, "rev"), h("th", {}, "kind"), h("th", {}, "state"), h("th", {}, "step"), h("th", {}, "error")),
        a.path.map((p) => h("tr", {}, h("td", {}, p.revision), h("td", {}, p.kind), h("td", {}, h("code", {}, p.state)), h("td", {}, p.step), h("td", { class: "v-terminated" }, p.error || "")))));
    } else body.append(h("pre", {}, pretty(r)));
  };
  tabs.forEach((t) => bar.append(h("button", { onclick: () => pick(t) }, t)));
  fill(side, h("h2", {}, h("code", {}, r.id)), h("div", { class: "dim" }, r.agent), bar, body);
  pick("overview");
}

// ---- workspace -----------------------------------------------------------------------------
async function showFiles(root) {
  closeStreams();
  const paths = await api(`/api/runs/${encodeURIComponent(root)}/files`);
  const list = h("div", { class: "list" });
  const view = h("div", { class: "panel" }, h("div", { class: "dim" }, "Pick a file."));
  const filter = h("input", { placeholder: "filter paths…", style: "width:100%" });
  const tops = {};
  for (const p of paths) (tops[p.split("/")[0]] ||= []).push(p);
  const draw = () => {
    const q = filter.value.toLowerCase();
    fill(list, );
    for (const [top, ps] of Object.entries(tops)) {
      const shown = ps.filter((p) => !q || p.toLowerCase().includes(q));
      if (!shown.length) continue;
      list.append(h("h3", {}, `${top}/ (${shown.length})`));
      for (const p of shown.slice(0, 400)) list.append(h("div", { class: "item", onclick: () => open(p) }, h("code", {}, p.slice(top.length + 1) || p)));
    }
  };
  const open = async (p) => {
    const f = await api(`/api/runs/${encodeURIComponent(root)}/file?path=${encodeURIComponent(p)}`);
    let text = f.text;
    if (text && p.endsWith(".json")) { try { text = pretty(JSON.parse(text)); } catch { /* as is */ } }
    fill(view, h("h2", {}, h("code", {}, p)), h("div", { class: "dim" }, `${f.bytes} bytes${f.truncated ? " (truncated)" : ""}`),
      f.binary ? h("div", { class: "dim" }, "binary") : h("pre", { style: "max-height:70vh" }, text));
  };
  filter.addEventListener("input", draw);
  draw();
  fill(main, h("div", { class: "toolbar" }, h("a", { href: `#/runs/${encodeURIComponent(root)}` }, "← ", root), h("span", { class: "dim" }, `${paths.length} files`)),
    h("div", { class: "split" }, h("div", { class: "panel" }, filter, list), view));
}

// ---- routing -------------------------------------------------------------------------------
async function route() {
  const parts = location.hash.replace(/^#\/?/, "").split("/").map(decodeURIComponent);
  const tab = parts[0] || "overview";
  document.querySelectorAll("#tabs a").forEach((a) => a.classList.toggle("on", a.dataset.tab === tab));
  try {
    if (tab === "registry") { closeStreams(); await showRegistry(parts.slice(1).join("/")); }
    else if (tab === "graph") { closeStreams(); await showGraph(); }
    else if (tab === "servers") { closeStreams(); await showServers(); }
    else if (tab === "runs" && parts[1] && parts[2] === "files") await showFiles(parts[1]);
    else if (tab === "runs" && parts[1]) {
      const selected = parts.slice(2).join("/");
      if (state.run && state.run.root === parts[1] && state.source) {
        state.run.selected = selected || parts[1];
        const data = await api(`/api/runs/${encodeURIComponent(parts[1])}`);
        const v = state.run;
        drawRun(data, v.header, v.tree, v.graph, v.questions);
        await drawAgent(v.side);
      } else await showRun(parts[1], selected);
    }
    else if (tab === "runs") { closeStreams(); await showRuns(); }
    else await showOverview();
  } catch (e) {
    fill(main, h("div", { class: "panel v-terminated" }, e.message));
  }
}

window.addEventListener("hashchange", route);
api("/api/overview").then((o) => { $("#title").textContent = o.title; document.title = `${o.title} · statecraft console`; }).catch(() => {});
route();
