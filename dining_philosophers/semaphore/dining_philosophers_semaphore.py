#!/usr/bin/env python3
"""
Problem 3 - Dining Philosophers (4 philosophers) using SEMAPHORES.

Python 3 standard library only (threading, random, json, argparse).

Pipeline required by the assignment:
    Execute -> Synchronize -> Capture state -> Generate HTML -> view in browser

  * Execute     : 4 real OS-level threads (threading.Thread), one per philosopher.
  * Synchronize : every fork is a binary semaphore (threading.Semaphore(1)).
                  Deadlock is prevented by RESOURCE ORDERING: every philosopher
                  picks up the LOWER-numbered of its two forks first. For
                  philosophers 0..2 that is their left fork; philosopher 3 (whose
                  forks are 3 and 0) picks up fork 0 (its RIGHT fork) first.
                  This breaks the "circular wait" Coffman condition.
  * Capture     : a thread-safe EventRecorder stores a timestamped event for
                  every state change, together with a full snapshot (all
                  philosopher states, all fork owners, all meal counts).
                  Runtime invariant checks run on every event.
  * Generate    : after all threads are joined, the recorded events are embedded
                  as JSON in a single self-contained HTML file (inline CSS/JS)
                  that replays the real execution on an SVG round table.

Nothing is pre-scripted: the order of events depends entirely on how the OS
schedules the threads and on the random think/eat delays. Running the program
twice (even with the same --seed) usually produces a different interleaving.

Table layout (N = 4); fork Fi lies between P(i-1) and Pi:

                 P0
          F1           F0          left fork of Pi  = Fi
       P1                 P3       right fork of Pi = F(i+1) mod 4
          F2           F3          (so Fi is shared by Pi and P(i-1))
                 P2

Example:
    python3 dining_philosophers_semaphore.py --meals 5 --seed 42 \
        --max-think 0.3 --max-eat 0.2
"""

import argparse
import json
import os
import random
import sys
import threading
import time

N = 4  # the assignment requires EXACTLY 4 philosophers (and therefore 4 forks)

# Philosopher states (used for colouring in the HTML as well).
THINKING = "THINKING"
HUNGRY = "HUNGRY"      # wants to eat; waiting for / holding only some forks
EATING = "EATING"
RELEASING = "RELEASING"  # finished eating, putting forks back on the table
DONE = "DONE"          # finished all meals; thread is about to exit


def left_fork(i):
    """Fork on the left of philosopher i."""
    return i


def right_fork(i):
    """Fork on the right of philosopher i."""
    return (i + 1) % N


def fork_order(i):
    """
    DEADLOCK PREVENTION (resource ordering / asymmetric solution).

    Return (first, second) forks for philosopher i: always the lower-numbered
    fork first. For P0..P2 this is (left, right); for P3 it is (right, left)
    because P3's right fork is F0. Since every thread acquires forks in
    increasing global order, a cycle "P0 waits for P1 waits for ... P3 waits
    for P0" can never form, so circular wait (and hence deadlock) is impossible.
    """
    l, r = left_fork(i), right_fork(i)
    return (l, r) if l < r else (r, l)


def side_of(i, fork):
    return "left" if fork == left_fork(i) else "right"


# --------------------------------------------------------------------------- #
#  SHARED RESOURCES                                                           #
# --------------------------------------------------------------------------- #
# The forks are the shared resources. Each fork is a binary semaphore with an
# initial value of 1 (= "fork is on the table"). acquire() = pick the fork up
# (blocks while a neighbour holds it), release() = put it back down.
forks = [threading.Semaphore(1) for _ in range(N)]


# --------------------------------------------------------------------------- #
#  STATE CAPTURE (observation only - NOT used for synchronization)            #
# --------------------------------------------------------------------------- #
class EventRecorder:
    """
    Thread-safe recorder of timestamped state-change events.

    It keeps a *mirror* of the system state (who holds each fork, what each
    philosopher is doing, meal counts) purely so it can (a) snapshot it into
    every event for the HTML replay and (b) check safety invariants at runtime.

    Its internal lock protects only this bookkeeping. The philosophers never use
    it to decide whether they may eat - mutual exclusion on forks is provided
    solely by the fork semaphores above.
    """

    def __init__(self, echo=True):
        self._lock = threading.Lock()
        self._t0 = time.perf_counter()
        self.events = []
        self.fork_owner = [None] * N        # philosopher id holding each fork
        self.states = [THINKING] * N
        self.waiting_for = [None] * N       # fork id a philosopher is blocked on
        self.meals = [0] * N
        self.violations = []                # human readable descriptions
        self.hungry_since = [None] * N
        self.wait_times = [[] for _ in range(N)]  # hungry -> eating latency
        self.echo = echo

    # -- internal helpers (caller must hold self._lock) ------------------------
    def _violation(self, msg):
        self.violations.append(msg)
        print(f"!!! INVARIANT VIOLATION: {msg}", file=sys.stderr)

    def _check_invariants(self):
        # Invariant 1: two adjacent philosophers are never eating at the same time.
        for i in range(N):
            j = (i + 1) % N
            if self.states[i] == EATING and self.states[j] == EATING:
                self._violation(f"adjacent P{i} and P{j} are eating simultaneously")
        # Invariant 2: an eating philosopher holds both of its forks.
        for i in range(N):
            if self.states[i] == EATING:
                if (self.fork_owner[left_fork(i)] != i
                        or self.fork_owner[right_fork(i)] != i):
                    self._violation(f"P{i} is eating without holding both forks")

    def _record(self, phil, kind, desc, fork=None):
        t = time.perf_counter() - self._t0
        self._check_invariants()
        ev = {
            "seq": len(self.events),
            "t": round(t, 6),
            "phil": phil,
            "kind": kind,
            "fork": fork,
            "desc": desc,
            "states": list(self.states),
            "waiting": list(self.waiting_for),
            "forks": list(self.fork_owner),
            "meals": list(self.meals),
            "violations": len(self.violations),
        }
        self.events.append(ev)
        if self.echo:
            owners = " ".join(
                f"F{f}:{'P' + str(o) if o is not None else '--'}"
                for f, o in enumerate(self.fork_owner))
            print(f"[{t:8.4f}s] #{ev['seq']:<4} P{phil} {kind:<13} {desc:<44} | {owners}",
                  flush=True)

    # -- public API, one method per state change ------------------------------
    def thinking(self, i):
        with self._lock:
            self.states[i] = THINKING
            self._record(i, "think", f"P{i} is thinking")

    def hungry(self, i):
        with self._lock:
            self.states[i] = HUNGRY
            self.hungry_since[i] = time.perf_counter()
            self._record(i, "hungry", f"P{i} is hungry")

    def requesting(self, i, f):
        """Called just BEFORE forks[f].acquire(): P{i} may block here."""
        with self._lock:
            self.waiting_for[i] = f
            holder = self.fork_owner[f]
            extra = f" (held by P{holder}, waiting)" if holder is not None else ""
            self._record(i, "request_fork",
                         f"P{i} requests {side_of(i, f)} fork F{f}{extra}", f)

    def acquired(self, i, f):
        """Called just AFTER forks[f].acquire() returned."""
        with self._lock:
            # Invariant 3: a fork is held by at most one philosopher.
            if self.fork_owner[f] is not None:
                self._violation(f"F{f} acquired by P{i} while held by P{self.fork_owner[f]}")
            self.fork_owner[f] = i
            self.waiting_for[i] = None
            self._record(i, "acquire_fork", f"P{i} picked up {side_of(i, f)} fork F{f}", f)

    def eating(self, i):
        with self._lock:
            self.states[i] = EATING
            self.meals[i] += 1
            self.wait_times[i].append(time.perf_counter() - self.hungry_since[i])
            self._record(i, "eat", f"P{i} starts eating (meal {self.meals[i]})")

    def releasing(self, i):
        with self._lock:
            # Stop "eating" before any fork goes back on the table.
            self.states[i] = RELEASING
            self._record(i, "release", f"P{i} finished eating, releasing forks")

    def released(self, i, f):
        """Called just BEFORE forks[f].release(): the mirror marks the fork free
        before any neighbour can acquire it, so it can never show two owners."""
        with self._lock:
            if self.fork_owner[f] != i:
                self._violation(f"P{i} released F{f} which it did not hold")
            self.fork_owner[f] = None
            self._record(i, "release_fork", f"P{i} put down {side_of(i, f)} fork F{f}", f)

    def done(self, i):
        with self._lock:
            self.states[i] = DONE
            self._record(i, "done", f"P{i} finished all meals and leaves")

    def elapsed(self):
        return time.perf_counter() - self._t0


# --------------------------------------------------------------------------- #
#  PHILOSOPHER THREAD                                                         #
# --------------------------------------------------------------------------- #
def philosopher(i, rec, meals, max_think, max_eat, grab_delay, rng):
    first, second = fork_order(i)
    for _ in range(meals):
        # ---- THINK (outside any critical section) ----
        rec.thinking(i)
        time.sleep(rng.uniform(0, max_think))   # random delay = simulated work only

        # ---- HUNGRY: try to get both forks in global (ascending) order ----
        rec.hungry(i)

        rec.requesting(i, first)
        forks[first].acquire()                  # P(fork_first) - may block
        rec.acquired(i, first)

        # Optional pause between the two pick-ups. It widens the window in which
        # a naive "everyone grabs left first" solution would deadlock, so the
        # demo actually stresses the ordering strategy.
        if grab_delay > 0:
            time.sleep(rng.uniform(0, grab_delay))

        rec.requesting(i, second)
        forks[second].acquire()                 # P(fork_second) - may block
        rec.acquired(i, second)

        # ======== CRITICAL SECTION: philosopher i uses forks `first`+`second` ===
        rec.eating(i)
        time.sleep(rng.uniform(0, max_eat))     # eating
        rec.releasing(i)
        # ======== END OF CRITICAL SECTION ========================================

        # ---- put the forks back (reverse order; order of release is irrelevant
        #      for deadlock, but releasing the second one first is conventional) --
        rec.released(i, second)
        forks[second].release()                 # V(fork_second)
        rec.released(i, first)
        forks[first].release()                  # V(fork_first)

    rec.done(i)


# --------------------------------------------------------------------------- #
#  HTML GENERATION                                                            #
# --------------------------------------------------------------------------- #
HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Dining Philosophers – Semaphores</title>
<style>
  :root {
    --bg: #f6f7fb; --panel: #ffffff; --ink: #1d2433; --muted: #667085;
    --line: #d9dde7; --accent: #3b5bdb;
    --think: #4dabf7; --hungry: #fab005; --eat: #40c057; --release: #be4bdb; --done: #adb5bd;
    --fork-free: #868e96; --table: #f1e3c8; --table-edge: #c9a96e;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.45 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
         background: var(--bg); color: var(--ink); }
  header { padding: 16px 20px 6px; }
  header h1 { margin: 0; font-size: 20px; }
  header p { margin: 4px 0 0; color: var(--muted); }
  .wrap { display: grid; grid-template-columns: minmax(320px, 520px) 1fr; gap: 16px; padding: 12px 20px 20px; }
  @media (max-width: 900px) { .wrap { grid-template-columns: 1fr; padding: 12px 16px; } }
  .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 14px; }
  .panel h2 { margin: 0 0 8px; font-size: 15px; }
  svg { width: 100%; height: auto; display: block; }
  .controls { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-top: 10px; }
  button { font: inherit; padding: 6px 12px; border: 1px solid var(--line); background: #fff;
           border-radius: 6px; cursor: pointer; }
  button:hover { border-color: var(--accent); }
  button.primary { background: var(--accent); color: #fff; border-color: var(--accent); }
  input[type=range] { flex: 1; min-width: 120px; }
  .now { margin-top: 10px; padding: 8px 10px; background: #eef2ff; border-radius: 6px; min-height: 38px; }
  .legend { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 8px; color: var(--muted); font-size: 12px; }
  .legend span::before { content: ""; display: inline-block; width: 10px; height: 10px; border-radius: 50%;
                         margin-right: 5px; vertical-align: -1px; background: var(--c); }
  table.stats { width: 100%; border-collapse: collapse; margin-top: 6px; }
  table.stats th, table.stats td { text-align: left; padding: 4px 6px; border-bottom: 1px solid var(--line); }
  table.stats td.num { font-variant-numeric: tabular-nums; }
  .ok { color: #2b8a3e; font-weight: 600; } .bad { color: #c92a2a; font-weight: 600; }
  #timeline { max-height: 470px; overflow-y: auto; border: 1px solid var(--line); border-radius: 6px;
              font: 12px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  #timeline div { padding: 3px 8px; border-bottom: 1px solid #f0f1f5; cursor: pointer; white-space: nowrap;
                  overflow: hidden; text-overflow: ellipsis; }
  #timeline div.future { color: #b0b6c3; }
  #timeline div.cur { background: #fff3bf; color: var(--ink); font-weight: 600; }
  .tag { display: inline-block; min-width: 92px; }
  .meta { color: var(--muted); font-size: 12px; margin-top: 8px; }
  code { background: #f1f3f5; padding: 1px 4px; border-radius: 4px; }
</style>
</head>
<body>
<header>
  <h1>Dining Philosophers (N = 4) — Semaphore solution</h1>
  <p>Replay of a <b>real</b> multithreaded run recorded by the program. Forks are
     <code>threading.Semaphore(1)</code>; deadlock is prevented by resource ordering
     (each philosopher picks up the lower-numbered fork first, so P3 takes F0 before F3).</p>
</header>
<div class="wrap">
  <div>
    <div class="panel">
      <svg id="scene" viewBox="0 0 400 400" role="img" aria-label="Round table with 4 philosophers and 4 forks"></svg>
      <div class="legend">
        <span style="--c:var(--think)">Thinking</span>
        <span style="--c:var(--hungry)">Hungry / waiting</span>
        <span style="--c:var(--eat)">Eating</span>
        <span style="--c:var(--release)">Releasing forks</span>
        <span style="--c:var(--done)">Done</span>
        <span style="--c:var(--fork-free)">Fork on table</span>
      </div>
      <div class="controls">
        <button id="play" class="primary">Play</button>
        <button id="back">◀ Step</button>
        <button id="step">Step ▶</button>
        <button id="reset">Reset</button>
      </div>
      <div class="controls">
        <label for="speed">Speed</label>
        <input id="speed" type="range" min="-3" max="4" step="1" value="0">
        <span id="speedLbl">1×</span>
      </div>
      <div class="controls">
        <label for="scrub">Event</label>
        <input id="scrub" type="range" min="-1" value="-1">
        <span id="pos"></span>
      </div>
      <div class="now" id="now">Press Play or Step to start the replay.</div>
    </div>
    <div class="panel" style="margin-top:16px">
      <h2>State at current event</h2>
      <table class="stats" id="stats"></table>
      <div class="meta" id="summary"></div>
    </div>
  </div>
  <div class="panel">
    <h2>Event timeline (recorded at runtime, click to jump)</h2>
    <div id="timeline"></div>
    <div class="meta" id="config"></div>
  </div>
</div>

<script id="sim-data" type="application/json">__DATA__</script>
<script>
(function () {
  "use strict";
  const DATA = JSON.parse(document.getElementById("sim-data").textContent);
  const EV = DATA.events, N = DATA.n;
  const COLORS = { THINKING: "var(--think)", HUNGRY: "var(--hungry)", EATING: "var(--eat)", RELEASING: "var(--release)", DONE: "var(--done)" };
  const LABEL = { THINKING: "Thinking", HUNGRY: "Hungry", EATING: "Eating", RELEASING: "Releasing", DONE: "Done" };
  const PHIL_COLORS = ["#e64980", "#7048e8", "#1098ad", "#f76707"];
  const NS = "http://www.w3.org/2000/svg";
  const C = 200, R_TABLE = 105, R_PHIL = 160, R_FORK = 78;

  // Philosopher i sits at angle 90deg*i going counter-clockwise from the top.
  // Fork Fi is Pi's left fork and P(i-1)'s right fork, so it is drawn halfway
  // between those two seats (vector average avoids angle wrap-around issues).
  const philAngle = i => -Math.PI / 2 - i * (2 * Math.PI / N);
  const forkAngle = f => { const a = philAngle(f), b = philAngle((f + N - 1) % N);
                           return Math.atan2(Math.sin(a) + Math.sin(b), Math.cos(a) + Math.cos(b)); };
  const pt = (a, r) => [C + r * Math.cos(a), C + r * Math.sin(a)];

  const svg = document.getElementById("scene");
  function el(tag, attrs, parent) {
    const e = document.createElementNS(NS, tag);
    for (const k in attrs) e.setAttribute(k, attrs[k]);
    (parent || svg).appendChild(e);
    return e;
  }
  el("circle", { cx: C, cy: C, r: R_TABLE, fill: "var(--table)", stroke: "var(--table-edge)", "stroke-width": 4 });
  const center = el("text", { x: C, y: C + 5, "text-anchor": "middle", "font-size": 13, fill: "#7a5c2e" });
  center.textContent = "Table";

  const forkG = [], forkLbl = [];
  for (let f = 0; f < N; f++) {
    const g = el("g", {});             // translated to the fork position
    g.style.transition = "transform 0.25s ease";
    const rg = el("g", {}, g);         // rotated so the fork points away from the centre
    rg.style.transition = "transform 0.25s ease";
    const line = el("line", { x1: 0, y1: -14, x2: 0, y2: 22, "stroke-width": 6, "stroke-linecap": "round" }, rg);
    const head = el("path", { d: "M-7,-14 L-7,-26 M0,-14 L0,-28 M7,-14 L7,-26 M-7,-14 L7,-14",
                              fill: "none", "stroke-width": 3, "stroke-linecap": "round" }, rg);
    const lbl = el("text", { x: 0, y: 4, "text-anchor": "middle", "font-size": 11, "font-weight": 700,
                             stroke: "#fff", "stroke-width": 3, "paint-order": "stroke" }, g);
    forkG.push({ g, rg, line, head, lbl });
  }

  const philG = [];
  for (let i = 0; i < N; i++) {
    const [x, y] = pt(philAngle(i), R_PHIL);
    const g = el("g", { transform: `translate(${x},${y})` });
    const ring = el("circle", { r: 34, fill: "none", stroke: PHIL_COLORS[i], "stroke-width": 3 }, g);
    const body = el("circle", { r: 29, stroke: "#fff", "stroke-width": 2 }, g);
    body.style.transition = "fill 0.2s";
    const name = el("text", { y: -4, "text-anchor": "middle", "font-size": 15, "font-weight": 700, fill: "#1d2433" }, g);
    name.textContent = "P" + i;
    const st = el("text", { y: 12, "text-anchor": "middle", "font-size": 10, fill: "#1d2433" }, g);
    const meals = el("text", { y: 50, "text-anchor": "middle", "font-size": 11, fill: "#495057" }, g);
    philG.push({ body, st, meals });
  }

  function renderForks(owners) {
    for (let f = 0; f < N; f++) {
      const o = owners[f];
      let a = forkAngle(f), r = R_FORK;
      if (o !== null) {           // slide the fork towards the philosopher holding it
        const pa = philAngle(o);
        let d = pa - a; while (d > Math.PI) d -= 2 * Math.PI; while (d < -Math.PI) d += 2 * Math.PI;
        a = a + d * 0.55; r = R_FORK + 32;
      }
      const [x, y] = pt(a, r);
      const rot = a * 180 / Math.PI + 90;
      const fg = forkG[f];
      fg.g.style.transform = `translate(${x}px,${y}px)`;
      fg.rg.style.transform = `rotate(${rot}deg)`;
      const col = o === null ? "var(--fork-free)" : PHIL_COLORS[o];
      fg.line.setAttribute("stroke", col); fg.head.setAttribute("stroke", col);
      fg.lbl.setAttribute("fill", col);
      fg.lbl.setAttribute("x", (-Math.cos(a) * 30).toFixed(1));      // label sits just inside the fork,
      fg.lbl.setAttribute("y", (-Math.sin(a) * 30 + 4).toFixed(1));  // towards the table centre
      fg.lbl.textContent = "F" + f + (o === null ? " free" : " → P" + o);
    }
  }

  const timeline = document.getElementById("timeline");
  const rows = EV.map((e, k) => {
    const d = document.createElement("div");
    d.className = "future";
    d.innerHTML = `<span class="tag">${e.t.toFixed(4)}s #${e.seq}</span>` + escapeHtml(e.desc);
    d.style.borderLeft = "4px solid " + PHIL_COLORS[e.phil];
    d.addEventListener("click", () => { pause(); go(k); });
    timeline.appendChild(d);
    return d;
  });
  function escapeHtml(s) { return s.replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }

  const statsEl = document.getElementById("stats");
  const nowEl = document.getElementById("now");
  const scrub = document.getElementById("scrub");
  const posEl = document.getElementById("pos");
  scrub.max = EV.length - 1;

  const initial = { t: 0, seq: -1, desc: "Initial state: all philosophers thinking, all forks on the table.",
                    states: Array(N).fill("THINKING"), waiting: Array(N).fill(null),
                    forks: Array(N).fill(null), meals: Array(N).fill(0), violations: 0, phil: null };

  let cur = -1, timer = null;
  function snapshot(k) { return k < 0 ? initial : EV[k]; }

  function render() {
    const e = snapshot(cur);
    for (let i = 0; i < N; i++) {
      const s = e.states[i];
      philG[i].body.setAttribute("fill", COLORS[s]);
      let txt = LABEL[s];
      if (s === "HUNGRY" && e.waiting[i] !== null) txt = "waits F" + e.waiting[i];
      philG[i].st.textContent = txt;
      philG[i].meals.textContent = "meals: " + e.meals[i] + " / " + DATA.config.meals;
    }
    renderForks(e.forks);
    nowEl.innerHTML = cur < 0 ? escapeHtml(e.desc)
      : `<b>t = ${e.t.toFixed(4)} s</b> &nbsp;event ${e.seq + 1}/${EV.length}: ${escapeHtml(e.desc)}`;
    let h = "<tr><th>Philosopher</th><th>State</th><th>Holds</th><th>Meals</th></tr>";
    for (let i = 0; i < N; i++) {
      const held = e.forks.map((o, f) => o === i ? "F" + f : null).filter(Boolean).join(", ") || "—";
      let s = LABEL[e.states[i]];
      if (e.states[i] === "HUNGRY" && e.waiting[i] !== null) s += " (blocked on F" + e.waiting[i] + ")";
      h += `<tr><td style="color:${PHIL_COLORS[i]};font-weight:700">P${i}</td><td>${s}</td><td>${held}</td><td class="num">${e.meals[i]}</td></tr>`;
    }
    const eatingNow = e.states.map((s, i) => s === "EATING" ? "P" + i : null).filter(Boolean);
    h += `<tr><td colspan="4">Eating now: <b>${eatingNow.join(", ") || "nobody"}</b> &nbsp;·&nbsp; Violations so far: <span class="${e.violations ? "bad" : "ok"}">${e.violations}</span></td></tr>`;
    statsEl.innerHTML = h;
    rows.forEach((r, k) => { r.className = k < cur ? "" : (k === cur ? "cur" : "future"); });
    if (cur >= 0) {
      const r = rows[cur], top = r.offsetTop - timeline.offsetTop;
      if (top < timeline.scrollTop || top > timeline.scrollTop + timeline.clientHeight - 30)
        timeline.scrollTop = top - timeline.clientHeight / 2;
    } else timeline.scrollTop = 0;
    scrub.value = cur;
    posEl.textContent = (cur + 1) + " / " + EV.length;
  }

  function go(k) { cur = Math.max(-1, Math.min(EV.length - 1, k)); render(); }
  function speed() { return Math.pow(2, +document.getElementById("speed").value); }
  function tick() {
    if (cur >= EV.length - 1) { pause(); return; }
    go(cur + 1);
    // Delay until the next event is proportional to the REAL gap between the two
    // recorded timestamps (clamped so long sleeps don't stall the replay).
    const next = EV[cur + 1];
    const gap = next ? Math.min(Math.max((next.t - EV[cur].t) * 1000 * 2, 60), 900) : 0;
    timer = setTimeout(tick, gap / speed());
  }
  function play() { if (timer) return; if (cur >= EV.length - 1) go(-1);
                    document.getElementById("play").textContent = "Pause"; tick(); }
  function pause() { clearTimeout(timer); timer = null; document.getElementById("play").textContent = "Play"; }

  document.getElementById("play").onclick = () => timer ? pause() : play();
  document.getElementById("step").onclick = () => { pause(); go(cur + 1); };
  document.getElementById("back").onclick = () => { pause(); go(cur - 1); };
  document.getElementById("reset").onclick = () => { pause(); go(-1); };
  scrub.oninput = () => { pause(); go(+scrub.value); };
  document.getElementById("speed").oninput = function () {
    const s = speed(); document.getElementById("speedLbl").textContent = (s < 1 ? s.toFixed(3).replace(/0+$/, "") : s) + "×";
  };

  const S = DATA.summary, cfg = DATA.config;
  document.getElementById("summary").innerHTML =
    `Run finished in ${S.duration.toFixed(3)} s with ${EV.length} events. ` +
    `Final meals: ${S.meals.map((m, i) => "P" + i + "=" + m).join(", ")}. ` +
    `Invariant violations: <span class="${S.violations ? "bad" : "ok"}">${S.violations}</span>. ` +
    `Max hunger wait: ${S.max_wait.map((w, i) => "P" + i + "=" + w.toFixed(3) + "s").join(", ")}.`;
  document.getElementById("config").innerHTML =
    `Config: meals=${cfg.meals}, seed=${cfg.seed}, max-think=${cfg.max_think}s, max-eat=${cfg.max_eat}s, ` +
    `grab-delay=${cfg.grab_delay}s · generated ${escapeHtml(DATA.generated)} by <code>${escapeHtml(DATA.command)}</code>`;
  render();
})();
</script>
</body>
</html>
"""


def write_html(path, rec, cfg, duration, command):
    data = {
        "n": N,
        "config": cfg,
        "command": command,
        "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "summary": {
            "duration": duration,
            "meals": rec.meals,
            "violations": len(rec.violations),
            "max_wait": [max(w) if w else 0.0 for w in rec.wait_times],
        },
        "events": rec.events,
    }
    # Escape "</" so the JSON can never terminate the <script> element early.
    payload = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(HTML_TEMPLATE.replace("__DATA__", payload))


# --------------------------------------------------------------------------- #
#  MAIN                                                                       #
# --------------------------------------------------------------------------- #
def main():
    here = os.path.dirname(os.path.abspath(__file__))
    default_out = os.path.normpath(
        os.path.join(here, "..", "..", "generated_html", "dining_philosophers_semaphore.html"))

    ap = argparse.ArgumentParser(
        description="Dining Philosophers (4) with semaphores + HTML replay generator.")
    ap.add_argument("--meals", type=int, default=5, help="meals each philosopher eats (default 5)")
    ap.add_argument("--seed", type=int, default=None,
                    help="seed for the random delays (default: random; the used seed is printed)")
    ap.add_argument("--max-think", type=float, default=0.3, help="max thinking time in s (default 0.3)")
    ap.add_argument("--max-eat", type=float, default=0.2, help="max eating time in s (default 0.2)")
    ap.add_argument("--grab-delay", type=float, default=0.05,
                    help="max pause between picking up 1st and 2nd fork in s (default 0.05)")
    ap.add_argument("--timeout", type=float, default=60.0,
                    help="watchdog: report a deadlock if threads are still alive after this many s")
    ap.add_argument("--output", default=default_out, help=f"HTML output path (default {default_out})")
    ap.add_argument("--quiet", action="store_true", help="do not echo events to the console")
    args = ap.parse_args()

    if args.meals < 1 or min(args.max_think, args.max_eat, args.grab_delay) < 0:
        ap.error("--meals must be >= 1 and delays must be >= 0")
    seed = args.seed if args.seed is not None else random.randrange(1_000_000)

    print(f"Dining Philosophers (semaphores), N={N}, meals={args.meals}, seed={seed}, "
          f"max-think={args.max_think}s, max-eat={args.max_eat}s, grab-delay={args.grab_delay}s")
    for i in range(N):
        a, b = fork_order(i)
        print(f"  P{i}: left=F{left_fork(i)} right=F{right_fork(i)} -> acquires F{a} then F{b}")
    print()

    rec = EventRecorder(echo=not args.quiet)
    threads = [
        threading.Thread(
            target=philosopher, name=f"P{i}",
            args=(i, rec, args.meals, args.max_think, args.max_eat, args.grab_delay,
                  random.Random(seed * 31 + i)),   # independent RNG per thread
            daemon=True)
        for i in range(N)
    ]
    for t in threads:
        t.start()

    # Watchdog join: with the ordering strategy this always finishes; if it ever
    # did not, the threads would be stuck in acquire() and we report it.
    deadline = time.perf_counter() + args.timeout
    for t in threads:
        t.join(max(0.0, deadline - time.perf_counter()))
    alive = [t.name for t in threads if t.is_alive()]
    duration = rec.elapsed()

    print()
    print("=" * 60)
    if alive:
        print(f"DEADLOCK/TIMEOUT: threads still blocked after {args.timeout}s: {alive}")
    else:
        print(f"All {N} philosopher threads terminated (no deadlock) in {duration:.3f}s.")
    print(f"Events recorded      : {len(rec.events)}")
    print(f"Meals per philosopher: " + ", ".join(f"P{i}={m}" for i, m in enumerate(rec.meals)))
    print(f"Max hunger wait (s)  : " + ", ".join(
        f"P{i}={max(w) if w else 0:.3f}" for i, w in enumerate(rec.wait_times)))
    print(f"Invariant violations : {len(rec.violations)}")

    cfg = {"meals": args.meals, "seed": seed, "max_think": args.max_think,
           "max_eat": args.max_eat, "grab_delay": args.grab_delay}
    command = "python3 dining_philosophers_semaphore.py " + " ".join(sys.argv[1:])
    write_html(args.output, rec, cfg, duration, command.strip())
    print(f"HTML simulation      : {args.output}")
    print("=" * 60)
    return 1 if (alive or rec.violations) else 0


if __name__ == "__main__":
    sys.exit(main())
