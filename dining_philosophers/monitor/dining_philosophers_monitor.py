#!/usr/bin/env python3
"""
Problem 4 - Dining Philosophers (4 philosophers) solved with a MONITOR.

Execute -> Synchronize -> Capture state -> Generate HTML
-------------------------------------------------------
  1. Execute:      4 real OS threads (threading.Thread), one per philosopher.
  2. Synchronize:  every access to the shared table state goes through the
                   DiningMonitor below (one lock + one condition per philosopher).
  3. Capture:      every state change is recorded *while it happens* by a
                   thread-safe EventRecorder (timestamp, actor, states, fork owners).
  4. Generate:     after all threads finish, the recorded events are embedded as
                   JSON into a self-contained HTML page that replays the run.

Nothing is pre-scripted: the order of events depends on the OS scheduler and
the random think/eat delays, so each run (and each seed) produces a different
interleaving. The random delays only decide *how long* a philosopher thinks or
eats; mutual exclusion is enforced exclusively by the monitor.

Why this class is a monitor
---------------------------
Python has no `monitor` keyword, so we build one explicitly (Hoare/Brinch
Hansen monitor as in Silberschatz, "Operating System Concepts", ch. 6):

  * ONE mutual-exclusion lock (`self._lock`) protects ALL monitor data.
    Every public (entry) procedure - pickup(i), putdown(i) - acquires it on
    entry and releases it on exit (`with self._lock:`), so at most one thread
    is ever "inside the monitor".
  * Condition variables `self._self[i]` (one per philosopher) are all bound to
    that same lock. `wait()` atomically releases the monitor lock and blocks;
    `notify()` wakes a waiter, which must re-acquire the lock before running.
  * Private procedure `_test(i)` is only ever called with the lock held.
  * Python's Condition has MESA (signal-and-continue) semantics, not Hoare
    semantics: the signalled thread does not run immediately, so by the time
    it re-acquires the lock the condition might no longer hold. Therefore the
    wait is always inside a `while` loop that re-checks the condition.

SHARED RESOURCE:   the 4 forks (and the state[] array that describes who may
                   use them), all stored inside DiningMonitor.
CRITICAL SECTION:  the body of pickup()/putdown()/_test() - i.e. everything
                   executed while holding DiningMonitor._lock.
"""

import argparse
import html
import json
import os
import random
import sys
import threading
import time

N = 4  # EXACTLY 4 philosophers (and 4 forks), as required by the assignment

# Monitor states (the classic three-valued state array).
THINKING, HUNGRY, EATING = "THINKING", "HUNGRY", "EATING"


def left(i):
    """Index of the philosopher to the left of i (counter-clockwise)."""
    return (i - 1) % N


def right(i):
    """Index of the philosopher to the right of i (clockwise)."""
    return (i + 1) % N


def forks_of(i):
    """Philosopher i uses fork i (left hand) and fork (i+1) % N (right hand)."""
    return (i, (i + 1) % N)


# --------------------------------------------------------------------------
# Event recorder - captures every state change with a timestamp while the
# program runs. Has its own lock so it is safe to call from any thread.
# --------------------------------------------------------------------------
class EventRecorder:
    def __init__(self, quiet=False):
        self._lock = threading.Lock()
        self._t0 = time.perf_counter()
        self.events = []
        self.quiet = quiet

    def record(self, actor, kind, message, snapshot):
        """Append one event. `snapshot` is a dict describing the whole table
        (display states, monitor states, fork owners, meal counts) at this
        instant; the caller takes it while holding the monitor lock so the
        snapshot is consistent."""
        with self._lock:
            t_ms = (time.perf_counter() - self._t0) * 1000.0
            ev = {
                "seq": len(self.events),
                "t": round(t_ms, 3),
                "actor": actor,
                "kind": kind,
                "msg": message,
                "thread": threading.current_thread().name,
            }
            ev.update(snapshot)
            self.events.append(ev)
            if not self.quiet:
                forks = " ".join(
                    f"F{f}:{'P%d' % o if o is not None else '--'}"
                    for f, o in enumerate(snapshot["forks"])
                )
                print(f"[{t_ms:9.2f} ms] {kind:<9} {message:<58} | {forks}",
                      flush=True)


# --------------------------------------------------------------------------
# The MONITOR
# --------------------------------------------------------------------------
class DiningMonitor:
    """Classic Silberschatz monitor solution, plus an optional fairness rule.

        monitor DiningPhilosophers {
            enum {THINKING, HUNGRY, EATING} state[N];
            condition self[N];
            void pickup(int i)  { state[i] = HUNGRY; test(i);
                                  if (state[i] != EATING) self[i].wait(); }
            void putdown(int i) { state[i] = THINKING;
                                  test((i + N - 1) % N); test((i + 1) % N); }
            void test(int i)    { if (state[(i+N-1)%N] != EATING &&
                                      state[i] == HUNGRY &&
                                      state[(i+1)%N] != EATING) {
                                      state[i] = EATING; self[i].signal(); } }
        }
    """

    def __init__(self, recorder, fair=True):
        # ---- monitor-private data (SHARED RESOURCE) ----------------------
        self._lock = threading.Lock()                        # the monitor lock
        self._self = [threading.Condition(self._lock) for _ in range(N)]
        self.state = [THINKING] * N                          # classic state[]
        self.forks = [None] * N          # fork f -> owning philosopher or None
        self.display = ["THINKING"] * N  # richer state used for visualisation
        self.meals = [0] * N
        self.hungry_ticket = [None] * N  # order in which philosophers got hungry
        self._next_ticket = 0
        self.fair = fair
        self.recorder = recorder
        # ---- statistics / runtime invariant checks -----------------------
        self.violations = 0
        self.violation_details = []
        self.signals = 0
        self.waits = 0

    # ---------------- helpers (always called with self._lock held) --------
    def _snapshot(self):
        return {
            "states": list(self.display),
            "mstates": list(self.state),
            "forks": list(self.forks),
            "meals": list(self.meals),
        }

    def _emit(self, actor, kind, message):
        self._check_invariants(message)
        self.recorder.record(actor, kind, message, self._snapshot())

    def _check_invariants(self, context):
        """Safety checks run on EVERY event while holding the monitor lock:
          (1) no two adjacent philosophers are EATING at the same time;
          (2) an EATING philosopher owns both of its forks;
          (3) a fork is only owned by one of its two adjacent philosophers."""
        problems = []
        for i in range(N):
            if self.state[i] == EATING and self.state[right(i)] == EATING:
                problems.append(f"P{i} and P{right(i)} both EATING")
            if self.state[i] == EATING:
                for f in forks_of(i):
                    if self.forks[f] != i:
                        problems.append(f"P{i} EATING without fork F{f}")
        for f, owner in enumerate(self.forks):
            if owner is not None and f not in forks_of(owner):
                problems.append(f"F{f} held by non-adjacent P{owner}")
        if problems:
            self.violations += len(problems)
            self.violation_details.append((context, problems))
            print("!!! INVARIANT VIOLATION:", problems, file=sys.stderr)

    def _test(self, i, caller):
        """PRIVATE monitor procedure: let philosopher i eat if it is hungry
        and neither neighbour is eating. If so, BOTH forks are handed to i in
        one atomic step (we hold the monitor lock), then i is signalled.

        Fairness rule (enabled by default, --no-fair disables it): a hungry
        philosopher also must not 'overtake' a hungry neighbour that has been
        waiting longer (smaller ticket). Without it the two neighbours of i
        could alternate eating forever and starve i."""
        if not (self.state[i] == HUNGRY
                and self.state[left(i)] != EATING
                and self.state[right(i)] != EATING):
            return False
        if self.fair:
            for n in (left(i), right(i)):
                if (self.state[n] == HUNGRY
                        and self.hungry_ticket[n] < self.hungry_ticket[i]):
                    return False  # neighbour n waited longer: let it go first
        # --- grant: take both forks atomically --------------------------
        self.state[i] = EATING
        self.hungry_ticket[i] = None
        for f in forks_of(i):
            self.forks[f] = i
        self.display[i] = "ACQUIRING"
        lf, rf = forks_of(i)
        if caller == i:
            self._emit(i, "ACQUIRE",
                       f"P{i} picks up F{lf} and F{rf} (both free)")
        else:
            # Called from a neighbour's putdown(): this is the signal that
            # wakes a philosopher blocked on its condition variable.
            self.signals += 1
            self._emit(caller, "SIGNAL",
                       f"P{caller} signals self[{i}]; F{lf}+F{rf} -> P{i}")
        self._self[i].notify()  # self[i].signal()
        return True

    # ---------------- ENTRY procedures -----------------------------------
    def pickup(self, i):
        """Entry procedure: called by philosopher i when hungry. Returns only
        when philosopher i owns both forks (state[i] == EATING)."""
        with self._lock:                                  # enter monitor
            self.state[i] = HUNGRY
            self.display[i] = "HUNGRY"
            self.hungry_ticket[i] = self._next_ticket
            self._next_ticket += 1
            self._emit(i, "HUNGRY", f"P{i} is hungry, enters pickup({i})")
            self._test(i, caller=i)
            # Mesa semantics -> re-check the condition in a WHILE loop.
            while self.state[i] != EATING:
                self.display[i] = "WAITING"
                self.waits += 1
                blockers = [f"P{n} {self.state[n].lower()}"
                            for n in (left(i), right(i))
                            if self.state[n] != THINKING]
                self._emit(i, "WAIT",
                           f"P{i} waits on self[{i}] ({', '.join(blockers)})")
                self._self[i].wait()     # releases monitor lock while blocked
                # lock re-acquired here; loop re-checks state[i]
            self.display[i] = "EATING"
            self.meals[i] += 1
            lf, rf = forks_of(i)
            self._emit(i, "EAT",
                       f"P{i} starts eating meal #{self.meals[i]} "
                       f"with F{lf}+F{rf}")
                                                          # leave monitor

    def putdown(self, i):
        """Entry procedure: philosopher i finished eating. Releases both forks
        and tests both neighbours (who may be waiting for those forks)."""
        with self._lock:                                  # enter monitor
            lf, rf = forks_of(i)
            self.display[i] = "RELEASING"
            self._emit(i, "RELEASE", f"P{i} puts down F{lf} and F{rf}")
            self.state[i] = THINKING
            for f in forks_of(i):
                self.forks[f] = None
            # Give the freed forks to a waiting neighbour if possible.
            self._test(left(i), caller=i)
            self._test(right(i), caller=i)
            self.display[i] = "THINKING"
                                                          # leave monitor

    def note(self, i, display_state, kind, message):
        """Record a state change that happens OUTSIDE the critical section
        (e.g. starting to think). Still takes the lock so the snapshot of the
        table is consistent."""
        with self._lock:
            self.display[i] = display_state
            self._emit(i, kind, message)


# --------------------------------------------------------------------------
# Independent safety check performed by the threads themselves, outside the
# monitor: who is *physically* eating right now (between pickup and putdown)?
# --------------------------------------------------------------------------
class TableObserver:
    def __init__(self):
        self._lock = threading.Lock()
        self.eating_now = [False] * N
        self.violations = 0

    def begin(self, i):
        with self._lock:
            if self.eating_now[left(i)] or self.eating_now[right(i)]:
                self.violations += 1
                print(f"!!! P{i} eating next to an eating neighbour",
                      file=sys.stderr)
            self.eating_now[i] = True

    def end(self, i):
        with self._lock:
            self.eating_now[i] = False


# --------------------------------------------------------------------------
# Philosopher thread
# --------------------------------------------------------------------------
def philosopher(i, monitor, observer, args, wait_stats):
    rng = random.Random(args.seed * 1000 + i)  # per-thread reproducible delays
    for meal in range(1, args.meals + 1):
        # THINK (outside the monitor - no shared data touched)
        think = rng.uniform(args.max_think * 0.2, args.max_think)
        monitor.note(i, "THINKING", "THINK",
                     f"P{i} thinks for {think * 1000:.0f} ms")
        time.sleep(think)

        # HUNGRY -> try to take both forks through the monitor
        t_hungry = time.perf_counter()
        monitor.pickup(i)
        wait_stats[i].append(time.perf_counter() - t_hungry)

        # EAT (outside the monitor; the forks are reserved for us)
        observer.begin(i)
        eat = rng.uniform(args.max_eat * 0.2, args.max_eat)
        time.sleep(eat)
        observer.end(i)

        monitor.putdown(i)
    monitor.note(i, "DONE", "DONE", f"P{i} finished all {args.meals} meals")


# --------------------------------------------------------------------------
# HTML generation
# --------------------------------------------------------------------------
def generate_html(path, events, summary):
    data = json.dumps({"events": events, "summary": summary})
    data = data.replace("</", "<\\/")  # never close the <script> tag early
    page = HTML_TEMPLATE.replace("__DATA__", data).replace(
        "__TITLE__", html.escape("Dining Philosophers - Monitor (N=4)"))
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(page)


def default_output():
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(
        here, "..", "..", "generated_html", "dining_philosophers_monitor.html"))


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Dining Philosophers (4 philosophers) with a monitor; "
                    "records events and generates an HTML simulation.")
    p.add_argument("--meals", type=int, default=3,
                   help="meals each philosopher eats before exiting (default 3)")
    p.add_argument("--seed", type=int, default=42,
                   help="random seed for think/eat delays (default 42)")
    p.add_argument("--max-think", type=float, default=0.30,
                   help="max thinking time in seconds (default 0.30)")
    p.add_argument("--max-eat", type=float, default=0.20,
                   help="max eating time in seconds (default 0.20)")
    p.add_argument("--fair", action=argparse.BooleanOptionalAction,
                   default=True,
                   help="anti-starvation ticket rule (default on; "
                        "--no-fair = textbook monitor)")
    p.add_argument("--output", default=default_output(),
                   help="path of the generated HTML file")
    p.add_argument("--quiet", action="store_true",
                   help="do not print every event to the console")
    a = p.parse_args(argv)
    if a.meals < 1 or a.max_think <= 0 or a.max_eat <= 0:
        p.error("--meals must be >= 1 and delays must be > 0")
    return a


def main(argv=None):
    args = parse_args(argv)
    recorder = EventRecorder(quiet=args.quiet)
    monitor = DiningMonitor(recorder, fair=args.fair)
    observer = TableObserver()
    wait_stats = [[] for _ in range(N)]

    print(f"Dining Philosophers with a MONITOR: N={N}, meals={args.meals}, "
          f"seed={args.seed}, max_think={args.max_think}s, "
          f"max_eat={args.max_eat}s, fair={args.fair}")
    threads = [threading.Thread(target=philosopher, name=f"Philosopher-{i}",
                                args=(i, monitor, observer, args, wait_stats))
               for i in range(N)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    # Generous timeout: if anything ever deadlocked we would report it
    # instead of hanging forever.
    timeout = 30 + args.meals * (args.max_think + args.max_eat) * N * 4
    for t in threads:
        t.join(timeout)
    alive = [t.name for t in threads if t.is_alive()]
    elapsed = time.perf_counter() - t_start

    total_violations = monitor.violations + observer.violations
    max_wait = [round(max(w) * 1000, 1) if w else 0.0 for w in wait_stats]
    summary = {
        "n": N,
        "meals_requested": args.meals,
        "seed": args.seed,
        "max_think": args.max_think,
        "max_eat": args.max_eat,
        "fair": args.fair,
        "elapsed_ms": round(elapsed * 1000, 1),
        "meals": monitor.meals,
        "max_wait_ms": max_wait,
        "violations": total_violations,
        "signals": monitor.signals,
        "waits": monitor.waits,
        "terminated": not alive,
        "event_count": len(recorder.events),
        "command": "python3 dining_philosophers_monitor.py " + " ".join(
            sys.argv[1:] if argv is None else argv),
    }

    print("\n========== SUMMARY ==========")
    print(f"Terminated normally : {not alive}" +
          (f" (still alive: {alive})" if alive else ""))
    print(f"Elapsed             : {elapsed:.3f} s, {len(recorder.events)} events")
    for i in range(N):
        print(f"  P{i}: meals={monitor.meals[i]}  "
              f"longest hungry wait={max_wait[i]} ms")
    print(f"Signals / waits     : {monitor.signals} / {monitor.waits}")
    print(f"Invariant violations: {total_violations} "
          f"(monitor={monitor.violations}, observer={observer.violations})")

    generate_html(args.output, recorder.events, summary)
    print(f"HTML simulation     : {os.path.abspath(args.output)}")
    return 0 if (not alive and total_violations == 0) else 1


# --------------------------------------------------------------------------
# Self-contained HTML template (inline CSS + JS, events embedded as JSON)
# --------------------------------------------------------------------------
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  :root { --bg:#f6f7fb; --panel:#fff; --ink:#1f2430; --muted:#6b7280; --line:#d9dce5;
          --thinking:#3b82f6; --hungry:#f59e0b; --waiting:#ef4444; --acquiring:#a855f7;
          --eating:#16a34a; --releasing:#0ea5e9; --done:#9ca3af; }
  * { box-sizing:border-box; }
  body { margin:0; font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
         background:var(--bg); color:var(--ink); }
  header { padding:14px 20px; background:#1f2430; color:#fff; }
  header h1 { margin:0; font-size:20px; }
  header p { margin:4px 0 0; font-size:13px; color:#cbd5e1; }
  main { display:grid; grid-template-columns:minmax(320px,520px) 1fr; gap:16px; padding:16px; }
  @media (max-width:900px) { main { grid-template-columns:1fr; } }
  .panel { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:14px; }
  .panel h2 { font-size:15px; margin:0 0 10px; }
  svg { width:100%; height:auto; display:block; }
  .controls { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin-top:10px; }
  button { border:1px solid var(--line); background:#fff; padding:6px 12px; border-radius:6px;
           cursor:pointer; font-size:14px; }
  button:hover { background:#eef2ff; }
  button.primary { background:#1f2430; color:#fff; border-color:#1f2430; }
  .legend { display:flex; flex-wrap:wrap; gap:10px; font-size:12px; margin-top:10px; }
  .legend span::before { content:""; display:inline-block; width:10px; height:10px;
                         border-radius:50%; margin-right:4px; background:var(--c); }
  #current { font-family:ui-monospace,Menlo,Consolas,monospace; font-size:13px; padding:8px;
             background:#f1f5f9; border-radius:6px; min-height:36px; margin-top:10px; }
  table { border-collapse:collapse; width:100%; font-size:13px; }
  th, td { border-bottom:1px solid var(--line); padding:5px 6px; text-align:left; }
  #timeline { height:420px; overflow-y:auto; font-family:ui-monospace,Menlo,Consolas,monospace;
              font-size:12px; border:1px solid var(--line); border-radius:6px; }
  .ev { padding:3px 8px; border-bottom:1px solid #f0f1f5; cursor:pointer; opacity:.45; }
  .ev.past { opacity:1; }
  .ev.cur { background:#fef9c3; opacity:1; font-weight:600; }
  .tag { display:inline-block; min-width:64px; font-weight:600; }
  .ok { color:#16a34a; font-weight:600; } .bad { color:#dc2626; font-weight:600; }
  .note { font-size:12px; color:var(--muted); line-height:1.45; }
</style>
</head>
<body>
<header>
  <h1>Dining Philosophers &mdash; Monitor solution (4 philosophers)</h1>
  <p id="subtitle"></p>
</header>
<main>
  <section class="panel">
    <h2>Round table</h2>
    <svg id="table" viewBox="0 0 400 400" role="img" aria-label="Dining table"></svg>
    <div class="legend">
      <span style="--c:var(--thinking)">Thinking</span>
      <span style="--c:var(--hungry)">Hungry</span>
      <span style="--c:var(--waiting)">Waiting (blocked on self[i])</span>
      <span style="--c:var(--acquiring)">Acquiring forks</span>
      <span style="--c:var(--eating)">Eating</span>
      <span style="--c:var(--releasing)">Releasing forks</span>
      <span style="--c:var(--done)">Done</span>
    </div>
    <div class="controls">
      <button id="play" class="primary">Play</button>
      <button id="step">Step &rarr;</button>
      <button id="back">&larr; Back</button>
      <button id="reset">Reset</button>
      <label>Speed <input id="speed" type="range" min="1" max="40" value="6"></label>
      <span id="speedLbl"></span>
    </div>
    <div id="current"></div>
    <div class="note" style="margin-top:6px">Event <b id="pos">0</b> / <span id="total"></span>
      &middot; t = <b id="time">0</b> ms</div>
  </section>
  <section class="panel">
    <h2>Philosophers</h2>
    <table>
      <thead><tr><th>Philosopher</th><th>State</th><th>Monitor state[i]</th><th>Forks held</th><th>Meals</th></tr></thead>
      <tbody id="ptable"></tbody>
    </table>
    <h2 style="margin-top:14px">Event timeline <span class="note">(click an event to jump)</span></h2>
    <div id="timeline"></div>
    <h2 style="margin-top:14px">Run summary</h2>
    <div id="summary" class="note"></div>
  </section>
</main>
<script id="run-data" type="application/json">__DATA__</script>
<script>
(function () {
  "use strict";
  var DATA = JSON.parse(document.getElementById("run-data").textContent);
  var EV = DATA.events, S = DATA.summary, N = S.n;
  var COLORS = { THINKING:"var(--thinking)", HUNGRY:"var(--hungry)", WAITING:"var(--waiting)",
                 ACQUIRING:"var(--acquiring)", EATING:"var(--eating)",
                 RELEASING:"var(--releasing)", DONE:"var(--done)" };
  var NS = "http://www.w3.org/2000/svg", C = 200, RP = 140, RF = 92;
  function el(tag, attrs, parent) {
    var e = document.createElementNS(NS, tag);
    for (var k in attrs) e.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(e); return e;
  }
  function pos(angleDeg, r) {
    var a = angleDeg * Math.PI / 180; return [C + r * Math.cos(a), C + r * Math.sin(a)];
  }
  // Philosopher i sits at angle -90 + 90*i (P0 top, clockwise).
  // forks_of(i) = (i, i+1) in Python, so fork f is shared by P(f-1) and P(f)
  // and is drawn half-way between them.
  function philAngle(i) { return -90 + 360 / N * i; }
  function forkAngle(f) { return philAngle(f) - 180 / N; }

  var svg = document.getElementById("table");
  el("circle", { cx:C, cy:C, r:110, fill:"#fde68a", stroke:"#b45309", "stroke-width":3 }, svg);
  el("text", { x:C, y:C+5, "text-anchor":"middle", "font-size":13, fill:"#92400e" }, svg).textContent = "Monitor";
  var forkG = [], philG = [];
  for (var f = 0; f < N; f++) {
    var g = el("g", {}, svg);
    var line = el("line", { "stroke-width":7, "stroke-linecap":"round" }, g);
    var lbl = el("text", { "text-anchor":"middle", "font-size":11, "font-weight":"bold" }, g);
    forkG.push({ line:line, lbl:lbl });
  }
  for (var i = 0; i < N; i++) {
    var p = pos(philAngle(i), RP), g2 = el("g", {}, svg);
    var circ = el("circle", { cx:p[0], cy:p[1], r:36, stroke:"#1f2430", "stroke-width":2 }, g2);
    var name = el("text", { x:p[0], y:p[1]-4, "text-anchor":"middle", "font-size":15,
                            "font-weight":"bold", fill:"#fff" }, g2);
    name.textContent = "P" + i;
    var st = el("text", { x:p[0], y:p[1]+12, "text-anchor":"middle", "font-size":9, fill:"#fff" }, g2);
    var meals = el("text", { x:p[0], y:p[1]+(i === 0 ? -44 : 50), "text-anchor":"middle",
                             "font-size":11, fill:"#1f2430" }, g2);
    philG.push({ circ:circ, st:st, meals:meals });
  }

  function drawFork(f, owner) {
    var a = forkAngle(f), r1 = RF - 30, r2 = RF + 8;
    var pl = pos(a, r1 - 16);           // label stays at the fork's home spot
    if (owner !== null) {               // slide the fork towards its owner
      a += (owner === f) ? 26 : -26;    // owner f sits at a+45, owner f-1 at a-45
    }
    var p1 = pos(a, r1), p2 = pos(a, r2);
    var col = owner === null ? "#6b7280" : "var(--eating)";
    var fg = forkG[f];
    fg.line.setAttribute("x1", p1[0]); fg.line.setAttribute("y1", p1[1]);
    fg.line.setAttribute("x2", p2[0]); fg.line.setAttribute("y2", p2[1]);
    fg.line.setAttribute("stroke", owner === null ? "#9ca3af" : "#15803d");
    fg.lbl.setAttribute("x", pl[0]); fg.lbl.setAttribute("y", pl[1] + 4);
    fg.lbl.setAttribute("fill", col);
    fg.lbl.textContent = "F" + f + (owner === null ? " free" : " →P" + owner);
  }

  var initial = { states:Array(N).fill("THINKING"), mstates:Array(N).fill("THINKING"),
                  forks:Array(N).fill(null), meals:Array(N).fill(0) };
  function render(k) {   // k = number of events applied (0..EV.length)
    var snap = k === 0 ? initial : EV[k - 1];
    for (var i = 0; i < N; i++) {
      philG[i].circ.setAttribute("fill", COLORS[snap.states[i]] || "#999");
      philG[i].st.textContent = snap.states[i];
      philG[i].meals.textContent = "meals: " + snap.meals[i];
    }
    for (var f = 0; f < N; f++) drawFork(f, snap.forks[f]);
    var rows = "";
    for (var j = 0; j < N; j++) {
      var held = [];
      for (var f2 = 0; f2 < N; f2++) if (snap.forks[f2] === j) held.push("F" + f2);
      rows += "<tr><td><b>P" + j + "</b></td><td style='color:" + COLORS[snap.states[j]] +
              ";font-weight:600'>" + snap.states[j] + "</td><td>" + snap.mstates[j] +
              "</td><td>" + (held.join(", ") || "&ndash;") + "</td><td>" + snap.meals[j] + "</td></tr>";
    }
    document.getElementById("ptable").innerHTML = rows;
    document.getElementById("pos").textContent = k;
    document.getElementById("time").textContent = k === 0 ? "0" : snap.t.toFixed(1);
    document.getElementById("current").textContent = k === 0 ? "Press Play or Step to start the replay." :
      "[" + snap.t.toFixed(2) + " ms] " + snap.kind + ": " + snap.msg + "  (" + snap.thread + ")";
    for (var m = 0; m < rowsEl.length; m++) {
      rowsEl[m].className = "ev" + (m < k - 1 ? " past" : "") + (m === k - 1 ? " cur" : "");
    }
    if (k > 0) {
      var cur = rowsEl[k - 1], tl = document.getElementById("timeline");
      if (cur.offsetTop < tl.scrollTop || cur.offsetTop > tl.scrollTop + tl.clientHeight - 30)
        tl.scrollTop = cur.offsetTop - tl.clientHeight / 2;
    }
  }

  // Timeline
  var tl = document.getElementById("timeline"), rowsEl = [];
  var KCOL = { THINK:"var(--thinking)", HUNGRY:"var(--hungry)", WAIT:"var(--waiting)",
               ACQUIRE:"var(--acquiring)", SIGNAL:"var(--acquiring)", EAT:"var(--eating)",
               RELEASE:"var(--releasing)", DONE:"var(--done)" };
  EV.forEach(function (e, idx) {
    var d = document.createElement("div");
    d.className = "ev";
    d.innerHTML = "<span style='color:var(--muted)'>" + e.t.toFixed(1).padStart(8, " ") +
      " ms</span> <span class='tag' style='color:" + (KCOL[e.kind] || "#333") + "'>" + e.kind +
      "</span> " + e.msg.replace(/</g, "&lt;");
    d.addEventListener("click", function () { pause(); k = idx + 1; render(k); });
    tl.appendChild(d); rowsEl.push(d);
  });

  // Summary
  var okV = S.violations === 0;
  document.getElementById("subtitle").textContent =
    "Recorded from a real multi-threaded run: " + S.event_count + " events, seed " + S.seed +
    ", " + S.meals_requested + " meals each, fairness rule " + (S.fair ? "ON" : "OFF") + ".";
  var mealsTxt = S.meals.map(function (m, i) { return "P" + i + "=" + m; }).join(", ");
  var waitTxt = S.max_wait_ms.map(function (w, i) { return "P" + i + "=" + w + " ms"; }).join(", ");
  document.getElementById("summary").innerHTML =
    "Command: <code>" + S.command.replace(/</g, "&lt;") + "</code><br>" +
    "Run time: " + S.elapsed_ms + " ms &middot; max think " + S.max_think + " s &middot; max eat " +
    S.max_eat + " s<br>Meals: " + mealsTxt + "<br>Longest hungry wait: " + waitTxt + "<br>" +
    "Signals: " + S.signals + " &middot; waits: " + S.waits + "<br>" +
    "Adjacent-eating violations: <span class='" + (okV ? "ok" : "bad") + "'>" + S.violations +
    "</span> &middot; terminated: <span class='" + (S.terminated ? "ok" : "bad") + "'>" +
    S.terminated + "</span>";
  document.getElementById("total").textContent = EV.length;

  // Playback
  var k = 0, timer = null;
  var speed = document.getElementById("speed"), playBtn = document.getElementById("play");
  function interval() { return 1000 / Number(speed.value); }
  function updSpeed() { document.getElementById("speedLbl").textContent = speed.value + " events/s"; }
  function tick() { if (k < EV.length) { k++; render(k); } if (k >= EV.length) pause(); }
  function play() { if (k >= EV.length) k = 0; timer = setInterval(tick, interval()); playBtn.textContent = "Pause"; }
  function pause() { if (timer) clearInterval(timer); timer = null; playBtn.textContent = "Play"; }
  playBtn.onclick = function () { timer ? pause() : play(); };
  document.getElementById("step").onclick = function () { pause(); if (k < EV.length) { k++; render(k); } };
  document.getElementById("back").onclick = function () { pause(); if (k > 0) { k--; render(k); } };
  document.getElementById("reset").onclick = function () { pause(); k = 0; render(0); tl.scrollTop = 0; };
  speed.oninput = function () { updSpeed(); if (timer) { pause(); play(); } };
  updSpeed(); render(0);
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    sys.exit(main())
