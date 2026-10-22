#!/usr/bin/env python3
"""
Readers-Writers Problem -- MONITOR-based solution (Python 3, standard library only).

Pipeline (as required by the assignment):
    Execute program -> Synchronize (monitor) -> Capture state changes
    -> Generate a self-contained HTML simulation -> View it in a browser

Python has no `monitor` keyword, so the class ReadWriteMonitor below builds one
explicitly from the two ingredients every monitor is made of:

  * ONE mutual-exclusion lock (threading.Lock) that every entry procedure
    acquires on entry and releases on exit.  At most one thread is ever
    "inside the monitor", exactly like a Hoare/Brinch-Hansen monitor or a Java
    `synchronized` object.
  * CONDITION VARIABLES (threading.Condition) bound to that same lock:
        ok_to_read   -- readers wait here
        ok_to_write  -- writers wait here
    cond.wait()       = release the monitor lock, sleep, re-acquire on wake-up
    cond.notify()     = signal   (wake one waiter)
    cond.notify_all() = broadcast (wake every waiter)

Python conditions have MESA semantics ("signal and continue"): a woken thread
does not run immediately; it must re-acquire the lock and by then the state may
have changed.  Therefore every wait is inside a `while <cannot proceed>` loop.

The only public entry procedures are start_read / end_read / start_write /
end_write.  All monitor state is private and only touched while holding the
monitor lock.

SHARED RESOURCE: the SharedDatabase object (an integer `value` plus a
`checksum` copy).  CRITICAL SECTION: the code between start_read()/end_read()
(reading) and start_write()/end_write() (writing) -- see SharedDatabase.read()
and SharedDatabase.write().  Note that the *resource* critical section is
executed OUTSIDE the monitor lock; the monitor only decides who may enter.
That is what lets several readers read at the same time.

Policy -- writer preference with reader batching (starvation free):
  * A newly arriving reader waits if a writer is writing OR any writer is
    already waiting (so a stream of readers cannot starve writers).
  * When a writer finishes, ALL readers that were waiting at that moment are
    admitted as one batch, ahead of the other waiting writers (so a stream of
    writers cannot starve readers).  Writers stay out until that batch has
    entered.
  * Bound: a writer waits for at most the readers already inside plus one
    batch; a reader waits for at most one writer.

Run:  python3 readers_writers_monitor.py --readers 5 --writers 2 --iterations 3
"""

import argparse
import json
import os
import random
import threading
import time
from datetime import datetime

# --------------------------------------------------------------------------- #
#  Event recorder (thread-safe)                                               #
# --------------------------------------------------------------------------- #


class EventRecorder:
    """Collects timestamped state-change events produced by the running threads.

    Each event stores a full snapshot of the monitor + shared resource + every
    thread's state, so the HTML player can jump to any point of the trace.
    """

    def __init__(self, actors, verbose=True):
        self._lock = threading.Lock()          # protects the event list only
        self._start = time.perf_counter()
        self._events = []
        self._states = {a: "Idle" for a in actors}
        self._verbose = verbose
        self.monitor = None                    # set after construction
        self.database = None

    def log(self, actor, kind, message, state=None, **extra):
        """Record one event.  Monitor events are logged while the caller holds
        the monitor lock, so their snapshots are exactly consistent."""
        with self._lock:
            t_ms = (time.perf_counter() - self._start) * 1000.0
            if state is not None:
                self._states[actor] = state
            event = {
                "seq": len(self._events),
                "t": round(t_ms, 3),
                "actor": actor,
                "kind": kind,
                "msg": message,
                "threads": dict(self._states),
                "monitor": self.monitor.snapshot() if self.monitor else {},
                "resource": self.database.snapshot() if self.database else {},
            }
            event.update(extra)
            self._events.append(event)
            if self._verbose:
                m = event["monitor"]
                print(f"[{t_ms:9.2f} ms] {actor:<4} {kind:<14} {message:<48} "
                      f"| active_readers={m.get('active_readers', 0)} "
                      f"writer={m.get('active_writer') or '-':<3} "
                      f"wait_R={len(m.get('ok_to_read_queue', []))} "
                      f"wait_W={len(m.get('ok_to_write_queue', []))}",
                      flush=True)

    @property
    def events(self):
        with self._lock:
            return list(self._events)


# --------------------------------------------------------------------------- #
#  The monitor                                                                #
# --------------------------------------------------------------------------- #


class ReadWriteMonitor:
    """Monitor that controls access to the shared database.

    Entry procedures: start_read, end_read, start_write, end_write.
    Everything else is private monitor state guarded by self._lock.
    """

    def __init__(self, recorder):
        # Monitor lock: holding it == being "inside the monitor".
        self._lock = threading.Lock()
        # Condition variables share the monitor lock (standard monitor design).
        self.ok_to_read = threading.Condition(self._lock)
        self.ok_to_write = threading.Condition(self._lock)

        # ---- private monitor state (only touched while holding _lock) ----
        self._active_readers = []      # IDs of readers currently reading
        self._active_writer = None     # ID of the writer currently writing
        self._waiting_readers = []     # IDs blocked on ok_to_read  (FIFO view)
        self._waiting_writers = []     # IDs blocked on ok_to_write (FIFO view)
        self._write_epoch = 0          # incremented every time a writer finishes
        self._admitted_readers = 0     # readers released by a writer, not yet in

        self._rec = recorder

    # ---- snapshot for the recorder (caller holds _lock or accepts staleness)
    def snapshot(self):
        return {
            "active_readers": len(self._active_readers),
            "reader_ids": list(self._active_readers),
            "writer_active": self._active_writer is not None,
            "active_writer": self._active_writer,
            "ok_to_read_queue": list(self._waiting_readers),
            "ok_to_write_queue": list(self._waiting_writers),
            "admitted_readers": self._admitted_readers,
            "write_epoch": self._write_epoch,
        }

    # ------------------------------------------------------------------ #
    #  Reader entry procedures                                            #
    # ------------------------------------------------------------------ #
    def start_read(self, rid):
        with self._lock:                                   # enter monitor
            my_epoch = self._write_epoch
            self._rec.log(rid, "request", "wants to read (enters monitor)",
                          state="Requesting")
            waited = False
            # Mesa semantics -> re-check the condition in a while loop.
            # Block if a writer is writing, or if writers are waiting and no
            # writer has finished since I arrived (writer preference for new
            # readers; readers that were waiting get a turn after each write).
            while (self._active_writer is not None or
                   (self._waiting_writers and self._write_epoch == my_epoch)):
                if not waited:
                    self._waiting_readers.append(rid)
                    waited = True
                reason = (f"writer {self._active_writer} is writing"
                          if self._active_writer else
                          f"writer(s) {','.join(self._waiting_writers)} waiting")
                self._rec.log(rid, "wait", f"wait(ok_to_read): {reason}",
                              state="Waiting", cond="ok_to_read")
                self.ok_to_read.wait()        # releases lock while sleeping
                self._rec.log(rid, "wakeup", "woke on ok_to_read, re-checking",
                              state="Waiting", cond="ok_to_read")
            if waited:
                self._waiting_readers.remove(rid)
                if self._write_epoch != my_epoch:
                    # I was part of the batch released by end_write().
                    self._admitted_readers -= 1
            self._active_readers.append(rid)
            self._rec.log(rid, "enter_cs",
                          f"ENTER critical section (reading), "
                          f"active readers={len(self._active_readers)}",
                          state="Reading")
        # leaving the monitor; the actual reading happens outside the lock

    def end_read(self, rid):
        with self._lock:
            self._active_readers.remove(rid)
            self._rec.log(rid, "exit_cs",
                          f"EXIT critical section, active readers="
                          f"{len(self._active_readers)}", state="Idle")
            # Last reader out lets one waiting writer in.
            if not self._active_readers and self._waiting_writers:
                self.ok_to_write.notify()
                self._rec.log(rid, "signal",
                              f"signal(ok_to_write) -> wake one of "
                              f"{','.join(self._waiting_writers)}",
                              cond="ok_to_write")

    # ------------------------------------------------------------------ #
    #  Writer entry procedures                                            #
    # ------------------------------------------------------------------ #
    def start_write(self, wid):
        with self._lock:
            self._rec.log(wid, "request", "wants to write (enters monitor)",
                          state="Requesting")
            waited = False
            # A writer needs exclusive access: nobody reading, nobody writing,
            # and no admitted reader batch still on its way in.
            while (self._active_writer is not None or self._active_readers or
                   self._admitted_readers > 0):
                if not waited:
                    self._waiting_writers.append(wid)
                    waited = True
                if self._active_writer:
                    reason = f"writer {self._active_writer} is writing"
                elif self._active_readers:
                    reason = f"readers {','.join(self._active_readers)} reading"
                else:
                    reason = f"{self._admitted_readers} admitted reader(s) entering"
                self._rec.log(wid, "wait", f"wait(ok_to_write): {reason}",
                              state="Waiting", cond="ok_to_write")
                self.ok_to_write.wait()
                self._rec.log(wid, "wakeup", "woke on ok_to_write, re-checking",
                              state="Waiting", cond="ok_to_write")
            if waited:
                self._waiting_writers.remove(wid)
            self._active_writer = wid
            self._rec.log(wid, "enter_cs", "ENTER critical section (writing, exclusive)",
                          state="Writing")

    def end_write(self, wid):
        with self._lock:
            self._active_writer = None
            self._write_epoch += 1
            self._rec.log(wid, "exit_cs", "EXIT critical section", state="Idle")
            if self._waiting_readers:
                # Give the waiting readers a turn (prevents reader starvation).
                self._admitted_readers = len(self._waiting_readers)
                self.ok_to_read.notify_all()
                self._rec.log(wid, "broadcast",
                              f"broadcast(ok_to_read) -> admit batch "
                              f"{','.join(self._waiting_readers)}",
                              cond="ok_to_read")
            elif self._waiting_writers:
                self.ok_to_write.notify()
                self._rec.log(wid, "signal",
                              f"signal(ok_to_write) -> wake one of "
                              f"{','.join(self._waiting_writers)}",
                              cond="ok_to_write")


# --------------------------------------------------------------------------- #
#  Shared resource + independent invariant checker                           #
# --------------------------------------------------------------------------- #


class SharedDatabase:
    """THE SHARED RESOURCE.

    `value` and `checksum` must always be equal when nobody is writing.  A
    writer updates them in two steps with a delay in between, so if the
    monitor ever let a reader in during a write the reader would observe a
    torn (inconsistent) record.

    The occupancy counters below are an *independent* checker that does not
    trust the monitor: it counts who is physically inside the critical section
    and counts invariant violations (expected: 0).
    """

    def __init__(self, recorder):
        self.value = 0
        self.checksum = 0
        self.last_writer = None
        self.write_count = 0
        self.read_count = 0
        # checker state (own tiny lock, separate from the monitor lock)
        self._check_lock = threading.Lock()
        self._readers_inside = 0
        self._writers_inside = 0
        self.max_concurrent_readers = 0
        self.violations = []
        self.torn_reads = 0
        self._rec = recorder

    def snapshot(self):
        return {
            "value": self.value,
            "checksum": self.checksum,
            "last_writer": self.last_writer,
            "readers_inside": self._readers_inside,
            "writers_inside": self._writers_inside,
            "violations": len(self.violations),
        }

    def _violation(self, actor, text):
        self.violations.append(text)
        self._rec.log(actor, "VIOLATION", text)

    # ---------------- CRITICAL SECTION: reading ----------------
    def read(self, rid, duration):
        with self._check_lock:
            self._readers_inside += 1
            self.max_concurrent_readers = max(self.max_concurrent_readers,
                                              self._readers_inside)
            if self._writers_inside:
                self._violation(rid, "reader inside while a writer is writing")
        v1, c1 = self.value, self.checksum
        self._rec.log(rid, "read", f"reads value={v1}", state="Reading", value=v1)
        time.sleep(duration)                 # reading takes time
        v2 = self.value
        if not (v1 == c1 == v2):
            self.torn_reads += 1
            self._violation(rid, f"torn read: value={v1} checksum={c1} later={v2}")
        with self._check_lock:
            self._readers_inside -= 1
            self.read_count += 1
        return v1

    # ---------------- CRITICAL SECTION: writing ----------------
    def write(self, wid, new_value, duration):
        with self._check_lock:
            self._writers_inside += 1
            if self._writers_inside > 1:
                self._violation(wid, "two writers inside at once")
            if self._readers_inside:
                self._violation(wid, "writer inside while readers are reading")
        old = self.value
        self.value = new_value               # step 1 of the update
        self._rec.log(wid, "write", f"writes value {old} -> {new_value}",
                      state="Writing", value=new_value)
        time.sleep(duration)                 # a non-atomic, slow update
        self.checksum = new_value            # step 2 of the update
        self.last_writer = wid
        with self._check_lock:
            self._writers_inside -= 1
            self.write_count += 1


# --------------------------------------------------------------------------- #
#  Threads                                                                    #
# --------------------------------------------------------------------------- #


def reader_thread(rid, monitor, db, rec, iterations, max_delay, rng):
    for i in range(iterations):
        think = rng.uniform(0, max_delay)
        rec.log(rid, "idle", f"idle / doing other work ({think*1000:.0f} ms), "
                f"round {i+1}/{iterations}", state="Idle")
        time.sleep(think)
        monitor.start_read(rid)                       # entry protocol
        db.read(rid, rng.uniform(max_delay * 0.2, max_delay))   # critical section
        monitor.end_read(rid)                         # exit protocol
    rec.log(rid, "done", "finished all rounds", state="Done")


def writer_thread(wid, monitor, db, rec, iterations, max_delay, rng):
    for i in range(iterations):
        think = rng.uniform(0, max_delay)
        rec.log(wid, "idle", f"idle / preparing data ({think*1000:.0f} ms), "
                f"round {i+1}/{iterations}", state="Idle")
        time.sleep(think)
        increment = rng.randint(1, 9)
        monitor.start_write(wid)                      # entry protocol
        db.write(wid, db.value + increment,           # critical section
                 rng.uniform(max_delay * 0.2, max_delay))
        monitor.end_write(wid)                        # exit protocol
    rec.log(wid, "done", "finished all rounds", state="Done")


# --------------------------------------------------------------------------- #
#  HTML generation                                                            #
# --------------------------------------------------------------------------- #

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Readers-Writers Monitor</title>
<style>
:root{--bg:#f6f7f9;--panel:#fff;--ink:#1d2330;--muted:#6b7385;--line:#dde1e8;
 --idle:#9aa3b2;--req:#8b6cd9;--wait:#e3a008;--read:#2f7de1;--write:#d9433b;--done:#3a9d5d;
 --hl:#fff4c2}
@media (prefers-color-scheme:dark){:root{--bg:#14171d;--panel:#1d2129;--ink:#e6e9ef;
 --muted:#9099aa;--line:#2e3440;--hl:#3b3520}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
header{padding:16px 20px 8px}
h1{margin:0 0 4px;font-size:20px}
.sub{color:var(--muted);font-size:13px}
main{display:grid;grid-template-columns:minmax(0,1.25fr) minmax(0,1fr);gap:14px;padding:8px 20px 20px}
@media (max-width:900px){main{grid-template-columns:minmax(0,1fr);padding:8px 16px 16px}header{padding:12px 16px 4px}}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.panel h2{margin:0 0 10px;font-size:13px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted)}
.controls{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
button{font:inherit;padding:6px 12px;border-radius:7px;border:1px solid var(--line);background:var(--bg);color:var(--ink);cursor:pointer}
button.primary{background:var(--read);border-color:var(--read);color:#fff}
input[type=range]{accent-color:var(--read)}
.prog{flex:1 1 200px;min-width:140px}
.clock{font-variant-numeric:tabular-nums;color:var(--muted);font-size:13px}
.actors{display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:8px}
.actor{border:2px solid var(--line);border-radius:9px;padding:8px;transition:all .2s}
.actor .id{font-weight:700;font-size:16px}
.actor .st{font-size:12px;font-weight:600}
.actor .info{font-size:11px;color:var(--muted);min-height:15px}
.actor.cur{box-shadow:0 0 0 3px var(--hl)}
.s-Idle{border-color:var(--idle)}.s-Idle .st{color:var(--idle)}
.s-Requesting{border-color:var(--req)}.s-Requesting .st{color:var(--req)}
.s-Waiting{border-color:var(--wait);background:color-mix(in srgb,var(--wait) 10%,transparent)}.s-Waiting .st{color:var(--wait)}
.s-Reading{border-color:var(--read);background:color-mix(in srgb,var(--read) 12%,transparent)}.s-Reading .st{color:var(--read)}
.s-Writing{border-color:var(--write);background:color-mix(in srgb,var(--write) 12%,transparent)}.s-Writing .st{color:var(--write)}
.s-Done{border-color:var(--done);opacity:.7}.s-Done .st{color:var(--done)}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.box{border:1px dashed var(--line);border-radius:8px;padding:8px 10px}
.box .lbl{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
.big{font-size:26px;font-weight:700;font-variant-numeric:tabular-nums}
.queue{display:flex;gap:5px;flex-wrap:wrap;min-height:28px;align-items:center}
.chip{padding:3px 8px;border-radius:12px;font-size:12px;font-weight:600;color:#fff}
.chip.r{background:var(--read)}.chip.w{background:var(--write)}.chip.empty{background:none;color:var(--muted);font-weight:400}
.resource{border:2px solid var(--line);border-radius:10px;padding:10px;text-align:center;transition:all .2s}
.resource.reading{border-color:var(--read)}.resource.writing{border-color:var(--write)}
.legend{display:flex;gap:12px;flex-wrap:wrap;font-size:12px;color:var(--muted);margin-top:6px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:4px;vertical-align:-1px}
#log{height:340px;overflow:auto;font:12px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;border-top:1px solid var(--line)}
#log div{padding:2px 4px;border-bottom:1px solid var(--line);white-space:nowrap}
#log div.cur{background:var(--hl)}
#log .t{color:var(--muted)}
.k{display:inline-block;min-width:78px;font-weight:700}
.k-enter_cs{color:var(--done)}.k-exit_cs{color:var(--muted)}.k-wait{color:var(--wait)}.k-wakeup{color:var(--wait)}
.k-signal,.k-broadcast{color:var(--req)}.k-read{color:var(--read)}.k-write{color:var(--write)}.k-VIOLATION{color:#fff;background:var(--write)}
svg text{fill:var(--muted);font-size:11px}
.full{grid-column:1/-1}
.stats{display:flex;gap:18px;flex-wrap:wrap;font-size:13px}
.stats b{font-variant-numeric:tabular-nums}
.ok{color:var(--done)}.bad{color:var(--write)}
</style>
</head>
<body>
<header>
 <h1>Readers-Writers &mdash; Monitor solution (execution replay)</h1>
 <div class="sub" id="meta"></div>
</header>
<main>
 <section class="panel full">
  <div class="controls">
   <button class="primary" id="play">&#9654; Play</button>
   <button id="back">&#9664; Step</button>
   <button id="step">Step &#9654;</button>
   <button id="reset">&#8634; Reset</button>
   <label class="clock">Speed <input type="range" id="speed" min="1" max="40" value="6"> <span id="speedv"></span> ev/s</label>
   <input type="range" class="prog" id="prog" min="0" value="0">
   <span class="clock" id="clock"></span>
  </div>
  <div class="stats" style="margin-top:8px" id="stats"></div>
 </section>

 <section class="panel">
  <h2>Threads (readers &amp; writers)</h2>
  <div class="actors" id="actors"></div>
  <div class="legend">
   <span><i style="background:var(--idle)"></i>Idle</span>
   <span><i style="background:var(--req)"></i>Requesting (in monitor)</span>
   <span><i style="background:var(--wait)"></i>Waiting on condition</span>
   <span><i style="background:var(--read)"></i>Reading (in CS)</span>
   <span><i style="background:var(--write)"></i>Writing (in CS)</span>
   <span><i style="background:var(--done)"></i>Done</span>
  </div>
 </section>

 <section class="panel">
  <h2>Monitor &amp; shared resource</h2>
  <div class="resource" id="res">
   <div class="lbl" style="font-size:11px;color:var(--muted)">SHARED RESOURCE &mdash; database value</div>
   <div class="big" id="resval">0</div>
   <div id="resstat"></div>
  </div>
  <div class="grid2" style="margin-top:10px">
   <div class="box"><div class="lbl">active readers</div><div class="big" id="ar">0</div></div>
   <div class="box"><div class="lbl">writer active</div><div class="big" id="wa">no</div></div>
   <div class="box"><div class="lbl">waiting readers</div><div class="big" id="wr">0</div></div>
   <div class="box"><div class="lbl">waiting writers</div><div class="big" id="ww">0</div></div>
  </div>
  <div class="box" style="margin-top:10px"><div class="lbl">condition queue: ok_to_read</div><div class="queue" id="qr"></div></div>
  <div class="box" style="margin-top:8px"><div class="lbl">condition queue: ok_to_write</div><div class="queue" id="qw"></div></div>
  <div class="box" style="margin-top:8px"><div class="lbl">admitted reader batch (not yet entered) / write epoch</div><div id="adm"></div></div>
 </section>

 <section class="panel full">
  <h2>Timeline (thread states over real execution time &mdash; click to jump)</h2>
  <div id="gantt"></div>
 </section>

 <section class="panel full">
  <h2>Event sequence (<span id="evcount"></span>)</h2>
  <div id="cur" style="margin-bottom:6px;font-weight:600"></div>
  <div id="log"></div>
 </section>
</main>

<script id="trace" type="application/json">__TRACE_JSON__</script>
<script>
"use strict";
const TRACE = JSON.parse(document.getElementById("trace").textContent);
const EV = TRACE.events, ACTORS = TRACE.config.actors;
const $ = id => document.getElementById(id);
let idx = -1, timer = null;

$("meta").textContent = `Generated ${TRACE.generated_at} by ${TRACE.program} | readers=${TRACE.config.readers}, writers=${TRACE.config.writers}, iterations=${TRACE.config.iterations}, seed=${TRACE.config.seed}, max delay=${TRACE.config.max_delay}s | policy: ${TRACE.config.policy}`;
const S = TRACE.summary;
$("stats").innerHTML =
 `<span>events: <b>${EV.length}</b></span>` +
 `<span>reads: <b>${S.reads}</b></span><span>writes: <b>${S.writes}</b></span>` +
 `<span>final value: <b>${S.final_value}</b></span>` +
 `<span>max concurrent readers: <b>${S.max_concurrent_readers}</b></span>` +
 `<span>run time: <b>${S.duration_ms.toFixed(0)} ms</b></span>` +
 `<span>invariant violations: <b class="${S.violations ? "bad" : "ok"}">${S.violations}</b></span>`;
$("prog").max = EV.length;
$("evcount").textContent = EV.length + " events recorded during execution";

// actor cards
for (const a of ACTORS) {
  const d = document.createElement("div");
  d.className = "actor s-Idle"; d.id = "a-" + a;
  d.innerHTML = `<div class="id">${a}</div><div class="st">Idle</div><div class="info"></div>`;
  d.title = a.startsWith("R") ? "Reader thread" : "Writer thread";
  $("actors").appendChild(d);
}
// event log rows (all built up front; hidden until reached)
const rows = EV.map(e => {
  const d = document.createElement("div");
  d.innerHTML = `<span class="t">${e.t.toFixed(1).padStart(8)} ms</span> <b>${e.actor.padEnd(4)}</b> <span class="k k-${e.kind}">${e.kind}</span> ${esc(e.msg)}`;
  d.style.display = "none"; d.style.cursor = "pointer";
  d.onclick = () => { pause(); go(e.seq); };
  $("log").appendChild(d);
  return d;
});
function esc(s){return String(s).replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]));}
function chips(ids, cls){ return ids.length ? ids.map(i=>`<span class="chip ${cls}">${i}</span>`).join("") : '<span class="chip empty">empty</span>'; }

// ---- Gantt timeline built from the recorded state transitions ----
const T_END = EV.length ? EV[EV.length-1].t : 1;
const COLORS = {Idle:"var(--idle)",Requesting:"var(--req)",Waiting:"var(--wait)",Reading:"var(--read)",Writing:"var(--write)",Done:"var(--done)"};
(function buildGantt(){
  const W = 1000, lane = 20, top = 6, left = 40, H = top + ACTORS.length*lane + 22;
  let svg = `<svg viewBox="0 0 ${W} ${H}" width="100%" preserveAspectRatio="none" style="display:block;height:${H}px" id="gsvg">`;
  const x = t => left + (W-left-6) * t / T_END;
  ACTORS.forEach((a, i) => {
    const y = top + i*lane;
    svg += `<text x="4" y="${y+14}">${a}</text>`;
    let st = "Idle", t0 = 0;
    const flush = t1 => { if (t1 > t0) svg += `<rect x="${x(t0)}" y="${y+3}" width="${Math.max(x(t1)-x(t0),0.5)}" height="${lane-6}" fill="${COLORS[st]}" opacity="${st==="Idle"?0.35:0.9}"><title>${a}: ${st} ${t0.toFixed(1)}-${t1.toFixed(1)} ms</title></rect>`; };
    for (const e of EV) {
      const ns = e.threads[a];
      if (ns !== st) { flush(e.t); st = ns; t0 = e.t; }
    }
    flush(T_END);
  });
  for (let k=0;k<=5;k++){ const t=T_END*k/5; svg += `<text x="${x(t)-(k===5?40:0)}" y="${H-6}">${t.toFixed(0)} ms</text>`; }
  svg += `<line id="cursor" x1="${left}" x2="${left}" y1="0" y2="${H-18}" stroke="var(--ink)" stroke-width="2"/>`;
  svg += `</svg>`;
  $("gantt").innerHTML = svg;
  const el = $("gsvg");
  el.style.cursor = "pointer";
  el.addEventListener("click", ev => {
    const r = el.getBoundingClientRect();
    const t = ((ev.clientX - r.left) / r.width * W - left) / (W-left-6) * T_END;
    let best = 0; for (const e of EV) { if (e.t <= t) best = e.seq; else break; }
    pause(); go(best);
  });
  window._gx = x;
})();

function render() {
  const e = idx >= 0 ? EV[idx] : null;
  const threads = e ? e.threads : Object.fromEntries(ACTORS.map(a=>[a,"Idle"]));
  const m = e ? e.monitor : {active_readers:0,reader_ids:[],writer_active:false,active_writer:null,ok_to_read_queue:[],ok_to_write_queue:[],admitted_readers:0,write_epoch:0};
  const r = e ? e.resource : {value:0,checksum:0,last_writer:null};
  for (const a of ACTORS) {
    const d = $("a-" + a), st = threads[a];
    d.className = "actor s-" + st + (e && e.actor === a ? " cur" : "");
    d.querySelector(".st").textContent = st;
    let info = "";
    if (st === "Waiting") info = a.startsWith("R") ? "on ok_to_read" : "on ok_to_write";
    if (st === "Reading" || st === "Writing") info = "in critical section";
    d.querySelector(".info").textContent = info;
  }
  $("resval").textContent = r.value;
  const res = $("res");
  if (m.writer_active) { res.className = "resource writing"; $("resstat").textContent = `being WRITTEN by ${m.active_writer} (exclusive)`; }
  else if (m.active_readers) { res.className = "resource reading"; $("resstat").textContent = `being READ by ${m.reader_ids.join(", ")}`; }
  else { res.className = "resource"; $("resstat").textContent = "free" + (r.last_writer ? ` (last written by ${r.last_writer})` : ""); }
  $("ar").textContent = m.active_readers;
  $("wa").textContent = m.writer_active ? m.active_writer : "no";
  $("wr").textContent = m.ok_to_read_queue.length;
  $("ww").textContent = m.ok_to_write_queue.length;
  $("qr").innerHTML = chips(m.ok_to_read_queue, "r");
  $("qw").innerHTML = chips(m.ok_to_write_queue, "w");
  $("adm").textContent = `admitted readers pending: ${m.admitted_readers} | write epoch: ${m.write_epoch}`;
  $("cur").innerHTML = e ? `#${e.seq} &nbsp; ${e.t.toFixed(2)} ms &nbsp; <b>${e.actor}</b> <span class="k k-${e.kind}">${e.kind}</span> ${esc(e.msg)}` : "Press Play or Step to replay the recorded execution.";
  $("prog").value = idx + 1;
  $("clock").textContent = `event ${idx+1}/${EV.length}` + (e ? ` @ ${e.t.toFixed(1)} ms` : "");
  const cx = window._gx(e ? e.t : 0);
  const c = $("cursor"); c.setAttribute("x1", cx); c.setAttribute("x2", cx);
}
function go(n) {
  n = Math.max(-1, Math.min(EV.length - 1, n));
  // show/hide log rows incrementally
  if (n > idx) for (let i = idx + 1; i <= n; i++) rows[i].style.display = "";
  else for (let i = idx; i > n; i--) { rows[i].style.display = "none"; }
  if (idx >= 0 && rows[idx]) rows[idx].classList.remove("cur");
  idx = n;
  if (idx >= 0) { rows[idx].classList.add("cur"); const L = $("log"); L.scrollTop = rows[idx].offsetTop - L.offsetTop - L.clientHeight + 40; }
  render();
}
function play() {
  if (idx >= EV.length - 1) go(-1);
  $("play").innerHTML = "&#10074;&#10074; Pause";
  const tick = () => { if (idx >= EV.length - 1) { pause(); return; } go(idx + 1); timer = setTimeout(tick, 1000 / +$("speed").value); };
  timer = setTimeout(tick, 0);
}
function pause() { clearTimeout(timer); timer = null; $("play").innerHTML = "&#9654; Play"; }
$("play").onclick = () => timer ? pause() : play();
$("step").onclick = () => { pause(); go(idx + 1); };
$("back").onclick = () => { pause(); go(idx - 1); };
$("reset").onclick = () => { pause(); go(-1); };
$("prog").oninput = ev => { pause(); go(+ev.target.value - 1); };
$("speed").oninput = () => $("speedv").textContent = $("speed").value;
$("speedv").textContent = $("speed").value;
document.addEventListener("keydown", ev => {
  if (ev.key === " ") { ev.preventDefault(); timer ? pause() : play(); }
  else if (ev.key === "ArrowRight") { pause(); go(idx + 1); }
  else if (ev.key === "ArrowLeft") { pause(); go(idx - 1); }
});
render();
</script>
</body>
</html>
"""


def generate_html(path, trace):
    """Embed the recorded trace as JSON into the self-contained HTML player."""
    data = json.dumps(trace, separators=(",", ":"))
    data = data.replace("</", "<\\/")        # keep </script> out of the JSON
    html = HTML_TEMPLATE.replace("__TRACE_JSON__", data)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


# --------------------------------------------------------------------------- #
#  Main                                                                       #
# --------------------------------------------------------------------------- #

DEFAULT_OUTPUT = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..",
    "generated_html", "readers_writers_monitor.html"))


def parse_args():
    p = argparse.ArgumentParser(
        description="Readers-Writers problem solved with a monitor "
                    "(Lock + Condition variables); writes an HTML replay.")
    p.add_argument("--readers", type=int, default=5, help="number of reader threads (default 5)")
    p.add_argument("--writers", type=int, default=2, help="number of writer threads (default 2)")
    p.add_argument("--iterations", type=int, default=3, help="read/write rounds per thread (default 3)")
    p.add_argument("--seed", type=int, default=None, help="random seed for the delays (default: random)")
    p.add_argument("--max-delay", type=float, default=0.3,
                   help="maximum random delay in seconds for idle/read/write (default 0.3)")
    p.add_argument("--output", default=DEFAULT_OUTPUT, help="path of the generated HTML file")
    p.add_argument("--quiet", action="store_true", help="do not print every event to the console")
    a = p.parse_args()
    if a.readers < 1 or a.writers < 1 or a.iterations < 1:
        p.error("--readers, --writers and --iterations must be >= 1")
    if a.max_delay <= 0:
        p.error("--max-delay must be > 0")
    return a


def main():
    args = parse_args()
    seed = args.seed if args.seed is not None else random.randrange(1_000_000)
    readers = [f"R{i}" for i in range(1, args.readers + 1)]
    writers = [f"W{i}" for i in range(1, args.writers + 1)]

    rec = EventRecorder(readers + writers, verbose=not args.quiet)
    monitor = ReadWriteMonitor(rec)
    db = SharedDatabase(rec)
    rec.monitor, rec.database = monitor, db

    print(f"Readers-Writers (monitor): {args.readers} readers, {args.writers} writers, "
          f"{args.iterations} iterations, seed={seed}, max_delay={args.max_delay}s\n")

    # Each thread gets its own RNG derived from the seed (random.Random is not
    # shared, so the delays are reproducible per thread for a given seed;
    # the interleaving itself is still decided by the real OS scheduler).
    threads = []
    for k, rid in enumerate(readers):
        threads.append(threading.Thread(target=reader_thread, name=rid, args=(
            rid, monitor, db, rec, args.iterations, args.max_delay,
            random.Random(seed * 1000 + k))))
    for k, wid in enumerate(writers):
        threads.append(threading.Thread(target=writer_thread, name=wid, args=(
            wid, monitor, db, rec, args.iterations, args.max_delay,
            random.Random(seed * 1000 + 500 + k))))
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    duration_ms = (time.perf_counter() - start) * 1000

    events = rec.events
    expected_reads = args.readers * args.iterations
    expected_writes = args.writers * args.iterations
    summary = {
        "reads": db.read_count,
        "writes": db.write_count,
        "expected_reads": expected_reads,
        "expected_writes": expected_writes,
        "final_value": db.value,
        "max_concurrent_readers": db.max_concurrent_readers,
        "violations": len(db.violations),
        "torn_reads": db.torn_reads,
        "duration_ms": duration_ms,
        "event_count": len(events),
    }
    trace = {
        "program": os.path.basename(__file__),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "config": {
            "readers": args.readers, "writers": args.writers,
            "iterations": args.iterations, "seed": seed,
            "max_delay": args.max_delay, "actors": readers + writers,
            "policy": "writer preference for new readers; waiting readers "
                      "admitted as a batch after each write (starvation free)",
        },
        "summary": summary,
        "events": events,
    }
    generate_html(args.output, trace)

    print("\n=== Summary ===")
    print(f"reads completed        : {db.read_count}/{expected_reads}")
    print(f"writes completed       : {db.write_count}/{expected_writes}")
    print(f"final shared value     : {db.value}")
    print(f"max concurrent readers : {db.max_concurrent_readers}")
    print(f"invariant violations   : {len(db.violations)}")
    print(f"events recorded        : {len(events)}")
    print(f"HTML simulation        : {args.output}")
    if db.violations or db.read_count != expected_reads or db.write_count != expected_writes:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
