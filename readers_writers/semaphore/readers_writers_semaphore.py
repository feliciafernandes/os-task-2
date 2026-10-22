#!/usr/bin/env python3
"""
Problem 1 - Readers-Writers using SEMAPHORES (Python 3, standard library only).

Pipeline (as required by the assignment):
    Execute  -> real reader/writer threads run concurrently
    Synchronize -> threading.Semaphore objects control access to the shared resource
    Capture state -> every state change is recorded as a timestamped event
    Generate HTML -> the recorded events are embedded as JSON in a self-contained page
    View -> open the generated HTML file in any browser

Algorithm: the "fair" (starvation-free / FIFO-ish) readers-writers solution.
Three semaphores are used:

    service_queue = Semaphore(1)  # turnstile: every arriving thread (reader OR writer)
                                  # must pass through it, in arrival order. A waiting
                                  # writer therefore blocks readers that arrive after it,
                                  # so writers cannot be starved by a stream of readers.
    read_count_mutex = Semaphore(1)  # protects the shared counter `read_count`
    resource = Semaphore(1)       # exclusive access to the shared resource; held by a
                                  # writer, or by the *group* of active readers
                                  # (first reader in acquires it, last reader out releases it)

The program prints every event to the console and checks the mutual-exclusion
invariants at runtime (no writer concurrent with any reader or other writer; readers
may overlap). The number of violations is reported at the end and must be 0.

Run:  python3 readers_writers_semaphore.py --readers 4 --writers 2 --iterations 3
"""

import argparse
import json
import os
import random
import threading
import time

# ---------------------------------------------------------------------------
# SHARED RESOURCE
# ---------------------------------------------------------------------------
class SharedDocument:
    """The shared resource that readers read and writers modify.

    It is deliberately NOT thread-safe on its own: all protection comes from the
    semaphores in ReadersWriters. `value` is the data; `version` counts writes.
    """

    def __init__(self):
        self.value = 0
        self.version = 0
        self.last_writer = None


# ---------------------------------------------------------------------------
# EVENT RECORDER (instrumentation only - NOT part of the synchronization)
# ---------------------------------------------------------------------------
class EventRecorder:
    """Thread-safe recorder of timestamped state-change events.

    It keeps its own lock (`_lock`) purely so that the log and the observed
    snapshot stay consistent. This lock never decides who may access the
    shared resource; that is done exclusively by the semaphores.
    """

    def __init__(self, reader_ids, writer_ids, quiet=False):
        self._lock = threading.Lock()
        self._t0 = time.perf_counter()
        self.quiet = quiet
        self.events = []
        # Observed state of every thread: Idle / Waiting / Reading / Writing / Done
        self.states = {tid: "Idle" for tid in reader_ids + writer_ids}
        self.active_readers = 0
        self.writer_active = None      # id of the writer in the CS, or None
        self.waiting_readers = []      # FIFO order of arrival
        self.waiting_writers = []
        self.waiting_order = []        # readers and writers together, in arrival order
        self.value = 0
        self.version = 0
        self.violations = 0
        self.max_concurrent_readers = 0

    def record(self, actor, kind, msg, **changes):
        """Apply `changes` to the observed state and append an event with a snapshot."""
        with self._lock:
            t_ms = (time.perf_counter() - self._t0) * 1000.0
            if "state" in changes:
                self.states[actor] = changes["state"]
            for key in ("value", "version"):
                if key in changes:
                    setattr(self, key, changes[key])
            event = {
                "seq": len(self.events),
                "t": round(t_ms, 2),
                "actor": actor,
                "kind": kind,
                "msg": msg,
                "snap": {
                    "states": dict(self.states),
                    "active_readers": self.active_readers,
                    "writer_active": self.writer_active,
                    "waiting_readers": list(self.waiting_readers),
                    "waiting_writers": list(self.waiting_writers),
                    "waiting_order": list(self.waiting_order),
                    "value": self.value,
                    "version": self.version,
                    "violations": self.violations,
                },
            }
            self.events.append(event)
            if not self.quiet:
                print(f"[{t_ms:9.2f} ms] {actor:<3} {kind:<12} {msg}", flush=True)

    # The helpers below update the observed counters atomically together with the
    # event, and check the mutual-exclusion invariants at the moment of entry.

    def arrive(self, actor, is_writer):
        with self._lock:
            (self.waiting_writers if is_writer else self.waiting_readers).append(actor)
            self.waiting_order.append(actor)
        self.record(actor, "ARRIVE", "arrives and waits for access", state="Waiting")

    def enter_read(self, actor):
        with self._lock:
            self.waiting_readers.remove(actor)
            self.waiting_order.remove(actor)
            # INVARIANT: a reader may never be in the CS together with a writer.
            if self.writer_active is not None:
                self.violations += 1
            self.active_readers += 1
            self.max_concurrent_readers = max(self.max_concurrent_readers, self.active_readers)
        self.record(actor, "ENTER_CS", f"starts READING (active readers={self.active_readers})",
                    state="Reading")

    def exit_read(self, actor):
        with self._lock:
            self.active_readers -= 1
        self.record(actor, "EXIT_CS", f"finishes reading (active readers={self.active_readers})",
                    state="Idle")

    def enter_write(self, actor):
        with self._lock:
            self.waiting_writers.remove(actor)
            self.waiting_order.remove(actor)
            # INVARIANT: a writer must be completely alone in the CS.
            if self.active_readers != 0 or self.writer_active is not None:
                self.violations += 1
            self.writer_active = actor
        self.record(actor, "ENTER_CS", "starts WRITING (exclusive access)", state="Writing")

    def exit_write(self, actor):
        with self._lock:
            self.writer_active = None
        self.record(actor, "EXIT_CS", "finishes writing, releases resource", state="Idle")

    def flag_violation(self, actor, why):
        with self._lock:
            self.violations += 1
        self.record(actor, "VIOLATION", why)


# ---------------------------------------------------------------------------
# SYNCHRONIZATION: fair readers-writers with semaphores
# ---------------------------------------------------------------------------
class ReadersWriters:
    def __init__(self, doc, recorder, max_delay):
        self.doc = doc
        self.rec = recorder
        self.max_delay = max_delay
        self.read_count = 0                           # shared counter, guarded by read_count_mutex
        self.service_queue = threading.Semaphore(1)   # fairness turnstile (FIFO order of arrival)
        self.read_count_mutex = threading.Semaphore(1)  # protects read_count
        self.resource = threading.Semaphore(1)        # exclusive access to the SharedDocument

    def _delay(self, scale=1.0):
        # Random delays only make interleavings visible; they never replace a semaphore.
        time.sleep(random.uniform(0.0, self.max_delay) * scale)

    def read(self, rid):
        self.rec.arrive(rid, is_writer=False)

        # ---- entry section ----
        self.service_queue.acquire()       # wait for our turn in arrival order
        self.read_count_mutex.acquire()
        self.read_count += 1
        if self.read_count == 1:
            self.resource.acquire()        # first reader locks writers out
        self.read_count_mutex.release()
        self.service_queue.release()       # let the next arrival (reader or writer) queue up

        # ---- CRITICAL SECTION (shared read access) ----
        self.rec.enter_read(rid)
        value, version = self.doc.value, self.doc.version
        self.rec.record(rid, "READ", f"reads value={value} (version {version}, by {self.doc.last_writer or '-'})")
        self._delay()                      # simulate reading time
        if (self.doc.value, self.doc.version) != (value, version):
            # A writer modified the data while we were reading -> broken exclusion.
            self.rec.flag_violation(rid, "data changed during read!")
        self.rec.exit_read(rid)
        # ---- end of critical section ----

        # ---- exit section ----
        self.read_count_mutex.acquire()
        self.read_count -= 1
        if self.read_count == 0:
            self.resource.release()        # last reader lets writers in
        self.read_count_mutex.release()

    def write(self, wid):
        self.rec.arrive(wid, is_writer=True)

        # ---- entry section ----
        self.service_queue.acquire()       # take our place in the arrival queue
        self.resource.acquire()            # wait until the resource is free
        self.service_queue.release()       # later arrivals may now queue behind us

        # ---- CRITICAL SECTION (exclusive write access) ----
        self.rec.enter_write(wid)
        old = self.doc.value               # read-modify-write: would race without exclusion
        self._delay(0.5)
        new = old + random.randint(1, 9)
        self.doc.value = new
        self.doc.version += 1
        self.doc.last_writer = wid
        self.rec.record(wid, "WRITE", f"writes value {old} -> {new} (version {self.doc.version})",
                        value=new, version=self.doc.version)
        self._delay(0.5)
        self.rec.exit_write(wid)
        # ---- end of critical section ----

        # ---- exit section ----
        self.resource.release()


# ---------------------------------------------------------------------------
# THREAD BODIES
# ---------------------------------------------------------------------------
def reader_thread(rw, rid, iterations, max_delay, rng_seed):
    rng = random.Random(rng_seed)
    for _ in range(iterations):
        time.sleep(rng.uniform(0.0, max_delay))    # do something else before reading
        rw.read(rid)
    rw.rec.record(rid, "DONE", "finished all iterations", state="Done")


def writer_thread(rw, wid, iterations, max_delay, rng_seed):
    rng = random.Random(rng_seed)
    for _ in range(iterations):
        time.sleep(rng.uniform(0.0, max_delay))    # prepare data before writing
        rw.write(wid)
    rw.rec.record(wid, "DONE", "finished all iterations", state="Done")


# ---------------------------------------------------------------------------
# HTML GENERATION
# ---------------------------------------------------------------------------
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Readers-Writers (Semaphores)</title>
<style>
  :root { --bg:#f5f6f8; --card:#fff; --ink:#1d2330; --muted:#677084; --line:#dde1e8;
          --idle:#9aa3b2; --wait:#e0a800; --read:#2f80ed; --write:#d64545; --done:#3c9d5d; }
  * { box-sizing:border-box; }
  body { margin:0; font:14px/1.45 system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
         background:var(--bg); color:var(--ink); }
  header { padding:16px 20px; background:#1d2330; color:#fff; }
  header h1 { margin:0; font-size:20px; }
  header p { margin:4px 0 0; color:#c4cad6; font-size:13px; }
  main { max-width:1200px; margin:0 auto; padding:16px; display:grid; gap:16px;
         grid-template-columns: 1fr 1fr; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:8px; padding:14px; }
  .card h2 { margin:0 0 10px; font-size:15px; }
  .full { grid-column: 1 / -1; }
  .controls { display:flex; flex-wrap:wrap; gap:8px; align-items:center; }
  button { font:inherit; padding:6px 14px; border:1px solid var(--line); border-radius:6px;
           background:#fff; cursor:pointer; }
  button:hover { background:#eef1f6; }
  button.primary { background:#2f80ed; color:#fff; border-color:#2f80ed; }
  input[type=range] { width:160px; }
  .progress { flex:1; min-width:180px; color:var(--muted); }
  .threads { display:grid; grid-template-columns:repeat(auto-fill, minmax(120px,1fr)); gap:8px; }
  .thread { border:2px solid var(--idle); border-radius:8px; padding:8px; text-align:center;
            transition:background .2s, border-color .2s; }
  .thread .id { font-weight:700; font-size:16px; }
  .thread .st { font-size:12px; text-transform:uppercase; letter-spacing:.05em; }
  .thread.Idle { border-color:var(--idle); }
  .thread.Waiting { border-color:var(--wait); background:#fff7dd; }
  .thread.Reading { border-color:var(--read); background:#e3eefc; }
  .thread.Writing { border-color:var(--write); background:#fbe3e3; }
  .thread.Done { border-color:var(--done); background:#e4f4e9; }
  .thread.flash { box-shadow:0 0 0 3px #1d233055; }
  .resource { border:3px solid var(--idle); border-radius:10px; padding:14px; text-align:center; }
  .resource.reading { border-color:var(--read); background:#e3eefc; }
  .resource.writing { border-color:var(--write); background:#fbe3e3; }
  .resource .big { font-size:34px; font-weight:700; }
  .stats { display:grid; grid-template-columns:repeat(2,1fr); gap:8px; margin-top:10px; }
  .stat { background:#f5f6f8; border-radius:6px; padding:8px; }
  .stat b { display:block; font-size:18px; }
  .stat span { color:var(--muted); font-size:12px; }
  .queue { min-height:28px; display:flex; flex-wrap:wrap; gap:4px; }
  .chip { padding:2px 8px; border-radius:12px; font-size:12px; font-weight:600; color:#fff; }
  .chip.r { background:var(--read); } .chip.w { background:var(--write); }
  .empty { color:var(--muted); font-size:12px; }
  #log { height:300px; overflow-y:auto; font:12px/1.5 ui-monospace, Menlo, Consolas, monospace;
         background:#fafbfc; border:1px solid var(--line); border-radius:6px; padding:6px; }
  #log div { padding:1px 4px; border-radius:3px; }
  #log div.cur { background:#fff3b0; }
  #log .ENTER_CS { color:#1a5fb4; } #log .EXIT_CS { color:#555; }
  #log .WRITE { color:var(--write); font-weight:600; } #log .READ { color:var(--read); }
  #log .ARRIVE { color:#a07800; } #log .DONE { color:var(--done); }
  #log .VIOLATION { color:#fff; background:#d64545; }
  .legend { display:flex; gap:12px; flex-wrap:wrap; font-size:12px; color:var(--muted); }
  .legend i { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:4px; }
  .ok { color:var(--done); font-weight:700; } .bad { color:var(--write); font-weight:700; }
  @media (max-width:760px) { main { grid-template-columns:1fr; padding:12px; } }
</style>
</head>
<body>
<header>
  <h1>Readers-Writers Problem &mdash; Semaphore solution (fair, service-queue variant)</h1>
  <p id="meta"></p>
</header>
<main>
  <section class="card full">
    <div class="controls">
      <button id="play" class="primary">Play</button>
      <button id="step">Step</button>
      <button id="reset">Reset</button>
      <label>Speed <input id="speed" type="range" min="0.25" max="8" step="0.25" value="1">
        <span id="speedv">1x</span></label>
      <span class="progress" id="progress"></span>
    </div>
    <div class="legend" style="margin-top:8px">
      <span><i style="background:var(--idle)"></i>Idle</span>
      <span><i style="background:var(--wait)"></i>Waiting</span>
      <span><i style="background:var(--read)"></i>Reading</span>
      <span><i style="background:var(--write)"></i>Writing</span>
      <span><i style="background:var(--done)"></i>Done</span>
    </div>
  </section>

  <section class="card">
    <h2>Readers</h2>
    <div class="threads" id="readers"></div>
    <h2 style="margin-top:14px">Writers</h2>
    <div class="threads" id="writers"></div>
  </section>

  <section class="card">
    <h2>Shared resource (critical section)</h2>
    <div class="resource" id="resource">
      <div class="big" id="value">0</div>
      <div id="rstatus">FREE</div>
      <div class="empty" id="version"></div>
    </div>
    <div class="stats">
      <div class="stat"><b id="ar">0</b><span>active readers</span></div>
      <div class="stat"><b id="wa">no</b><span>writer active</span></div>
      <div class="stat"><b id="wr">0</b><span>waiting readers</span></div>
      <div class="stat"><b id="ww">0</b><span>waiting writers</span></div>
    </div>
    <h2 style="margin-top:12px">Waiting (arrival order)</h2>
    <div class="queue" id="queue"></div>
    <p style="margin:10px 0 0">Invariant violations: <span id="viol"></span></p>
  </section>

  <section class="card full">
    <h2>Event timeline</h2>
    <div id="log"></div>
  </section>
</main>

<script>
// Events captured during the real execution of the Python program.
const DATA = __DATA__;

const ev = DATA.events;
let idx = -1;          // index of the last applied event
let playing = false;
let timer = null;
const $ = id => document.getElementById(id);

$("meta").textContent =
  `Generated ${DATA.meta.generated} | readers=${DATA.meta.readers}, writers=${DATA.meta.writers}, ` +
  `iterations=${DATA.meta.iterations}, max-delay=${DATA.meta.max_delay}s, seed=${DATA.meta.seed} | ` +
  `${ev.length} events, run took ${DATA.meta.duration_ms} ms, violations=${DATA.meta.violations}`;

function buildThreads() {
  for (const [box, ids] of [["readers", DATA.meta.reader_ids], ["writers", DATA.meta.writer_ids]]) {
    $(box).innerHTML = ids.map(id =>
      `<div class="thread Idle" id="th-${id}"><div class="id">${id}</div><div class="st">Idle</div></div>`).join("");
  }
  $("log").innerHTML = ev.map((e, i) =>
    `<div id="ev-${i}" class="${e.kind}" style="display:none">` +
    `[${e.t.toFixed(2).padStart(9)} ms] ${e.actor.padEnd(3)} ${e.kind.padEnd(10)} ${escapeHtml(e.msg)}</div>`).join("");
}

function escapeHtml(s) {
  return s.replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
}

const INITIAL = {
  states: Object.fromEntries([...DATA.meta.reader_ids, ...DATA.meta.writer_ids].map(id => [id, "Idle"])),
  active_readers: 0, writer_active: null, waiting_readers: [], waiting_writers: [], waiting_order: [],
  value: 0, version: 0, violations: 0
};

function render() {
  const s = idx >= 0 ? ev[idx].snap : INITIAL;
  const actor = idx >= 0 ? ev[idx].actor : null;
  for (const [id, st] of Object.entries(s.states)) {
    const el = $("th-" + id);
    el.className = "thread " + st + (id === actor ? " flash" : "");
    el.querySelector(".st").textContent = st;
  }
  const res = $("resource");
  if (s.writer_active) { res.className = "resource writing"; $("rstatus").textContent = `WRITING by ${s.writer_active}`; }
  else if (s.active_readers > 0) {
    const rs = Object.entries(s.states).filter(([, v]) => v === "Reading").map(([k]) => k);
    res.className = "resource reading"; $("rstatus").textContent = `READING by ${rs.join(", ")}`;
  } else { res.className = "resource"; $("rstatus").textContent = "FREE"; }
  $("value").textContent = s.value;
  $("version").textContent = `version ${s.version}`;
  $("ar").textContent = s.active_readers;
  $("wa").textContent = s.writer_active || "no";
  $("wr").textContent = s.waiting_readers.length;
  $("ww").textContent = s.waiting_writers.length;
  const q = s.waiting_order.map(id => [id, id[0] === "W" ? "w" : "r"]);
  $("queue").innerHTML = q.length ? q.map(([id, c]) => `<span class="chip ${c}">${id}</span>`).join("")
                                  : `<span class="empty">nobody waiting</span>`;
  $("viol").innerHTML = s.violations === 0 ? `<span class="ok">0</span>` : `<span class="bad">${s.violations}</span>`;
  $("progress").textContent = `event ${idx + 1} / ${ev.length}` + (idx >= 0 ? `  (t = ${ev[idx].t.toFixed(2)} ms)` : "");
  document.querySelectorAll("#log div.cur").forEach(d => d.classList.remove("cur"));
  if (idx >= 0) { const d = $("ev-" + idx); d.classList.add("cur"); d.scrollIntoView({block: "nearest"}); }
}

function step() {
  if (idx >= ev.length - 1) { pause(); return false; }
  idx++;
  $("ev-" + idx).style.display = "";
  render();
  return true;
}

function schedule() {
  if (!playing) return;
  if (idx >= ev.length - 1) { pause(); return; }
  // Replay with the real time gaps between events (clamped so the replay stays watchable).
  const gap = idx >= 0 ? ev[idx + 1].t - ev[idx].t : 0;
  const ms = Math.min(Math.max(gap * 2, 120), 1200) / parseFloat($("speed").value);
  timer = setTimeout(() => { step(); schedule(); }, ms);
}

function play() { if (idx >= ev.length - 1) reset(); playing = true; $("play").textContent = "Pause"; schedule(); }
function pause() { playing = false; clearTimeout(timer); $("play").textContent = "Play"; }
function reset() {
  pause(); idx = -1;
  document.querySelectorAll("#log div").forEach(d => { d.style.display = "none"; d.classList.remove("cur"); });
  render();
}

$("play").onclick = () => playing ? pause() : play();
$("step").onclick = () => { pause(); step(); };
$("reset").onclick = reset;
$("speed").oninput = () => { $("speedv").textContent = $("speed").value + "x"; };

buildThreads();
render();
</script>
</body>
</html>
"""


def generate_html(path, events, meta):
    data = json.dumps({"meta": meta, "events": events}, separators=(",", ":"))
    data = data.replace("</", "<\\/")      # keep "</script>" from ending the script block
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(HTML_TEMPLATE.replace("__DATA__", data))


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    default_out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..",
                               "generated_html", "readers_writers_semaphore.html")
    p = argparse.ArgumentParser(description="Readers-Writers with semaphores + HTML simulation")
    p.add_argument("--readers", type=int, default=4, help="number of reader threads (default 4)")
    p.add_argument("--writers", type=int, default=2, help="number of writer threads (default 2)")
    p.add_argument("--iterations", type=int, default=3, help="reads/writes per thread (default 3)")
    p.add_argument("--seed", type=int, default=None, help="random seed (default: random)")
    p.add_argument("--max-delay", type=float, default=0.3, help="max random delay in seconds (default 0.3)")
    p.add_argument("--output", default=os.path.normpath(default_out), help="path of generated HTML file")
    p.add_argument("--quiet", action="store_true", help="do not print events to the console")
    args = p.parse_args()
    if args.readers < 1 or args.writers < 1 or args.iterations < 1 or args.max_delay < 0:
        p.error("--readers, --writers, --iterations must be >= 1 and --max-delay >= 0")

    seed = args.seed if args.seed is not None else random.randrange(1_000_000)
    random.seed(seed)
    reader_ids = [f"R{i}" for i in range(1, args.readers + 1)]
    writer_ids = [f"W{i}" for i in range(1, args.writers + 1)]

    doc = SharedDocument()
    rec = EventRecorder(reader_ids, writer_ids, quiet=args.quiet)
    rw = ReadersWriters(doc, rec, args.max_delay)

    print(f"Readers-Writers (semaphores): {args.readers} readers, {args.writers} writers, "
          f"{args.iterations} iterations, max delay {args.max_delay}s, seed {seed}\n")

    threads = [threading.Thread(target=reader_thread, name=rid,
                                args=(rw, rid, args.iterations, args.max_delay, seed * 1000 + i))
               for i, rid in enumerate(reader_ids)]
    threads += [threading.Thread(target=writer_thread, name=wid,
                                 args=(rw, wid, args.iterations, args.max_delay, seed * 1000 + 500 + i))
                for i, wid in enumerate(writer_ids)]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    duration_ms = round((time.perf_counter() - start) * 1000, 1)

    meta = {
        "readers": args.readers, "writers": args.writers, "iterations": args.iterations,
        "max_delay": args.max_delay, "seed": seed, "reader_ids": reader_ids, "writer_ids": writer_ids,
        "violations": rec.violations, "duration_ms": duration_ms,
        "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    generate_html(args.output, rec.events, meta)

    print("\n--- Summary ---")
    print(f"Events recorded         : {len(rec.events)}")
    print(f"Final value / version   : {doc.value} / {doc.version} "
          f"(expected version {args.writers * args.iterations})")
    print(f"Max concurrent readers  : {rec.max_concurrent_readers}")
    print(f"Invariant violations    : {rec.violations}")
    print(f"HTML simulation written : {os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
