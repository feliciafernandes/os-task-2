# Readers-Writers Problem — Monitor-based solution

`readers_writers_monitor.py` — Python 3, standard library only (`threading`, `argparse`, `json`, `random`, `time`).

## How to run

```bash
cd "readers_writers/monitor"
python3 readers_writers_monitor.py                      # 5 readers, 2 writers, 3 rounds, random seed
python3 readers_writers_monitor.py --readers 6 --writers 3 --iterations 4 --seed 42 --max-delay 0.2
```

| Option | Default | Meaning |
|---|---|---|
| `--readers N` | 5 | number of reader threads (`R1..RN`) |
| `--writers N` | 2 | number of writer threads (`W1..WN`) |
| `--iterations N` | 3 | read/write rounds per thread |
| `--seed N` | random | seed for the random delays (printed, and stored in the HTML) |
| `--max-delay S` | 0.3 | maximum random idle/read/write time in seconds |
| `--output PATH` | `../../generated_html/readers_writers_monitor.html` | where the HTML simulation is written |
| `--quiet` | off | do not print every event to the console |

The program prints every event to the console, then a summary (reads/writes completed, final value,
maximum number of concurrent readers, **invariant violations — must be 0**) and writes the HTML file.
Exit code is 1 if any violation occurred or a thread did not finish its rounds.

Open the HTML file in any browser (double-click it). It is fully self-contained: inline CSS/JS, the
recorded trace embedded as JSON, no network access needed.

## Workflow

```
Execute program -> threads synchronize through the monitor -> EventRecorder captures every state change
-> trace embedded as JSON into HTML -> replay in browser
```

The HTML is not a hand-made animation: every frame is an event that a real thread recorded while running,
together with a snapshot of the monitor and the shared resource at that moment. A different seed or
thread count produces a different trace and therefore a different simulation.

## Monitor design

Python has no `monitor` keyword, so `ReadWriteMonitor` builds one from its two standard parts:

| Monitor concept | Implementation |
|---|---|
| Mutual exclusion inside the monitor | one `threading.Lock` (`self._lock`); every entry procedure starts with `with self._lock:` |
| Condition variables | `ok_to_read = Condition(self._lock)`, `ok_to_write = Condition(self._lock)` — both share the monitor lock |
| `wait(c)` | `c.wait()` — atomically releases the monitor lock, sleeps, re-acquires the lock on wake-up |
| `signal(c)` / `broadcast(c)` | `c.notify()` / `c.notify_all()` |
| Entry procedures (only public operations) | `start_read`, `end_read`, `start_write`, `end_write` |
| Private monitor data | `_active_readers`, `_active_writer`, `_waiting_readers`, `_waiting_writers`, `_write_epoch`, `_admitted_readers` |

Because only one thread at a time can hold `_lock`, only one thread at a time is executing monitor code —
that is exactly the guarantee of a Hoare / Brinch-Hansen monitor (or a Java `synchronized` object).

**Mesa semantics.** Python's `Condition` is "signal and continue": a signalled thread does not run
immediately, it must re-acquire the lock first, and by then another thread may have changed the state.
Therefore every wait is written as `while not <can proceed>: cond.wait()` and the condition is always
re-checked after waking (visible as `wakeup` events in the trace, sometimes followed by another `wait`).

### Entry procedures (pseudo-code)

```
start_read(r):                           start_write(w):
  my_epoch = write_epoch                   while writer_active or active_readers > 0
  while writer_active or                         or admitted_readers > 0:
        (writers waiting and                 wait(ok_to_write)
         write_epoch == my_epoch):         writer_active = w
      wait(ok_to_read)
  if I was released by a writer:         end_write(w):
      admitted_readers -= 1                writer_active = none; write_epoch += 1
  active_readers += 1                      if readers waiting:
                                               admitted_readers = #waiting readers
end_read(r):                                   broadcast(ok_to_read)
  active_readers -= 1                      elif writers waiting:
  if active_readers == 0 and                   signal(ok_to_write)
     writers waiting:
      signal(ok_to_write)
```

## Shared resource and critical section

* **Shared resource:** `SharedDatabase` — an integer `value` and a `checksum` copy of it.
* **Critical section:** `SharedDatabase.read()` (between `start_read`/`end_read`) and
  `SharedDatabase.write()` (between `start_write`/`end_write`).

The critical section runs *outside* the monitor lock: the monitor only decides who may enter. This is what
allows several readers to read simultaneously (the HTML regularly shows 3–5 readers in the resource at once),
while a writer is always alone.

A writer updates the record in two steps (`value`, sleep, `checksum`). If a reader were ever let in during a
write it would see `value != checksum` (a *torn read*), so the shared resource itself detects races.

### Runtime invariant checks

`SharedDatabase` keeps its own occupancy counters (with a separate small lock, independent of the monitor)
and records a `VIOLATION` event if

* a reader enters while a writer is inside,
* a writer enters while any reader or another writer is inside,
* a reader observes a torn record.

With the monitor these counts are 0 (verified over many seeds, including 10 readers / 4 writers /
30 rounds with 5 ms delays). As a negative test, replacing the four entry procedures with no-ops produces
dozens of violations — the checker is real, and the monitor is what prevents them.

## Policy, starvation and deadlock

**Policy: writer preference for new readers + batch admission of waiting readers.**

* A *newly arriving* reader waits if a writer is writing **or any writer is waiting**. So a continuous
  stream of readers cannot keep a writer out forever (the classic first readers-writers solution starves
  writers this way).
* When a writer finishes it **broadcasts** `ok_to_read` and admits exactly the readers that were waiting at
  that moment as one batch (`admitted_readers`), *ahead of* the other waiting writers. Writers stay out until
  that batch has entered. So a continuous stream of writers cannot starve readers either (pure writer
  preference would).
* `write_epoch` is how a waiting reader knows "a writer has finished since I arrived, so it is my turn even
  though writers are still waiting". Readers that arrive *after* that write do not get into the batch.

Bounded waiting: a writer waits for at most the readers currently reading plus one batch of readers
(and other writers ahead of it); a waiting reader waits for at most the writer currently writing. (Because of Mesa semantics a newly
arriving writer could try to barge in between the broadcast and the woken readers re-acquiring the lock;
`admitted_readers > 0` makes it wait, so the batch cannot be overtaken.)
Note: among *writers*, `notify()` does not guarantee FIFO order, so a particular writer could in theory be
overtaken repeatedly by other writers; with a finite number of rounds every thread always finishes.

**Deadlock** cannot occur: there is a single lock (no circular wait between locks), the shared resource is
not held while waiting on a condition, and every state change that could enable a waiter is followed by a
signal/broadcast (last reader out → signal a writer; writer out → broadcast readers or signal a writer).

**Race conditions** on the shared data are prevented by the monitor's admission rules; races on the
monitor's own bookkeeping are prevented by the monitor lock.

## What the HTML shows

* Every reader/writer with its current state: Idle, Requesting (inside the monitor), Waiting (on which
  condition), Reading / Writing (in the critical section), Done.
* Shared resource: current value and status (free / read by R… / written by W…).
* Monitor state: active readers, writer active, number of waiting readers/writers, the contents of the
  `ok_to_read` and `ok_to_write` condition queues, pending admitted readers and the write epoch.
* The current event (critical-section entry/exit, wait, wake-up, signal/broadcast, values read/written).
* A Gantt-style timeline of all thread states over the real execution time (click to jump).
* A scrolling event log (click an entry to jump).
* Controls: Play/Pause, Step back/forward, Reset, speed slider, progress scrubber; keyboard: Space, ←, →.

## Known limitations

* The seed makes the random *delays* reproducible, but the exact interleaving is decided by the OS
  scheduler, so two runs with the same seed can differ slightly. The HTML always replays the run that
  generated it.
* CPython's GIL means threads do not execute Python bytecode in parallel, but they are real OS threads that
  are pre-empted and block/wake on the lock and conditions, so the synchronization problem is genuine.
* Events recorded outside the monitor lock (`read`, `write`, `idle`) may carry a
  monitor snapshot that is a few microseconds stale; events recorded inside the monitor are exact.
