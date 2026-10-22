# Problem 1 — Readers-Writers using Semaphores

`readers_writers_semaphore.py` uses Python 3 and only the standard library (`threading`). It starts real reader and writer threads and uses `threading.Semaphore` to synchronize them. While the threads run, it records every state change with a timestamp. It then writes a self-contained HTML replay of that run to `../../generated_html/readers_writers_semaphore.html`.

## Algorithm: fair (service-queue) readers-writers

| Semaphore | Initial value | Purpose |
|---|---|---|
| `service_queue` | 1 | Turnstile that every arriving thread, reader or writer, must pass in arrival order. A waiting writer holds it, so readers that arrive *after* it cannot overtake it. |
| `read_count_mutex` | 1 | Protects the shared counter `read_count`. |
| `resource` | 1 | Exclusive access to the shared resource. A writer holds it, or the active readers hold it as a group: the first reader in acquires it and the last reader out releases it. |

```
Reader                                  Writer
  service_queue.acquire()                 service_queue.acquire()
  read_count_mutex.acquire()              resource.acquire()
  read_count += 1                         service_queue.release()
  if read_count == 1: resource.acquire()    --- CRITICAL SECTION: write ---
  read_count_mutex.release()              resource.release()
  service_queue.release()
    --- CRITICAL SECTION: read ---
  read_count_mutex.acquire()
  read_count -= 1
  if read_count == 0: resource.release()
  read_count_mutex.release()
```

**Shared resource:** `SharedDocument` (`value`, `version`, `last_writer`). It has no lock of its own; only the semaphores protect it.
**Critical sections:** the blocks marked `CRITICAL SECTION` in `ReadersWriters.read()` and `ReadersWriters.write()`. Readers may be inside together. A writer is always alone. The writer does a read-modify-write (`old → old + k`) with a delay in the middle, so it would race without exclusion.

## Starvation

- **Classic readers-preference solution** (only `mutex` + `rw_mutex`): new readers keep joining while any reader is active, so a steady stream of readers can starve writers forever.
- **Writers-preference solution:** the opposite problem, because readers can starve.
- **This solution (service queue):** once a writer is waiting at `resource`, it holds `service_queue`, so later readers queue *behind* it. Readers that arrived *before* the writer finish, then the writer goes in. Neither side can starve. The HTML "Waiting (arrival order)" panel shows this: readers that arrive after a writer stay behind it.
- Caveat: Python's `threading.Semaphore` does not promise strict FIFO wake-up. In practice it wakes waiters roughly in order, so fairness is "no starvation" rather than strict FIFO.

## Runtime checks

The event recorder checks two invariants each time a thread enters the critical section:
- a reader never enters while a writer is active
- a writer only enters when there are 0 active readers and no other writer

Each reader also checks that the value did not change while it was reading. The program prints the number of violations at the end, and it must be 0. The recorder's own lock exists only so the event log stays consistent. It never decides who gets access to the resource.

Random delays only change the interleaving; they never replace a semaphore. With the semaphores replaced by no-ops, the checker reported 20 violations (seed 3).

## How to run

```bash
python3 readers_writers_semaphore.py
```

```bash
python3 readers_writers_semaphore.py --readers 6 --writers 3 --iterations 4 --max-delay 0.5 --seed 7
```

| Option | Default | Meaning |
|---|---|---|
| `--readers` | 4 | number of reader threads |
| `--writers` | 2 | number of writer threads |
| `--iterations` | 3 | reads/writes per thread |
| `--max-delay` | 0.3 | maximum random delay (seconds) |
| `--seed` | random | seed for a repeatable run (printed every run) |
| `--output` | `../../generated_html/readers_writers_semaphore.html` | HTML output path |
| `--quiet` | off | do not print events to the console |

Open the generated HTML in a browser. It shows:
- each thread's state (Idle / Waiting / Reading / Writing / Done)
- the shared resource's value and status
- active and waiting counts, and the waiting queue
- the violation count
- a scrolling event timeline

Controls: Play / Pause, Step, Reset and a speed slider.
