# Problem 3 – Dining Philosophers (4 philosophers) with Semaphores

Program: `dining_philosophers_semaphore.py`. It needs only Python 3 and the standard library (`threading`).
Output: `../../generated_html/dining_philosophers_semaphore.html`, a self-contained replay of the recorded run.

## How to run

```bash
cd "os/task 2/dining_philosophers/semaphore"
python3 dining_philosophers_semaphore.py --meals 5 --seed 42
```

Then open `generated_html/dining_philosophers_semaphore.html` in any browser. It works offline because all CSS, JS and data are inline.

| Option | Default | Meaning |
|---|---|---|
| `--meals N` | 5 | meals each philosopher must eat |
| `--seed S` | random (printed) | seed for the random think/eat delays |
| `--max-think T` | 0.3 | max thinking time (s), uniform random in [0, T] |
| `--max-eat T` | 0.2 | max eating time (s) |
| `--grab-delay T` | 0.05 | max pause between picking up the 1st and 2nd fork (s); a wider window makes contention more likely |
| `--timeout T` | 60 | watchdog: report deadlock if threads haven't finished after T s |
| `--output PATH` | `../../generated_html/dining_philosophers_semaphore.html` | HTML file to write |
| `--quiet` | off | don't print every event to the console |

Example for a viva where the delays must change: `--max-think 1.0 --max-eat 0.8 --grab-delay 0.2`.
The exit code is 0 when the run ends with no deadlock and no invariant violations, and 1 otherwise.

## Pipeline (Execute → Synchronize → Capture state → Generate HTML)

1. **Execute**: four `threading.Thread`s run `philosopher(i)` concurrently.
2. **Synchronize**: forks are semaphores, and forks are acquired in a fixed global order (see below).
3. **Capture state**: a thread-safe `EventRecorder` records a timestamped event at every state change, with a full
   snapshot of all philosopher states, the fork owners, which fork each philosopher is blocked on, and meal counts.
4. **Generate HTML**: after the threads are joined, the events are embedded as JSON in the HTML file.
   The page draws an SVG round table: philosophers are coloured by state, and each fork moves next to its
   owner or shows "free". The page also has meal counters, a "state at current event" table, a clickable
   scrolling timeline, Play/Pause, Step forward/back, Reset, an event scrubber and a speed slider. Playback
   timing follows the real gaps between recorded timestamps.

Nothing is pre-scripted. The interleaving comes from OS thread scheduling and random delays, so two runs differ, even with the same seed.
The seed only fixes each thread's delay sequence, not the scheduling.

## Shared resources and critical section

- **Shared resources:** the 4 forks. Fork `Fi` sits between `P(i-1)` and `Pi`.
  Philosopher `i`'s left fork is `Fi` and its right fork is `F(i+1) mod 4`.
- **Critical section:** the eating phase, between acquiring both forks and releasing them. It is marked
  with `CRITICAL SECTION` comments in `philosopher()`.

```
                 P0
          F1           F0
       P1                 P3
          F2           F3
                 P2
```

## Semaphores and initial values

| Semaphore | Type | Initial value | Meaning |
|---|---|---|---|
| `forks[0..3]` | `threading.Semaphore(1)` (binary) | 1 each | 1 = fork on the table; `acquire()` = pick up (blocks if a neighbour holds it), `release()` = put down |

That is the whole synchronization. `EventRecorder` uses its own `threading.Lock`, but only to keep its
observation mirror and event list consistent. It never decides whether a philosopher may eat.
Random `sleep`s only simulate thinking and eating; they never replace a semaphore.

## Algorithm

```
first, second = min(left, right), max(left, right)    # resource ordering
repeat meals times:
    think()                     # random delay
    hungry
    P(fork[first])              # may block
    P(fork[second])             # may block
    eat()                       # CRITICAL SECTION
    V(fork[second]); V(fork[first])
```

Resulting fork order:

| Philosopher | left / right | acquires |
|---|---|---|
| P0 | F0 / F1 | F0 then F1 |
| P1 | F1 / F2 | F1 then F2 |
| P2 | F2 / F3 | F2 then F3 |
| P3 | F3 / F0 | **F0 then F3** (right fork first, the asymmetric one) |

## Deadlock prevention: which Coffman condition is broken

Deadlock needs all four Coffman conditions:

1. mutual exclusion: a fork has one holder. This is kept on purpose, because it is the point of the problem.
2. hold-and-wait: a philosopher holds one fork while waiting for the other. This is kept.
3. no preemption: forks are not taken away. This is kept.
4. **circular wait: this condition is broken.**

The naive solution has everyone take the left fork first. If all four pick up their left fork at the
same moment, P0 waits for P1, P1 for P2, P2 for P3 and P3 for P0. That cycle is a deadlock.

With resource ordering, every thread acquires forks in strictly increasing fork number.
A waits-for cycle would need some thread that holds a higher-numbered fork while waiting for a
lower-numbered one, and no thread ever does that. In concrete terms, P0 and P3 both start with F0, so
at most one of them can hold F0. If P3 is blocked on F0, it holds nothing and cannot close a cycle.
At least one philosopher can therefore always get both forks, eat, and release them, so the system always makes progress.

**Alternative (not used): the "footman"/room semaphore.** Add `room = Semaphore(N-1) = Semaphore(3)` and
call `room.acquire()` before taking any fork and `room.release()` after putting them down. At most 3
philosophers then compete for 4 forks, so by the pigeonhole principle one of them gets two forks.
This also breaks circular wait, but it needs one extra semaphore and serialises entry to the table.
Resource ordering needs no extra primitive.

## Starvation considerations

- Resource ordering prevents **deadlock**, but in theory it does not guarantee **starvation-freedom**.
  Python's `threading.Semaphore` gives **no FIFO guarantee** about which waiting thread wakes up.
  A philosopher could keep losing a contended fork to a faster neighbour.
- The asymmetry creates an imbalance: P0 and P3 both compete for F0 first, and P3 waits on F0 holding
  nothing. Under heavy contention P0 and P3 contend more, while P2, whose first fork F2 has one
  first-fork competitor, tends to wait less.
- In practice the run is bounded: each philosopher eats exactly `--meals` meals and then leaves.
  Termination of all four threads therefore means nobody starved in that run.
- **Evidence in the output:** the program prints and embeds the per-philosopher meal counts and the
  **maximum hunger wait** (time from "hungry" to "eating"). Similar max-wait values show the run was
  fair. To make starvation impossible, you would add FIFO ticketing or a waiter that serves philosophers
  in order (for example, a monitor with per-philosopher condition variables).

## Runtime invariant checks

These checks run on every recorded event, under the recorder lock:

1. two adjacent philosophers are never `EATING` at the same time;
2. an eating philosopher holds both its left and right fork;
3. a fork is never acquired while the mirror says someone else holds it, and a fork is only released by its holder.

The fork owner is set right **after** `acquire()` returns and cleared right **before** `release()`, so the mirror
can never show two owners unless the semaphores themselves failed. Any violation is printed to stderr,
counted in the summary and embedded in the HTML ("Violations so far").
As a check on the checker, replacing the forks with `Semaphore(2)` (a deliberately broken solution) produces hundreds of violations.

## Verified results

Each run below finished, and every one reported **0 violations** and full meal counts:

- seeds 1, 2, 3, 7, 42, 99, 1234 with the defaults;
- a stress run with `--meals 300 --max-think 0 --max-eat 0 --grab-delay 0.001`, which recorded 12,004 events.

The shipped HTML comes from the `--meals 5 --seed 42` run: 204 events, and moments where two non-adjacent
philosophers eat simultaneously and others are blocked on a held fork.
