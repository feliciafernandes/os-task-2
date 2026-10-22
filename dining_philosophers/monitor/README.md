# Problem 4 — Dining Philosophers (4 philosophers) using a Monitor

Python 3, standard library only (`threading`, `argparse`, `json`, `random`, `time`).

## How to run

From this folder:

```bash
python3 dining_philosophers_monitor.py
```

Then open `../../generated_html/dining_philosophers_monitor.html` in a browser.

| Option | Default | Meaning |
|---|---|---|
| `--meals N` | 3 | meals each philosopher eats before its thread exits |
| `--seed S` | 42 | seed for the random think/eat durations |
| `--max-think SEC` | 0.30 | max thinking time (actual time is uniform in [0.2·max, max]) |
| `--max-eat SEC` | 0.20 | max eating time (same rule) |
| `--fair` / `--no-fair` | fair | turn the anti-starvation ticket rule on/off (`--no-fair` gives the plain textbook monitor) |
| `--output PATH` | `../../generated_html/dining_philosophers_monitor.html` | where the HTML file is written |
| `--quiet` | off | don't print every event to the console |

Examples (e.g. if the viva asks you to change the delays and run again):

```bash
python3 dining_philosophers_monitor.py --meals 5 --max-think 1.0 --max-eat 0.5 --seed 7
python3 dining_philosophers_monitor.py --no-fair --meals 10 --max-think 0.05 --max-eat 0.05
```

The exit code is 0 only if every thread terminated and no invariant was violated.

## Pipeline: Execute → Synchronize → Capture → Generate HTML

1. **Execute**: four real `threading.Thread`s (`Philosopher-0..3`) run concurrently.
   Each one loops: think → `pickup(i)` → eat → `putdown(i)`.
2. **Synchronize**: all shared state is touched only inside the `DiningMonitor`.
3. **Capture state**: whenever a state changes, the monitor calls `EventRecorder.record()`.
   That stores a timestamp (ms since start), the actor, the event kind, a message, the
   thread name, and a full snapshot (display state, monitor `state[]`, owner of each fork,
   meal counts). Every event is printed to the console as well.
4. **Generate HTML**: once all threads are joined, the events are embedded as JSON in a
   single self-contained HTML file (inline CSS/JS, no network access). The page replays the
   run with a round SVG table: philosophers coloured by state, forks drawn next to their
   owner or marked "free", meal counters, a per-philosopher table and a scrolling event
   timeline (click an event to jump to it). Controls: Play/Pause, Step, Back, Reset and a
   speed slider.

Nothing is pre-scripted. The interleaving comes from the OS scheduler and the random
delays, so it changes between runs and between seeds. The delays only decide *how long*
a philosopher thinks or eats. They never stand in for synchronization.

## Shared resource and critical section

* **Shared resource:** the 4 forks. Fork `f` sits between philosophers `f-1` and `f`, and
  philosopher `i` uses forks `i` and `(i+1) % 4`. The monitor also holds the `state[]` array
  that decides who may use the forks, plus an explicit `forks[f] → owner` array that the
  visualisation draws.
* **Critical section:** the bodies of `pickup(i)`, `putdown(i)` and `_test(i)`, meaning
  everything that runs while `DiningMonitor._lock` is held.
* Eating happens **outside** the monitor. The monitor only *reserves* the forks, so a long
  meal does not stop other philosophers from entering the monitor.

## Monitor design (Silberschatz solution)

```
monitor DiningPhilosophers
    state[i] ∈ {THINKING, HUNGRY, EATING}
    condition self[i]

    pickup(i):  state[i] = HUNGRY; test(i)
                while state[i] != EATING: self[i].wait()
    putdown(i): state[i] = THINKING; test(left(i)); test(right(i))
    test(i):    if state[i] == HUNGRY and neighbours not EATING:
                    state[i] = EATING; give forks i and i+1 to i; self[i].signal()
```

### Why this Python class is a real monitor

Python has no `monitor` keyword, so the class builds the same guarantees explicitly:

* **Mutual exclusion:** there is one `threading.Lock` (`self._lock`). Every entry procedure
  starts with `with self._lock:`, so at most one thread is ever inside the monitor.
* **Condition variables:** `self._self[i] = threading.Condition(self._lock)`, one per
  philosopher, all built on the *same* monitor lock. `wait()` atomically releases the
  monitor lock and blocks. When the thread is woken, it re-acquires the lock before it
  continues, which is exactly how monitor condition variables behave.
* **Private procedure:** `_test(i)` is only ever called with the lock already held.
* **Mesa semantics:** Python's `notify()` is *signal-and-continue*. The signalled thread
  does not run immediately; it competes for the lock later. That is why `pickup` waits in a
  `while state[i] != EATING` loop rather than an `if`, so the condition is re-checked after
  every wake-up. The textbook (Hoare) version can use `if`; the Mesa version must use `while`.

### Event kinds recorded

| Kind | Meaning |
|---|---|
| `THINK` | philosopher is thinking (outside the monitor) |
| `HUNGRY` | entered `pickup(i)`, `state[i] = HUNGRY` |
| `WAIT` | `test(i)` failed: blocked on `self[i].wait()`; the message names the neighbour(s) responsible |
| `ACQUIRE` | `test(i)` succeeded right away, so both forks are taken atomically |
| `SIGNAL` | a neighbour's `putdown` ran `test(i)`, handed both forks to `i` and signalled `self[i]` |
| `EAT` | the philosopher's thread has left `pickup` and is eating (meal counter incremented) |
| `RELEASE` | `putdown(i)`: both forks returned, then the neighbours are tested |
| `DONE` | the thread finished all its meals and exits |

## Why it is deadlock-free

A deadlock needs *hold-and-wait*: someone holding one fork while waiting for the other. That
cannot happen here. `_test(i)` hands over **both** forks in a single step, inside the monitor
lock, and only when both are free (neither neighbour is EATING). A philosopher therefore
holds either zero forks or two, never one. The circular wait "everyone holds the left fork"
can't form. Also, a waiting philosopher never holds the monitor lock (`wait()` releases it),
so the monitor itself can't be blocked.

## Starvation and the anti-starvation rule

The plain textbook monitor (`--no-fair`) is deadlock-free but **not starvation-free**.
Suppose P1 is hungry. Its neighbours P0 and P2 could take turns eating: P0 eats, P2 starts
before P0 finishes, P0 starts again before P2 finishes, and so on. Then at least one of P1's
neighbours is always EATING, `test(1)` never succeeds, and P1 waits forever.

This program adds a simple **ticket (age) rule**, which is on by default:

* When a philosopher becomes hungry it takes the next ticket number from a counter kept in
  the monitor.
* `test(i)` also refuses to let `i` eat if a **hungry neighbour holds an older ticket**. A
  neighbour that has waited longer goes first. (You can see this in the console as messages
  like `P3 waits on self[3] (P2 hungry, P0 hungry)`.)

Why it stays live and starvation-free: tickets are totally ordered, so the hungry
philosopher with the oldest ticket can only be blocked by a neighbour that is **eating**.
That neighbour finishes in finite time and its `putdown` runs `test()` on the oldest one,
which then eats. Each hungry philosopher has only finitely many older tickets ahead of it,
so it eventually becomes the oldest and eats. The trade-off is a little less parallelism:
sometimes a philosopher waits even though both of its forks are free.

## Runtime verification

* **Inside the monitor:** after every event (with the lock held) the program checks three
  things. No two adjacent philosophers are EATING. An EATING philosopher owns both of its
  forks. A fork is only ever owned by one of its two adjacent philosophers.
* **Outside the monitor:** a separate `TableObserver` with its own lock tracks who is
  *physically* eating (between `pickup` returning and `putdown`). It flags any moment when
  two neighbours eat at the same time.
* **Termination:** threads are joined with a generous timeout. If one were still alive, the
  program would report it rather than hang.
* At the end it prints meal counts per philosopher, the longest hungry wait, the
  signal/wait counts and the violation count (expected **0**). The HTML summary shows the
  same figures.

### Verified results

* 12 stress runs (seeds 1, 2, 3, 7, 11, 99, each with `--fair` and `--no-fair`, 20 meals
  each, 10 ms delays): all terminated, 0 violations, every philosopher ate 20 meals.
* A deliberately broken `test()` that ignores the neighbours, injected only for testing:
  the checkers reported hundreds of violations, confirming that they detect real errors.
