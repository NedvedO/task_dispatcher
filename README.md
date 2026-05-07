# Concurrent Task Dispatcher

Systems Programming Final Project — Rust  
**Student:** [your name]

## Overview

A concurrent task dispatcher simulation with:
- 1 generator thread, 1 dispatcher (main thread), 8 worker threads, 1 monitor thread = **11 threads total**
- Two task types: **IO** (10% CPU, 200 ms) and **CPU** (35% CPU, 200 ms)
- Global CPU cap enforced at 100% — dispatcher will not schedule a task if adding it would exceed the cap
- Two scheduling policies compared: **FIFO** and **Optimized**
- Two workloads compared: **70/30** IO/CPU and **80/20** IO/CPU

## How to Build and Run

### Prerequisites

Install Rust via [https://rustup.rs](https://rustup.rs):
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# Windows: download and run rustup-init.exe from rustup.rs
```

### Build
```
cd task_dispatcher
cargo build --release
```

### Run all four experiments (takes ~2–3 minutes)
```
cargo run --release
```

### Run a single experiment (faster for demo)
```
cargo run --release -- fifo70    # 70/30, FIFO
cargo run --release -- opt70     # 70/30, Optimized
cargo run --release -- fifo80    # 80/20, FIFO
cargo run --release -- opt80     # 80/20, Optimized
```

### Compare just one distribution (both policies)
Run `fifo70` first, note the makespan. Then run `opt70`. Compare.

## Architecture

```
Generator ──(DispatchMsg::Arrived)──► Dispatcher ──► Worker 0
                                           │         Worker 1
Monitor ◄──(Arc<Mutex<SharedState>>)──────┘          ...
                                                      Worker 7
Workers ──(DispatchMsg::WorkerFree)──────────────► Dispatcher
```

### Threads and their roles

| Thread | Count | Role |
|---|---|---|
| Generator | 1 | Creates 1000 tasks, sends one every 20 ms, then sends `GeneratorDone` |
| Dispatcher | 1 (main) | Holds the queues; checks CPU cap + free workers before dispatching |
| Workers | 8 | Receive a task, sleep 200 ms, send `WorkerFree` back to dispatcher |
| Monitor | 1 | Reads `SharedState` every 10 ms; accumulates snapshots for metrics |

### Shared data and synchronisation

| Data | Protection | Who reads | Who writes |
|---|---|---|---|
| `SharedState` (cpu, workers, queue lengths) | `Arc<Mutex<_>>` | Monitor thread | Dispatcher (after each dispatch or completion) |
| Per-worker channels | `mpsc::Sender` / `Receiver` | Each worker | Dispatcher |
| Dispatcher inbox | `mpsc::Receiver<DispatchMsg>` | Dispatcher only | Generator + all workers |

Channels carry ownership — no extra lock needed.  
`Arc<Mutex<SharedState>>` is the only true shared-state lock, and it is held only briefly.

### Queues

**FIFO mode**: one `VecDeque<Task>`. Tasks enter in arrival order. Dispatcher takes the head only if `current_cpu + head.cpu_cost ≤ 100`. If not, the dispatcher stalls (head-of-line blocking) until a worker finishes and CPU drops.

**Optimized mode**: two `VecDeque<Task>` — one for CPU tasks, one for IO tasks. Dispatcher checks CPU tasks first (since they are the scarce resource at 35% each); if the CPU budget is full it falls back to IO tasks. This avoids head-of-line blocking and keeps all 8 workers busy whenever possible.

### Scheduling policies

#### FIFO
- Single queue, strict arrival order
- Simple and fair within arrival order
- **Problem**: if the front is a CPU task and two CPU tasks are already running (70% CPU used), *all* workers sit idle even though IO tasks are waiting

#### Optimized (two-queue, CPU-first dispatch)
- Separate CPU and IO queues
- Dispatcher prefers CPU tasks when budget allows (≤ 65% used), otherwise picks IO
- This matches the LP-derived optimal batch shape:  
  `2 CPU (70%) + up to 3 IO (30%) = 100% CPU, all 8 workers busy`
- Result: lower makespan, lower wait time, higher CPU utilisation

### Why channels over `Arc<Mutex<VecDeque>>` for task delivery?

Channels transfer *ownership*. The dispatcher is the sole owner of any task at any time — no two threads can touch the same task concurrently. A shared `Mutex<VecDeque>` would require every worker to lock the queue on completion, creating contention and making it harder to reason about who owns a task.

### Optimisation math (LP derivation)

With 8 workers and 200 ms per task, each "round" can hold:

| Option | CPU tasks | IO tasks | CPU used |
|---|---|---|---|
| 1 | 2 | 3 | 70 + 30 = 100% |
| 2 | 1 | 6 | 35 + 60 = 95% |
| 3 | 0 | 8 | 0 + 80 = 80% |

For 1000 tasks at 70/30: 700 IO + 300 CPU.  
Let x, y, z = number of rounds using options 1, 2, 3.

Constraints:
- `2x + y       = 300`  (CPU tasks)
- `3x + 6y + 8z = 700`  (IO tasks)
- Minimise `x + y + z`  (total rounds = makespan / 200 ms)

Solution: x = 124, y = 52, z = 2 → **178 rounds × 200 ms = 35.6 s** minimum makespan.

FIFO cannot achieve this because it stalls behind CPU-task head-of-line blocks.

## Experiments

### Experiment A & B — 70/30 IO/CPU

**Balanced workload** — IO tasks dominate but CPU tasks still create meaningful pressure.  
FIFO stalls when 2 CPU tasks are executing and a third CPU task is at the queue head.  
Optimized bypasses the stall by pulling from the IO queue instead.

### Experiment C & D — 80/20 IO/CPU

**Stressed towards IO** — only 200 CPU tasks, so CPU cap is rarely the bottleneck.  
Both policies perform similarly; any gap shows the residual cost of FIFO head-of-line blocking on the smaller number of CPU tasks.

## Metrics collected

| Metric | Description |
|---|---|
| Makespan | Wall-clock time from first arrival to last completion |
| Average wait time | Time from arrival to dispatch, averaged over all tasks |
| Max wait time | Worst-case wait across all tasks |
| Average turnaround | Time from arrival to completion |
| Avg CPU utilisation | Mean simulated CPU% across monitor snapshots |
| Avg active workers | Mean number of busy workers |
| Peak queue length | Maximum total queued tasks seen by monitor |

## Lessons learned

- Head-of-line blocking in FIFO is a real cost even when workers are free — separating queues by task type directly fixes it
- Channels naturally enforce ownership; shared locks should be reserved for state that multiple threads genuinely need to *read and write concurrently* (like the monitor snapshot state)
- The CPU cap creates an interesting LP-style scheduling problem — reasoning about it mathematically predicted exactly what the Optimized policy does in practice

## Tool Use Disclosure

Claude Code (claude-sonnet-4-6) was used to scaffold this project.  
**Help provided**: wrote the Rust code for the dispatcher, worker, generator, and monitor threads; structured the channel-based message-passing design.  
**Advice accepted**: using a single unified `DispatchMsg` enum so both the generator and workers share one dispatcher inbox — cleaner than two separate channels.  
**Advice rejected / fixed**: initial monitor implementation polled with a tight loop; changed to `thread::sleep(10 ms)` with an `AtomicBool` stop flag to avoid busy-waiting.
