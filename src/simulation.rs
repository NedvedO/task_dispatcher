use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}},
    thread,
    time::{Duration, Instant},
};
 
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
 
use crate::task::{Task, TaskKind};
use crate::shared::SharedState;
 
// ── Configuration ─────────────────────────────────────────────────────────────
 
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Policy {
    /// Single FIFO queue. Dispatcher takes tasks in arrival order;
    /// blocks on head-of-line if adding that task would exceed the CPU cap.
    Fifo,
    /// Two queues (CPU / IO). Dispatcher picks whichever queue maximises
    /// throughput without exceeding the 100 % CPU cap.
    Optimized,
}
 
impl std::fmt::Display for Policy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Policy::Fifo => write!(f, "FIFO"),
            Policy::Optimized => write!(f, "Optimized"),
        }
    }
}
 
#[derive(Clone)]
pub struct Config {
    pub total_tasks: usize,
    /// Fraction of tasks that are IO (e.g. 0.70 means 70 % IO, 30 % CPU).
    pub io_ratio: f64,
    pub workers: usize,
    pub policy: Policy,
    pub seed: u64,
    /// Gap between consecutive task arrivals in milliseconds.
    pub arrival_interval_ms: u64,
}
 
// ── Metrics records ───────────────────────────────────────────────────────────
 
pub struct CompletedRecord {
    pub task_id: u64,
    pub kind: TaskKind,
    pub arrival_time: Instant,
    pub dispatch_time: Instant,
    pub completion_time: Instant,
}
 
impl CompletedRecord {
    pub fn wait_ms(&self) -> f64 {
        self.dispatch_time.duration_since(self.arrival_time).as_secs_f64() * 1000.0
    }
    pub fn turnaround_ms(&self) -> f64 {
        self.completion_time.duration_since(self.arrival_time).as_secs_f64() * 1000.0
    }
}
 
// Snapshot taken by the monitor thread every 10 ms.
struct MonitorSnapshot {
    elapsed_ms: u64,
    current_cpu: f64,
    active_workers: usize,
    cpu_q: usize,
    io_q: usize,
    completed: usize,
}
 
// ── Result ────────────────────────────────────────────────────────────────────
 
pub struct SimulationResult {
    pub label: String,
    pub policy: Policy,
    pub io_ratio: f64,
    pub total_tasks: usize,
    pub makespan_ms: f64,
    pub avg_wait_ms: f64,
    pub avg_turnaround_ms: f64,
    pub max_wait_ms: f64,
    pub cpu_tasks_done: usize,
    pub io_tasks_done: usize,
    pub avg_cpu_utilisation: f64,
    pub avg_active_workers: f64,
    pub peak_queue_len: usize,
}
 
impl SimulationResult {
    pub fn print(&self) {
        println!("  Policy            : {}", self.policy);
        println!("  IO ratio          : {:.0}% IO / {:.0}% CPU", self.io_ratio * 100.0, (1.0 - self.io_ratio) * 100.0);
        println!("  Tasks completed   : {} ({} CPU + {} IO)", self.total_tasks, self.cpu_tasks_done, self.io_tasks_done);
        println!("  Makespan          : {:.1} ms  ({:.1} s)", self.makespan_ms, self.makespan_ms / 1000.0);
        println!("  Avg wait time     : {:.1} ms", self.avg_wait_ms);
        println!("  Max wait time     : {:.1} ms", self.max_wait_ms);
        println!("  Avg turnaround    : {:.1} ms", self.avg_turnaround_ms);
        println!("  Avg CPU usage     : {:.1}%", self.avg_cpu_utilisation);
        println!("  Avg active workers: {:.2} / 8", self.avg_active_workers);
        println!("  Peak queue length : {}", self.peak_queue_len);
    }
}
 
// ── Thread message types ──────────────────────────────────────────────────────
 
/// Messages flowing INTO the dispatcher.
enum DispatchMsg {
    /// A new task has arrived from the generator.
    Arrived(Task),
    /// A worker has finished a task and is now free.
    WorkerFree {
        worker_id: usize,
        cpu_released: f64,
        record: CompletedRecord,
    },
    /// Generator has sent all tasks; no more Arrived messages will come.
    GeneratorDone,
}
 
/// Messages flowing FROM the dispatcher TO a worker.
enum WorkerMsg {
    Execute { task: Task, dispatch_time: Instant },
    Shutdown,
}
 
// ── run_simulation ─────────────────────────────────────────────────────────────
 
pub fn run_simulation(label: &str, cfg: Config) -> SimulationResult {
    let sim_start = Instant::now();
 
    // Shared state for monitor thread.
    let shared = Arc::new(Mutex::new(SharedState::new()));
    let monitor_stop = Arc::new(AtomicBool::new(false));
 
    // Dispatcher receives from both generator and workers via a single channel.
    let (disp_tx, disp_rx) = std::sync::mpsc::channel::<DispatchMsg>();
 
    // Per-worker channels.
    let mut worker_txs: Vec<std::sync::mpsc::Sender<WorkerMsg>> = Vec::new();
    let mut worker_handles: Vec<thread::JoinHandle<()>> = Vec::new();
 
    for worker_id in 0..cfg.workers {
        let (w_tx, w_rx) = std::sync::mpsc::channel::<WorkerMsg>();
        worker_txs.push(w_tx);
        let disp_tx_clone = disp_tx.clone();
 
        let handle = thread::spawn(move || {
            loop {
                match w_rx.recv() {
                    Ok(WorkerMsg::Execute { task, dispatch_time }) => {
                        thread::sleep(Duration::from_millis(task.duration_ms));
                        let completion_time = Instant::now();
                        let record = CompletedRecord {
                            task_id: task.id,
                            kind: task.kind,
                            arrival_time: task.arrival_time,
                            dispatch_time,
                            completion_time,
                        };
                        let _ = disp_tx_clone.send(DispatchMsg::WorkerFree {
                            worker_id,
                            cpu_released: task.cpu_cost,
                            record,
                        });
                    }
                    Ok(WorkerMsg::Shutdown) | Err(_) => break,
                }
            }
        });
        worker_handles.push(handle);
    }
 
    // Generator thread: emits tasks at arrival_interval_ms, then signals done.
    {
        let disp_tx_gen = disp_tx.clone();
        let cfg_gen = cfg.clone();
        thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(cfg_gen.seed);
            for id in 0..cfg_gen.total_tasks as u64 {
                let arrival_time = Instant::now();
                let task = if rng.gen::<f64>() < cfg_gen.io_ratio {
                    Task::io(id, arrival_time)
                } else {
                    Task::cpu(id, arrival_time)
                };
                let _ = disp_tx_gen.send(DispatchMsg::Arrived(task));
                thread::sleep(Duration::from_millis(cfg_gen.arrival_interval_ms));
            }
            let _ = disp_tx_gen.send(DispatchMsg::GeneratorDone);
        });
    }
 
    // Monitor thread: samples SharedState every 10 ms.
    let snapshots: Arc<Mutex<Vec<MonitorSnapshot>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let shared_mon = Arc::clone(&shared);
        let stop_flag = Arc::clone(&monitor_stop);
        let snaps_mon = Arc::clone(&snapshots);
        thread::spawn(move || {
            let start = Instant::now();
            while !stop_flag.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(10));
                let elapsed_ms = start.elapsed().as_millis() as u64;
                let s = shared_mon.lock().unwrap();
                let snap = MonitorSnapshot {
                    elapsed_ms,
                    current_cpu: s.current_cpu,
                    active_workers: s.active_workers,
                    cpu_q: s.cpu_queue_len,
                    io_q: s.io_queue_len,
                    completed: s.tasks_completed,
                };
                drop(s);
                snaps_mon.lock().unwrap().push(snap);
            }
        });
    }
 
    // ── Dispatcher logic ───────────────────────────────────────────────────────
 
    // The dispatcher runs on the current thread (main) until all tasks complete.
    // This keeps the design simple: no extra thread, and the loop is the
    // "think before send" manager the spec requires.
 
    let mut cpu_queue: VecDeque<Task> = VecDeque::new();
    let mut io_queue: VecDeque<Task> = VecDeque::new();
    let mut fifo_queue: VecDeque<Task> = VecDeque::new();
 
    let mut free_workers: VecDeque<usize> = (0..cfg.workers).collect();
    let mut current_cpu: f64 = 0.0;
    let mut generator_done = false;
    let mut completed_records: Vec<CompletedRecord> = Vec::new();
 
    // Helper: update shared state so the monitor can read it.
    let update_shared = |shared: &Arc<Mutex<SharedState>>,
                          cpu: f64,
                          active: usize,
                          done: usize,
                          cq: usize,
                          iq: usize| {
        let mut s = shared.lock().unwrap();
        s.current_cpu = cpu;
        s.active_workers = active;
        s.tasks_completed = done;
        s.cpu_queue_len = cq;
        s.io_queue_len = iq;
    };
 
    loop {
        // Try to dispatch as many tasks as possible before blocking on recv.
        loop {
            if free_workers.is_empty() {
                break; // no worker available
            }
            let dispatched = match cfg.policy {
                Policy::Fifo => try_dispatch_fifo(
                    &mut fifo_queue,
                    &mut free_workers,
                    &mut current_cpu,
                    &worker_txs,
                ),
                Policy::Optimized => try_dispatch_optimized(
                    &mut cpu_queue,
                    &mut io_queue,
                    &mut free_workers,
                    &mut current_cpu,
                    &worker_txs,
                ),
            };
            if !dispatched {
                break;
            }
        }
 
        // Update monitor snapshot.
        let active = cfg.workers - free_workers.len();
        let (cq, iq) = match cfg.policy {
            Policy::Fifo => (0, fifo_queue.len()),
            Policy::Optimized => (cpu_queue.len(), io_queue.len()),
        };
        update_shared(&shared, current_cpu, active, completed_records.len(), cq, iq);
 
        // Check for termination: generator done, no queued tasks, no busy workers.
        let queued = fifo_queue.len() + cpu_queue.len() + io_queue.len();
        if generator_done && queued == 0 && active == 0 {
            break;
        }
 
        // If there are queued tasks, don't block forever — use a short timeout
        // so we can re-attempt dispatch as soon as a worker frees up.
        // If queues are empty, block normally to avoid busy-waiting.
        let queued_now = fifo_queue.len() + cpu_queue.len() + io_queue.len();
        let msg = if queued_now > 0 {
            disp_rx.recv_timeout(Duration::from_millis(1)).ok()
        } else {
            disp_rx.recv().ok()
        };
        if let Some(msg) = msg {
            match msg {
                DispatchMsg::Arrived(task) => {
                    match cfg.policy {
                        Policy::Fifo => fifo_queue.push_back(task),
                        Policy::Optimized => match task.kind {
                            TaskKind::Cpu => cpu_queue.push_back(task),
                            TaskKind::Io => io_queue.push_back(task),
                        },
                    }
                }
                DispatchMsg::WorkerFree { worker_id, cpu_released, record } => {
                    current_cpu -= cpu_released;
                    if current_cpu < 0.0 { current_cpu = 0.0; }
                    free_workers.push_back(worker_id);
                    completed_records.push(record);
                }
                DispatchMsg::GeneratorDone => {
                    generator_done = true;
                }
            }
        }
    }
 
    // Shut down all workers.
    for tx in &worker_txs {
        let _ = tx.send(WorkerMsg::Shutdown);
    }
    for h in worker_handles {
        let _ = h.join();
    }
 
    // Stop monitor.
    monitor_stop.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(15)); // let monitor tick once more then exit
 
    let makespan_ms = sim_start.elapsed().as_secs_f64() * 1000.0;
 
    // ── Compute result metrics ────────────────────────────────────────────────
 
    let total = completed_records.len();
    let cpu_done = completed_records.iter().filter(|r| r.kind == TaskKind::Cpu).count();
    let io_done = total - cpu_done;
 
    let avg_wait = completed_records.iter().map(|r| r.wait_ms()).sum::<f64>() / total as f64;
    let max_wait = completed_records.iter().map(|r| r.wait_ms()).fold(0.0_f64, f64::max);
    let avg_turn = completed_records.iter().map(|r| r.turnaround_ms()).sum::<f64>() / total as f64;
 
    let snaps = snapshots.lock().unwrap();
    let avg_cpu = if snaps.is_empty() {
        0.0
    } else {
        snaps.iter().map(|s| s.current_cpu).sum::<f64>() / snaps.len() as f64
    };
    let avg_workers = if snaps.is_empty() {
        0.0
    } else {
        snaps.iter().map(|s| s.active_workers as f64).sum::<f64>() / snaps.len() as f64
    };
    let peak_q = snaps.iter().map(|s| s.cpu_q + s.io_q).max().unwrap_or(0);
 
    SimulationResult {
        label: label.to_string(),
        policy: cfg.policy,
        io_ratio: cfg.io_ratio,
        total_tasks: total,
        makespan_ms,
        avg_wait_ms: avg_wait,
        avg_turnaround_ms: avg_turn,
        max_wait_ms: max_wait,
        cpu_tasks_done: cpu_done,
        io_tasks_done: io_done,
        avg_cpu_utilisation: avg_cpu,
        avg_active_workers: avg_workers,
        peak_queue_len: peak_q,
    }
}
 
// ── Dispatch helpers ──────────────────────────────────────────────────────────
 
/// FIFO: try to dispatch the front of the single queue.
/// Returns true if a task was sent to a worker.
fn try_dispatch_fifo(
    queue: &mut VecDeque<Task>,
    free_workers: &mut VecDeque<usize>,
    current_cpu: &mut f64,
    worker_txs: &[std::sync::mpsc::Sender<WorkerMsg>],
) -> bool {
    let task = match queue.front() {
        Some(t) => t,
        None => return false,
    };
    // Head-of-line check: can we run this task without exceeding CPU cap?
    if *current_cpu + task.cpu_cost > 100.0 + 1e-9 {
        return false; // blocked — FIFO stalls here
    }
    let task = queue.pop_front().unwrap();
    let worker_id = free_workers.pop_front().unwrap();
    *current_cpu += task.cpu_cost;
    let _ = worker_txs[worker_id].send(WorkerMsg::Execute {
        task,
        dispatch_time: Instant::now(),
    });
    true
}
 
/// Optimized: two queues. Pick the task that best fills the remaining CPU
/// budget. Tries to pack workers greedily:
///   - If a CPU task fits AND fills budget better than IO, prefer CPU
///   - If CPU doesn't fit but IO does, take IO
///   - If both fit, prefer CPU (35% is the scarce resource)
///   - Special case: if remaining budget is exactly 10% (only IO fits), take IO
fn try_dispatch_optimized(
    cpu_queue: &mut VecDeque<Task>,
    io_queue: &mut VecDeque<Task>,
    free_workers: &mut VecDeque<usize>,
    current_cpu: &mut f64,
    worker_txs: &[std::sync::mpsc::Sender<WorkerMsg>],
) -> bool {
    let remaining = 100.0 - *current_cpu;
 
    // Determine what fits
    let cpu_fits = !cpu_queue.is_empty() && remaining >= 35.0 - 1e-9;
    let io_fits  = !io_queue.is_empty()  && remaining >= 10.0 - 1e-9;
 
    let chosen: Option<Task> = if cpu_fits && io_fits {
        // Both fit — prefer CPU task to consume the scarce budget slot.
        // Exception: if we have many more IO tasks queued and budget is tight,
        // still prefer CPU first (greedier = lower makespan).
        Some(cpu_queue.pop_front().unwrap())
    } else if cpu_fits {
        Some(cpu_queue.pop_front().unwrap())
    } else if io_fits {
        Some(io_queue.pop_front().unwrap())
    } else {
        None
    };
 
    match chosen {
        None => false,
        Some(task) => {
            let worker_id = free_workers.pop_front().unwrap();
            *current_cpu += task.cpu_cost;
            let _ = worker_txs[worker_id].send(WorkerMsg::Execute {
                task,
                dispatch_time: Instant::now(),
            });
            true
        }
    }
}