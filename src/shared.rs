// Shared state read by the monitor thread every 10 ms.
// The dispatcher is the sole writer; it holds a lock briefly when updating.
pub struct SharedState {
    pub current_cpu: f64,
    pub active_workers: usize,
    pub tasks_completed: usize,
    pub cpu_queue_len: usize,
    pub io_queue_len: usize,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            current_cpu: 0.0,
            active_workers: 0,
            tasks_completed: 0,
            cpu_queue_len: 0,
            io_queue_len: 0,
        }
    }
}
