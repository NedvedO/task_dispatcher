use std::time::Instant;
 
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    Cpu,
    Io,
}
 
impl std::fmt::Display for TaskKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskKind::Cpu => write!(f, "CPU"),
            TaskKind::Io => write!(f, "IO"),
        }
    }
}
 
#[derive(Debug, Clone)]
pub struct Task {
    pub id: u64,
    pub arrival_time: Instant,
    pub kind: TaskKind,
    pub duration_ms: u64,
    pub cpu_cost: f64,
}
 
impl Task {
    pub fn cpu(id: u64, arrival_time: Instant) -> Self {
        Self {
            id,
            arrival_time,
            kind: TaskKind::Cpu,
            duration_ms: 200,
            cpu_cost: 35.0,
        }
    }
 
    pub fn io(id: u64, arrival_time: Instant) -> Self {
        Self {
            id,
            arrival_time,
            kind: TaskKind::Io,
            duration_ms: 200,
            cpu_cost: 10.0,
        }
    }
}
