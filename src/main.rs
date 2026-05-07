mod task;
mod shared;
mod simulation;
 
use simulation::{Config, Policy, run_simulation};
use std::env;
 
fn separator(title: &str) {
    println!("\n{}", "=".repeat(60));
    println!("  {}", title);
    println!("{}", "=".repeat(60));
}
 
fn main() {
    // Optional CLI arg: "fifo70" | "opt70" | "fifo80" | "opt80" | "all" (default)
    let arg = env::args().nth(1).unwrap_or_else(|| "all".to_string());
 
    let configs: Vec<(&str, Config)> = vec![
        (
            "Experiment A — 70/30 IO/CPU  |  FIFO",
            Config {
                total_tasks: 1000,
                io_ratio: 0.70,
                workers: 8,
                policy: Policy::Fifo,
                seed: 42,
                arrival_interval_ms: 20,
            },
        ),
        (
            "Experiment B — 70/30 IO/CPU  |  Optimized",
            Config {
                total_tasks: 1000,
                io_ratio: 0.70,
                workers: 8,
                policy: Policy::Optimized,
                seed: 42,
                arrival_interval_ms: 20,
            },
        ),
        (
            "Experiment C — 80/20 IO/CPU  |  FIFO",
            Config {
                total_tasks: 1000,
                io_ratio: 0.80,
                workers: 8,
                policy: Policy::Fifo,
                seed: 42,
                arrival_interval_ms: 20,
            },
        ),
        (
            "Experiment D — 80/20 IO/CPU  |  Optimized",
            Config {
                total_tasks: 1000,
                io_ratio: 0.80,
                workers: 8,
                policy: Policy::Optimized,
                seed: 42,
                arrival_interval_ms: 20,
            },
        ),
    ];
 
    // Filter based on CLI arg.
    let selected: Vec<_> = configs
        .into_iter()
        .filter(|(label, _)| {
            match arg.as_str() {
                "fifo70"  => label.contains("70") && label.contains("FIFO"),
                "opt70"   => label.contains("70") && label.contains("Optimized"),
                "fifo80"  => label.contains("80") && label.contains("FIFO"),
                "opt80"   => label.contains("80") && label.contains("Optimized"),
                _         => true, // "all" or anything else
            }
        })
        .collect();
 
    println!("\nConcurrent Task Dispatcher — Systems Programming Final Project");
    println!("Running {} simulation(s). Each task sleeps 200 ms; workers = 8.", selected.len());
    println!("Arrival interval = 20 ms  |  Total tasks = 1000  |  Seed = 42");
 
    let mut results = Vec::new();
 
    for (label, cfg) in selected {
        separator(label);
        println!("  [running — this takes ~30-40 s per simulation]");
        let result = run_simulation(label, cfg);
        result.print();
        results.push(result);
    }
 
    // ── Comparison summary ────────────────────────────────────────────────────
    if results.len() >= 2 {
        separator("COMPARISON SUMMARY");
        println!("{:<46} {:>12} {:>12} {:>12} {:>10}",
            "Label", "Makespan(s)", "AvgWait(ms)", "AvgTurn(ms)", "AvgCPU%");
        println!("{}", "-".repeat(96));
        for r in &results {
            println!("{:<46} {:>12.1} {:>12.1} {:>12.1} {:>10.1}",
                r.label,
                r.makespan_ms / 1000.0,
                r.avg_wait_ms,
                r.avg_turnaround_ms,
                r.avg_cpu_utilisation);
        }
 
        // If we have both FIFO and Optimized for the same ratio, print delta.
        for ratio_label in ["70/30", "80/20"] {
            let fifo = results.iter().find(|r| {
                r.label.contains(ratio_label) && r.policy == Policy::Fifo
            });
            let opt = results.iter().find(|r| {
                r.label.contains(ratio_label) && r.policy == Policy::Optimized
            });
            if let (Some(f), Some(o)) = (fifo, opt) {
                println!("\n{} — Optimized vs FIFO:", ratio_label);
                let delta_ms = f.makespan_ms - o.makespan_ms;
                let pct = if f.makespan_ms > 0.0 { delta_ms / f.makespan_ms * 100.0 } else { 0.0 };
                println!("  Makespan improvement : {:.1} ms  ({:.1}%)", delta_ms, pct);
                println!("  Wait time reduction  : {:.1} ms", f.avg_wait_ms - o.avg_wait_ms);
                println!("  CPU utilisation gain : {:.1}%", o.avg_cpu_utilisation - f.avg_cpu_utilisation);
            }
        }
    }
 
    println!("\nDone.");
}
