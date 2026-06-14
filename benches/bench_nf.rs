//! `nf()` Scaling Benchmark
//!
//! Measures wall-clock time, memory growth, and iteration counts for the
//! Normal Form reduction operator across three synthetic event-set sizes:
//! 1 M, 10 M, and 100 M events.
//!
//! ## DAG shapes exercised
//!
//! Each size is tested under three synthetic topologies, chosen to stress
//! different phases of `nf()`:
//!
//! | Shape          | Primary stress         | Phase triggered |
//! |----------------|------------------------|-----------------|
//! | Linear chain   | Chain contraction (C2) | Phase C2        |
//! | Wide fan-out   | Cone hashing (C1)      | Phase C1 + D    |
//! | Noop-heavy     | No-op elimination (C3) | Phase C3        |
//!
//! ## How to run
//!
//! ```
//! cargo build --release --bin bench_nf
//! ./target/release/bench_nf
//! ```
//!
//! For a quick smoke-test at reduced sizes:
//!
//! ```
//! BENCH_QUICK=1 ./target/release/bench_nf
//! ```

use jc_computation::{CausalDag, Event, EventId, NormalForm};
use jc_computation::nf::NfConfig;
use std::collections::BTreeSet;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const SIZES: &[usize] = &[1_000_000, 10_000_000, 100_000_000];
const QUICK_SIZES: &[usize] = &[10_000, 100_000, 1_000_000];

// Noop ratio for the noop-heavy topology (0.0–1.0).
const NOOP_RATIO: f64 = 0.40;
// Fan-out width for the wide topology.
const FAN_WIDTH: usize = 4;

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct BenchResult {
    shape: &'static str,
    n_events_in: usize,
    n_events_out: usize,
    build_ms: f64,
    reduce_ms: f64,
    iterations: usize,
    cones_merged: usize,
    chains_contracted: usize,
    noops_eliminated: usize,
    /// Estimated resident bytes (Linux /proc only; 0 on other platforms)
    rss_kb: u64,
}

// ---------------------------------------------------------------------------
// Topology builders
// ---------------------------------------------------------------------------

/// Build a strictly linear chain: genesis → e1 → e2 → … → eN
///
/// Stresses Phase C2 because every interior node with a no-payload effect
/// is a chain-contraction candidate.  Here all events carry real data so
/// C2 won't fire, but the *structure* maximises the topological-sort cost
/// of Phase D (cone recomputation).
fn build_linear_chain(n: usize) -> (CausalDag, f64) {
    let t0 = Instant::now();
    let mut dag = CausalDag::with_capacity(n + 1);
    let g = Event::genesis();
    let mut tip: EventId = g.id.clone();
    dag.insert(g);

    for i in 0..n {
        let e = Event::data(
            "tx",
            serde_json::json!({"seq": i}),
            BTreeSet::from([tip.clone()]),
        );
        tip = e.id.clone();
        dag.insert(e);
    }
    (dag, t0.elapsed().as_secs_f64() * 1000.0)
}

/// Build a wide fan-out tree of depth ⌈log_{FAN_WIDTH}(n)⌉.
///
/// Each node fans out to FAN_WIDTH children until we reach n events.
/// This creates many concurrent events with distinct but related cones,
/// stressing Phase C1 (cone hashing across the full DAG) and Phase D.
fn build_wide_fanout(n: usize) -> (CausalDag, f64) {
    let t0 = Instant::now();
    let mut dag = CausalDag::with_capacity(n + 1);
    let g = Event::genesis();
    let gid = g.id.clone();
    dag.insert(g);

    let mut frontier: Vec<EventId> = vec![gid];
    let mut count = 1usize;

    'outer: loop {
        let mut next_frontier = Vec::new();
        for parent_id in &frontier {
            for branch in 0..FAN_WIDTH {
                if count >= n + 1 {
                    break 'outer;
                }
                let e = Event::data(
                    "branch",
                    serde_json::json!({"b": branch, "c": count}),
                    BTreeSet::from([parent_id.clone()]),
                );
                next_frontier.push(e.id.clone());
                dag.insert(e);
                count += 1;
            }
        }
        frontier = next_frontier;
        if frontier.is_empty() {
            break;
        }
    }

    (dag, t0.elapsed().as_secs_f64() * 1000.0)
}

/// Build a linear chain where NOOP_RATIO of events are no-ops.
///
/// Stresses Phase C3 (no-op elimination) and, indirectly, the subsequent
/// re-run of Phase D after the graph shrinks.
fn build_noop_chain(n: usize) -> (CausalDag, f64) {
    let t0 = Instant::now();
    let mut dag = CausalDag::with_capacity(n + 1);
    let g = Event::genesis();
    let mut tip: EventId = g.id.clone();
    dag.insert(g);

    let step = (1.0 / NOOP_RATIO).round() as usize;

    for i in 0..n {
        let e = if i % step == 0 {
            // No-op: eligible for Phase C3 elimination
            Event::noop(BTreeSet::from([tip.clone()]))
        } else {
            Event::data(
                "op",
                serde_json::json!({"i": i}),
                BTreeSet::from([tip.clone()]),
            )
        };
        tip = e.id.clone();
        dag.insert(e);
    }

    (dag, t0.elapsed().as_secs_f64() * 1000.0)
}

// ---------------------------------------------------------------------------
// Memory helpers
// ---------------------------------------------------------------------------

/// Read current RSS from /proc/self/status on Linux; 0 elsewhere.
fn rss_kb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        if let Ok(contents) = fs::read_to_string("/proc/self/status") {
            for line in contents.lines() {
                if line.starts_with("VmRSS:") {
                    let kb: u64 = line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    return kb;
                }
            }
        }
    }
    0
}

// ---------------------------------------------------------------------------
// Single benchmark run
// ---------------------------------------------------------------------------

fn run_one(
    shape: &'static str,
    (mut dag, build_ms): (CausalDag, f64),
) -> BenchResult {
    let n_in = dag.len();

    // Use a config with Phase A disabled — the synthetic DAGs built by
    // this benchmark are always causally closed, so the O(N) closure scan
    // is pure overhead.  All reduction phases (C1/C2/C3) remain enabled.
    let config = NfConfig {
        enable_closure_check: false,
        ..NfConfig::default()
    };
    let mut nf = NormalForm::new(config);

    let t1 = Instant::now();
    let stats = nf.reduce(&mut dag);
    let reduce_ms = t1.elapsed().as_secs_f64() * 1000.0;

    // Capture RSS while `dag` is still alive — measuring after drop gives
    // the baseline process RSS, not the peak DAG footprint.
    let rss = rss_kb();

    BenchResult {
        shape,
        n_events_in: n_in,
        n_events_out: stats.events_after,
        build_ms,
        reduce_ms,
        iterations: stats.iterations,
        cones_merged: stats.cones_merged,
        chains_contracted: stats.chains_contracted,
        noops_eliminated: stats.noops_eliminated,
        rss_kb: rss,
    }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn fmt_ms(ms: f64) -> String {
    if ms < 1_000.0 {
        format!("{:.1} ms", ms)
    } else if ms < 60_000.0 {
        format!("{:.2} s", ms / 1_000.0)
    } else {
        format!("{:.1} min", ms / 60_000.0)
    }
}

fn fmt_events(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn print_header() {
    println!(
        "\n{:<14} {:>10} {:>12} {:>12} {:>12} {:>6} {:>8} {:>10} {:>9} {:>9}",
        "Shape", "N-in", "N-out", "Build", "nf() time", "Iters",
        "Merged", "Contracted", "Noops-elim", "RSS (KB)"
    );
    println!("{}", "-".repeat(111));
}

fn print_row(r: &BenchResult) {
    let reduction_pct = if r.n_events_in > 0 {
        100.0 * (1.0 - r.n_events_out as f64 / r.n_events_in as f64)
    } else {
        0.0
    };
    let n_out_str = if reduction_pct.abs() < 0.5 {
        fmt_events(r.n_events_out)
    } else {
        format!("{} (-{:.0}%)", fmt_events(r.n_events_out), reduction_pct)
    };
    println!(
        "{:<14} {:>10} {:>12} {:>12} {:>12} {:>6} {:>8} {:>10} {:>10} {:>9}",
        r.shape,
        fmt_events(r.n_events_in),
        n_out_str,
        fmt_ms(r.build_ms),
        fmt_ms(r.reduce_ms),
        r.iterations,
        r.cones_merged,
        r.chains_contracted,
        r.noops_eliminated,
        r.rss_kb,
    );
}

fn print_scaling_analysis(results: &[BenchResult]) {
    println!("\n── Scaling Analysis (nf() time) ──────────────────────────────────────────");
    println!(
        "{:<14} {:<8} {:<14} {:<14} {:<12}",
        "Shape", "N", "nf() ms", "x prev", "Implied O()"
    );
    println!("{}", "-".repeat(65));

    // Group by shape
    let shapes = ["linear", "fanout", "noop-chain"];
    for shape in &shapes {
        let group: Vec<&BenchResult> = results.iter().filter(|r| r.shape == *shape).collect();
        let mut prev_ms = 0.0f64;
        let mut prev_n = 0usize;
        for r in &group {
            let ratio_str = if prev_ms > 0.0 && prev_n > 0 {
                let time_ratio = r.reduce_ms / prev_ms;
                let n_ratio = r.n_events_in as f64 / prev_n as f64;
                let exponent = time_ratio.log(n_ratio);
                format!("{:.2}×  → O(n^{:.2})", time_ratio, exponent)
            } else {
                "  –".to_string()
            };
            println!(
                "{:<14} {:<8} {:<14} {}",
                r.shape,
                fmt_events(r.n_events_in),
                fmt_ms(r.reduce_ms),
                ratio_str,
            );
            prev_ms = r.reduce_ms;
            prev_n = r.n_events_in;
        }
        println!();
    }
}

fn print_verdict(results: &[BenchResult]) {
    println!("── Verdict ───────────────────────────────────────────────────────────────");
    let max_reduce_ms = results.iter().map(|r| r.reduce_ms as u64).max().unwrap_or(0);
    let max_rss = results.iter().map(|r| r.rss_kb).max().unwrap_or(0);

    // Estimate implied exponent from the two largest sizes of the linear shape
    let linear: Vec<&BenchResult> = results.iter().filter(|r| r.shape == "linear").collect();
    let implied_o = if linear.len() >= 2 {
        let last = linear[linear.len() - 1];
        let prev = linear[linear.len() - 2];
        if prev.reduce_ms > 0.0 && prev.n_events_in > 0 {
            let time_ratio = last.reduce_ms / prev.reduce_ms;
            let n_ratio = last.n_events_in as f64 / prev.n_events_in as f64;
            Some(time_ratio.log(n_ratio))
        } else {
            None
        }
    } else {
        None
    };

    if let Some(exp) = implied_o {
        if exp < 1.15 {
            println!("✓  nf() appears to scale sub-linearly or linearly (n^{:.2}) on linear histories.", exp);
            println!("   The idea has legs for event streams that arrive as chains.");
        } else if exp < 1.60 {
            println!("~  nf() scales quasi-linearly (n^{:.2}) — acceptable for moderate loads.", exp);
            println!("   Watch cone-hash recomputation cost at 100 M+ events.");
        } else {
            println!("✗  nf() appears super-linear (n^{:.2}) — may not be viable at 100 M scale.", exp);
            println!("   Investigate incremental cone hashing and lazy Phase D.");
        }
    }

    println!(
        "\nPeak wall time: {}   |   Peak RSS: {} MB",
        fmt_ms(max_reduce_ms as f64),
        max_rss / 1024,
    );
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let quick = std::env::var("BENCH_QUICK").is_ok();
    let sizes: &[usize] = if quick { QUICK_SIZES } else { SIZES };

    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║           jc-computation  ·  nf() Scaling Benchmark                ║");
    println!("╚══════════════════════════════════════════════════════════════════════╝");
    println!();
    if quick {
        println!("  [BENCH_QUICK mode: sizes = {:?}]", sizes);
    } else {
        println!("  Sizes: 1 M / 10 M / 100 M events");
    }
    println!("  Phases: A (closure) · B (order) · C1 (cone-merge) · C2 (chain)");
    println!("          C3 (noop-elim) · D (hash stabilise)");
    println!("  Topologies: linear chain · wide fan-out ({}×) · noop-chain (~{:.0}% noops)",
             FAN_WIDTH, NOOP_RATIO * 100.0);
    println!();
    println!("  Building synthetic DAGs and running nf() — this may take a while …");
    println!("  (set BENCH_QUICK=1 for a 1K/10K/100K smoke-test)");

    print_header();

    let mut all_results: Vec<BenchResult> = Vec::new();

    for &n in sizes {
        // ── Linear chain ──────────────────────────────────────────────────
        println!("  [ building linear n={} ]", fmt_events(n));
        let built = build_linear_chain(n);
        println!("  [ done in {:.0} ms — running nf() ]", built.1);
        let r = run_one("linear", built);
        print_row(&r);
        all_results.push(r);

        // ── Wide fan-out ──────────────────────────────────────────────────
        println!("  [ building fanout n={} ]", fmt_events(n));
        let built = build_wide_fanout(n);
        println!("  [ done in {:.0} ms — running nf() ]", built.1);
        let r = run_one("fanout", built);
        print_row(&r);
        all_results.push(r);

        // ── Noop chain ────────────────────────────────────────────────────
        println!("  [ building noop-chain n={} ]", fmt_events(n));
        let built = build_noop_chain(n);
        println!("  [ done in {:.0} ms — running nf() ]", built.1);
        let r = run_one("noop-chain", built);
        print_row(&r);
        all_results.push(r);

        println!(); // blank row between size groups
    }

    print_scaling_analysis(&all_results);
    print_verdict(&all_results);

    println!();
}
