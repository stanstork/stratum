use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// One stage bucket: accumulated nanoseconds + how many times it was recorded.
pub struct Bucket {
    nanos: AtomicU64,
    count: AtomicU64,
}

impl Bucket {
    const fn new() -> Self {
        Self {
            nanos: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    fn add(&self, d: Duration) {
        self.nanos.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn read(&self) -> (Duration, u64) {
        (
            Duration::from_nanos(self.nanos.load(Ordering::Relaxed)),
            self.count.load(Ordering::Relaxed),
        )
    }
}

// Producer-side stages.
pub static FETCH: Bucket = Bucket::new(); // reading a page from the source
pub static TRANSFORM: Bucket = Bucket::new(); // field mapping / computed columns / plugin
pub static SEND: Bucket = Bucket::new(); // handing the batch to the channel (blocks under backpressure)
pub static PLUGIN_CALL: Bucket = Bucket::new(); // subset of TRANSFORM: the WASM plugin boundary itself

/// Consumer-side stages.
pub static WRITE: Bucket = Bucket::new(); // writing a batch to the destination
pub static CHECKPOINT: Bucket = Bucket::new(); // state-store commit + checkpoint per batch

/// True when profiling output is requested (`STRATUM_PROFILE` set).
pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("STRATUM_PROFILE").is_some())
}

/// Record `d` against `bucket` (always cheap; recording is unconditional so the
/// numbers are correct even if the summary is never printed).
#[inline]
pub fn record(bucket: &Bucket, d: Duration) {
    bucket.add(d);
}

/// Per-stage timing keyed by a stable stage kind (e.g. "wasm", "computed",
/// "prune"). Locked per batch (not per row), so the mutex is not a hot path.
static STAGES: Mutex<BTreeMap<&'static str, (u128, u64)>> = Mutex::new(BTreeMap::new());

/// Record `d` against a named pipeline stage, subdividing the TRANSFORM total.
pub fn record_stage(kind: &'static str, d: Duration) {
    if let Ok(mut map) = STAGES.lock() {
        let (nanos, count) = map.entry(kind).or_insert((0, 0));
        *nanos += d.as_nanos();
        *count += 1;
    }
}

/// Print the stage summary to stderr. No-op unless `STRATUM_PROFILE` is set.
pub fn dump() {
    if !enabled() {
        return;
    }

    eprintln!("\n==== stratum stage profile (cumulative actor wall-time) ====");

    let rows = [
        ("producer: fetch (read source)", FETCH.read()),
        ("producer: transform (total)", TRANSFORM.read()),
        ("  of which: plugin WASM boundary", PLUGIN_CALL.read()),
        ("producer: send (backpressure)", SEND.read()),
        ("consumer: write (dest)", WRITE.read()),
        ("consumer: checkpoint (state)", CHECKPOINT.read()),
    ];

    for (label, (dur, count)) in rows {
        print_row(label, dur, count, "  ");
    }

    let fetch = FETCH.read().0;
    let transform = TRANSFORM.read().0;
    let send = SEND.read().0;
    let write = WRITE.read().0;
    let checkpoint = CHECKPOINT.read().0;

    eprintln!("  ----");
    eprintln!(
        "  producer busy (fetch+transform)   : {:>12.3?}",
        fetch + transform
    );
    eprintln!("  producer send-blocked             : {send:>12.3?}   (high => writer-bound)");
    eprintln!(
        "  consumer busy (write+checkpoint)  : {:>12.3?}",
        write + checkpoint
    );

    if let Ok(map) = STAGES.lock()
        && !map.is_empty()
    {
        eprintln!("\n  transform stage breakdown (subset of transform total):");
        let mut rows: Vec<_> = map.iter().collect();

        // Sort in descending order of duration
        rows.sort_by_key(|(_, (nanos, _))| std::cmp::Reverse(*nanos));

        for (kind, (nanos, count)) in rows {
            let dur = Duration::from_nanos(*nanos as u64);
            print_row(kind, dur, *count, "    ");
        }
    }
    eprintln!();
}

fn print_row(label: &str, dur: Duration, count: u64, indent: &str) {
    let per = if count > 0 {
        dur / count as u32
    } else {
        Duration::ZERO
    };
    eprintln!("{indent}{label:32} {dur:>12.3?}  ({count:>6} calls, {per:>10.3?}/call)");
}
