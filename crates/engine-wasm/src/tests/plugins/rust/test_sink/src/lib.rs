//! A minimal sink plugin for integration tests.

use core::sync::atomic::{AtomicU64, Ordering};
use stratum_plugin_sdk::{
    PluginBatch, PluginError, PluginResult, WriteResult, log_info, sink_config, stratum_sink,
};

static TOTAL: AtomicU64 = AtomicU64::new(0);

#[stratum_sink(
    name = "test_sink",
    version = "1.0.0",
    input = [
        { name = "id", type = "i64", nullable = false },
    ],
    prepare = "open",
    finalize = "flush"
)]
fn write_batch(batch: PluginBatch) -> PluginResult<WriteResult> {
    let n = batch.len() as u64;
    let running = TOTAL.fetch_add(n, Ordering::Relaxed) + n;
    log_info(&format!("test_sink wrote {n} (running total {running})"));
    Ok(WriteResult::new(n))
}

fn open() -> PluginResult<()> {
    TOTAL.store(0, Ordering::Relaxed);
    log_info("test_sink prepare");
    Ok(())
}

fn flush() -> PluginResult<()> {
    let total = TOTAL.load(Ordering::Relaxed);
    log_info(&format!("test_sink finalize total={total}"));

    // When the host supplies `expect`, assert the drained total matches. A
    // mismatch fails finalize -> fails the migration, giving tests a robust
    // pass/fail signal without inspecting logs.
    if let Ok(cfg) = sink_config()
        && let Some(expect) = cfg.get("expect")
    {
        let expect: u64 = expect
            .parse()
            .map_err(|_| PluginError::internal(format!("invalid expect value: {expect:?}")))?;
        if total != expect {
            return Err(PluginError::internal(format!(
                "test_sink expected {expect} rows but received {total}"
            )));
        }
    }
    Ok(())
}
