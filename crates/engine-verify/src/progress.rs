use model::integrity::result::VerificationResult;

/// Observes verification as it happens, table by table, so a caller can render
/// live progress instead of waiting for the whole run to finish.
pub trait VerifyProgress: Send {
    /// A table with a receipt is about to be read back and diffed.
    fn table_started(&mut self, pipeline: &str, table: &str);

    /// A named sub-phase within the current table began, e.g. "reading
    /// destination", "sorting row hashes", "comparing".
    fn table_phase(&mut self, phase: &str) {
        let _ = phase;
    }

    /// A table's verification produced `result`.
    fn table_finished(&mut self, result: &VerificationResult);
}

/// A progress sink that does nothing, for callers that only want the results.
pub struct NoopProgress;

impl VerifyProgress for NoopProgress {
    fn table_started(&mut self, _pipeline: &str, _table: &str) {}
    fn table_finished(&mut self, _result: &VerificationResult) {}
}
