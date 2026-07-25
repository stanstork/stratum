use model::core::value::Value;

/// Provides database-specific *text* encoding for COPY/LOAD style ingestion.
pub trait TextCopyEncoder {
    /// Encodes a concrete value into the backend's text representation.
    fn encode_value(&self, value: &Value) -> String;

    /// Encodes a SQL NULL into its text literal form (e.g. `\N`).
    fn encode_null(&self) -> String;

    /// Helper that encodes an optional value, delegating NULL handling.
    fn encode_optional(&self, value: Option<&Value>) -> String {
        match value {
            Some(v) => self.encode_value(v),
            None => self.encode_null(),
        }
    }
}
