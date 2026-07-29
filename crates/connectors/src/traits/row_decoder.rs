use model::{
    core::value::Value,
    records::{Record, RecordSchema},
};
use std::sync::Arc;

pub trait RowDecoder: Send + Sync {
    /// Build the shared column schema (names + types) for this result set.
    fn schema(&self, table: &str) -> Arc<RecordSchema>;

    /// Extract this row's values, aligned with `schema`'s columns.
    fn decode_with_schema(&self, schema: &Arc<RecordSchema>) -> Record;

    /// Decode a standalone row, building a fresh schema.
    fn decode(&self, table: &str) -> Record {
        let schema = self.schema(table);
        self.decode_with_schema(&schema)
    }

    fn columns(&self) -> Vec<String>;

    fn get_string(&self, column: &str) -> Option<String>;
    fn get_i32(&self, column: &str) -> Option<i32>;
    fn get_u32(&self, column: &str) -> Option<u32>;
    fn get_i64(&self, column: &str) -> Option<i64>;
    fn get_bool(&self, column: &str) -> Option<bool>;
    fn get_value(&self, column: &str) -> Option<Value>;
}
