use crate::{
    error::DriverError,
    sql::{filter::SqlFilter, metadata::table::TableMetadata, request::FetchRowsRequest},
    traits::driver::Driver,
};
use async_trait::async_trait;
use model::{core::value::Value, records::Record};
use query_builder::dialect::Dialect;

#[async_trait]
pub trait DataReader: Driver {
    async fn fetch(&self, request: FetchRowsRequest) -> Result<Vec<Record>, DriverError>;
    async fn count(
        &self,
        table: &str,
        schema: Option<&str>,
        filter: Option<&SqlFilter>,
    ) -> Result<u64, DriverError>;
    async fn count_fast(&self, table: &str) -> Result<u64, DriverError>;

    /// If `table` has exactly one integer primary key, returns `(pk_column, min,
    /// max)` so a scan can be split into parallel range lanes. `None` when the
    /// key isn't a single integer or the table is empty.
    async fn int_key_range(&self, table: &str) -> Result<Option<(String, u64, u64)>, DriverError>;
}

/// The lone integer primary key of `table`, if it has exactly one and it is an
/// integer in this `dialect`. Used to decide whether range-lane splitting
/// applies before running the min/max probe.
pub(crate) fn single_int_pk(meta: &TableMetadata, dialect: &dyn Dialect) -> Option<String> {
    if meta.primary_keys.len() != 1 {
        return None;
    }

    let pk = &meta.primary_keys[0];
    let column = meta.columns.get(pk)?;

    dialect
        .is_integer_type(&column.data_type)
        .then(|| pk.clone())
}

/// Build the `(pk, lo, hi)` range from a `select_key_range` result row, keeping
/// it only when non-empty and well-ordered (`lo <= hi`).
pub(crate) fn key_range_from_rows(pk: String, rows: &[Record]) -> Option<(String, u64, u64)> {
    let row = rows.first()?;
    let lo = as_u64(&row.get_value("lo"))?;
    let hi = as_u64(&row.get_value("hi"))?;

    (lo <= hi).then_some((pk, lo, hi))
}

/// Coerce a min/max scalar to `u64` (lane keys are non-negative).
fn as_u64(v: &Value) -> Option<u64> {
    match v {
        Value::UInt(u) => Some(*u),
        Value::Int(i) if *i >= 0 => Some(*i as u64),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}
