use serde::{Deserialize, Serialize};

/// Represents the pagination cursor.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Cursor {
    /// No cursor, fetch the first page (unbounded lower range).
    None,

    /// Cursor for simple Primary Key offset (strictly increasing PK).
    Pk { pk_col: String, id: u64 },

    /// Cursor for a single numeric column (append-only; not unique).
    /// NOTE: Without a tie-breaker this can be ambiguous; prefer CompositeNumPk.
    Numeric { col: String, val: i128 },

    /// Cursor for a single timestamp column in microseconds (not unique).
    /// NOTE: Without a tie-breaker this can be ambiguous; prefer CompositeTsPk.
    Timestamp { col: String, ts: i64 },

    /// Composite cursor for NUMERIC + PK (tie-break).
    CompositeNumPk {
        num_col: String,
        pk_col: String,
        val: i128, // numeric value
        id: u64,   // tie-breaker id
    },

    /// Composite cursor for TIMESTAMP (micros) + PK (tie-break).
    CompositeTsPk {
        ts_col: String,
        pk_col: String,
        ts: i64, // timestamp in micros
        id: u64, // tie-breaker id
    },
}
