use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QualCol {
    pub table: String, // table or alias as used in FROM/JOINs
    pub column: String,
}

/// Represents the pagination cursor.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Cursor {
    /// No cursor specified.
    None,

    /// Default cursor with an offset.
    Default { offset: usize },

    /// Cursor for simple Primary Key offset (strictly increasing PK).
    Pk { pk_col: QualCol, id: u64 },

    /// Cursor for a single numeric column (append-only; not unique).
    /// NOTE: Without a tie-breaker this can be ambiguous; prefer CompositeNumPk.
    Numeric { col: QualCol, val: i128 },

    /// Cursor for a single timestamp column in microseconds (not unique).
    /// NOTE: Without a tie-breaker this can be ambiguous; prefer CompositeTsPk.
    Timestamp { col: QualCol, ts: i64 },

    /// Composite cursor for NUMERIC + PK (tie-break).
    CompositeNumPk {
        num_col: QualCol,
        pk_col: QualCol,
        val: i128, // numeric value
        id: u64,   // tie-breaker id
    },

    /// Composite cursor for TIMESTAMP (micros) + PK (tie-break).
    CompositeTsPk {
        ts_col: QualCol,
        pk_col: QualCol,
        ts: i64, // timestamp in microseconds
        id: u64, // tie-breaker id
    },
}
