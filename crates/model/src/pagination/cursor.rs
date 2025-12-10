use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QualCol {
    pub table: String, // table or alias as used in FROM/JOINs
    pub column: String,
}

impl FromStr for QualCol {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() == 2 {
            Ok(QualCol {
                table: parts[0].to_string(),
                column: parts[1].to_string(),
            })
        } else {
            Ok(QualCol {
                table: "".to_string(),
                column: s.to_string(),
            })
        }
    }
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
