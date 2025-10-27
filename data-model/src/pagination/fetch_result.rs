use crate::{pagination::cursor::Cursor, records::row_data::RowData};
use serde::{Deserialize, Serialize};

/// Result of a single fetch page.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchResult {
    /// Fetched rows from the source.
    pub rows: Vec<RowData>,

    /// Cursor that should be used to fetch the next page.
    /// `None` means no next page (end of source data).
    pub next_cursor: Option<Cursor>,

    /// Indicates whether the source was fully consumed.
    pub reached_end: bool,

    /// Number of rows fetched in this batch.
    pub row_count: usize,

    /// Total time spent fetching (ms).
    pub took_ms: u128,
}
