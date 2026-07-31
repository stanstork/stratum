use crate::ast::common::TableRef;

#[derive(Debug, Clone)]
pub struct LoadData {
    pub table: TableRef,
    pub columns: Vec<String>,
    pub local: bool,       // LOCAL INFILE (client-side) or INFILE (server-side)
    pub file_name: String, // dummy name for the local handler
    /// Conflict handling for rows that collide on a primary/unique key.
    pub on_conflict: LoadDataConflict,
    pub fields_terminated_by: String, // inner literal, e.g. r"\t"
    pub fields_escaped_by: String,    // inner literal, e.g. r"\\"
    pub lines_terminated_by: String,  // inner literal, e.g. r"\n"
}

/// How `LOAD DATA` resolves rows that collide on a primary/unique key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LoadDataConflict {
    /// Emit no modifier. For `LOCAL INFILE` the server implicitly treats this as
    /// `IGNORE` (colliding rows are skipped); server-side `INFILE` errors instead.
    #[default]
    Default,
    /// `REPLACE` - overwrite existing rows that collide on a key.
    Replace,
    /// `IGNORE` - explicitly skip colliding rows.
    Ignore,
}

impl LoadDataConflict {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "default" => Some(Self::Default),
            "replace" => Some(Self::Replace),
            "ignore" => Some(Self::Ignore),
            _ => None,
        }
    }
}
