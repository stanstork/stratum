use crate::pagination::cursor::QualCol;

#[derive(Debug, Clone)]
pub struct OffsetConfig {
    pub strategy: Option<String>,    // e.g., "pk", "timestamp"
    pub cursor: Option<QualCol>,     // e.g., "id" column for incremental fetch
    pub tiebreaker: Option<QualCol>, // required when cursor is not unique
    pub timezone: Option<String>,    // optional, for DATETIME <-> TIMESTAMP handling
}
