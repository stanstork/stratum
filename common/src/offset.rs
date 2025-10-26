pub struct OffsetConfig {
    pub cursor: Option<String>,     // e.g., "id" column for incremental fetch
    pub tiebreaker: Option<String>, // required when cursor is not unique
    pub timezone: Option<String>,   // optional, for DATETIME <-> TIMESTAMP handling
}
