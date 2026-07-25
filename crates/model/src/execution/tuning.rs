/// Postgres: drop the target's indexes/PK for the bulk load, rebuild after.
pub const DROP_INDEXES: &str = "drop_indexes";
/// Postgres: COPY wire format (`"binary"` / `"text"`).
pub const COPY_FORMAT: &str = "copy_format";
/// Conflict handling - Postgres `ON CONFLICT`, MySQL `LOAD DATA` modifier.
pub const ON_CONFLICT: &str = "on_conflict";
