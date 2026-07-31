/// Postgres: when to create the primary key - `"pre"` (default; PK present
/// during the load) or `"post"` (create the table without its PK and add it
/// after the bulk load, so index maintenance doesn't slow the COPY).
pub const PK_CREATION: &str = "pk_creation";
/// Postgres: COPY wire format (`"binary"` / `"text"`).
pub const COPY_FORMAT: &str = "copy_format";
/// Conflict handling - Postgres `ON CONFLICT`, MySQL `LOAD DATA` modifier.
pub const ON_CONFLICT: &str = "on_conflict";
