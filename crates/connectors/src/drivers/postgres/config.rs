/// Which COPY wire format the driver uses for bulk writes. `Binary` really means
/// "prefer binary": it is type-exact and ~2x cheaper to encode, but requires
/// every column to be binary-encodable, so `copy_rows` transparently falls back
/// to the text path per batch when a value can't be encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CopyFormat {
    /// Prefer binary COPY, falling back to text when a value can't be encoded.
    #[default]
    Binary,
    /// Always use the CSV text COPY path.
    Text,
}

impl CopyFormat {
    /// Parse a config value (`"binary"`, `"text"`/`"csv"`).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "binary" => Some(Self::Binary),
            "text" | "csv" => Some(Self::Text),
            _ => None,
        }
    }
}

/// How a Postgres bulk write resolves rows that collide on the primary key.
///
/// Resolving a conflict requires a key to conflict *on*, so a table without a primary key
/// falls back to a plain `COPY` (the only thing possible) rather than failing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgConflictAction {
    /// `ON CONFLICT … DO NOTHING` - skip colliding rows.
    DoNothing,
    /// `ON CONFLICT … DO UPDATE` - overwrite colliding rows (upsert).
    DoUpdate,
}

impl PgConflictAction {
    /// Parse a config value (`"do_nothing"`, `"do_update"`).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "do_nothing" | "nothing" | "skip" => Some(Self::DoNothing),
            "do_update" | "update" | "upsert" => Some(Self::DoUpdate),
            _ => None,
        }
    }
}
