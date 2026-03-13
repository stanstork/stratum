#[derive(Debug, Clone, Default)]
pub struct Capabilities {
    /// Database version string
    pub version: String,

    /// Feature support
    pub transactions: bool,
    pub savepoints: bool,
    pub copy_protocol: bool,    // COPY (PostgreSQL) or LOAD DATA (MySQL)
    pub upsert: bool,           // INSERT ... ON CONFLICT / ON DUPLICATE KEY
    pub returning_clause: bool, // RETURNING (PostgreSQL)
    pub json_type: bool,
    pub jsonb_type: bool, // PostgreSQL only
    pub array_type: bool, // PostgreSQL only
    pub uuid_type: bool,
    pub geometry_type: bool, // PostGIS / MySQL spatial

    /// Limits
    pub max_parameters: Option<usize>,
    pub max_query_size: Option<usize>,
}
