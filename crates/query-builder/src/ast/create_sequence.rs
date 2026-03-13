//! Defines the AST for a CREATE SEQUENCE statement.

/// Represents a complete CREATE SEQUENCE statement.
#[derive(Debug, Clone)]
pub struct CreateSequence {
    pub name: String,
    pub if_not_exists: bool,
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    /// (table, column) for OWNED BY clause (PostgreSQL)
    pub owned_by: Option<(String, String)>,
}
