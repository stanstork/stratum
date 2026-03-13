/// A single schema operation (DDL statement) to execute against the destination.
#[derive(Debug, Clone)]
pub struct SchemaOp {
    /// The SQL DDL statement to execute.
    pub sql: String,
    /// Human-readable description for logging.
    pub description: String,
    /// If true, "already exists" errors are silently ignored.
    pub idempotent: bool,
}

/// Collected schema operations split into pre-migration and post-migration phases.
///
/// Pre-migration ops run before data transfer (CREATE ENUM, CREATE TABLE, ADD COLUMN).
/// Post-migration ops run after data transfer (ALTER TABLE ADD CONSTRAINT for FKs, CREATE INDEX).
#[derive(Debug, Clone, Default)]
pub struct SchemaOps {
    pub pre: Vec<SchemaOp>,
    pub post: Vec<SchemaOp>,
}

impl SchemaOps {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.pre.is_empty() && self.post.is_empty()
    }

    /// Merge another `SchemaOps` into this one, appending both pre and post ops.
    pub fn merge(&mut self, other: SchemaOps) {
        self.pre.extend(other.pre);
        self.post.extend(other.post);
    }
}
