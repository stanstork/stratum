/// Definition for creating a UNIQUE constraint via ALTER TABLE.
#[derive(Debug, Clone)]
pub struct UniqueConstraintDef {
    pub constraint_name: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
}

/// Definition for creating a CHECK constraint via ALTER TABLE.
#[derive(Debug, Clone)]
pub struct CheckConstraintDef {
    pub constraint_name: Option<String>,
    pub table: String,
    pub expression: String,
}
