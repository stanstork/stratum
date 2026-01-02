use crate::plan::hooks::impact::HookImpact;
use serde::Serialize;

#[derive(Serialize, Debug, Clone, Default)]
pub struct HooksPlan {
    /// SQL statements to execute before pipeline
    pub before: Vec<HookStatement>,

    /// SQL statements to execute after pipeline (only if pipeline succeeds)
    pub after: Vec<HookStatement>,

    pub before_count: usize,
    pub after_count: usize,
}

#[derive(Serialize, Debug, Clone)]
pub struct HookStatement {
    pub sql: String,

    /// Connection where this SQL will execute
    pub connection: String,

    /// Analysis of what this SQL does (DDL, DML, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub impact: Option<HookImpact>,

    /// Warnings about potential issues with this SQL
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}
