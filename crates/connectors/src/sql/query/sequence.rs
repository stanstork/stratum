/// Definition for creating a sequence, used by the query generator.
#[derive(Debug, Clone)]
pub struct SequenceDef {
    pub name: String,
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    /// (table, column) that owns this sequence
    pub owned_by: Option<(String, String)>,
}
