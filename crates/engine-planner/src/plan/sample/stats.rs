use serde::Serialize;

#[derive(Serialize, Debug, Clone, Default)]
pub struct SampleStats {
    pub ok: usize,
    pub warnings: usize,
    pub skipped: usize,
    pub errors: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub validation_stats: Vec<ValidationStats>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ColumnLocation {
    Source,
    Output,
    Lookup,
}

#[derive(Serialize, Debug, Clone)]
pub struct ValidationStats {
    pub name: String,
    pub passed: usize,
    pub failed: usize,
    /// Percentage of rows that passed (0.0 to 1.0)
    pub pass_rate: f32,
}
