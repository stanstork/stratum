use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct FilterPlan {
    pub name: String,
    pub expression: String,
    pub sql_preview: String,
    pub selectivity: FilterSelectivity,
    pub columns_referenced: Vec<String>,

    /// Whether this filter can use an index (faster execution)
    pub uses_index: bool,
}

#[derive(Serialize, Debug, Clone)]
pub struct FilterSelectivity {
    pub selectivity: f32,
    pub is_estimated: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
}
