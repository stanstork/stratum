use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct SampleIssue {
    pub level: SampleIssueLevel,
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SampleIssueLevel {
    Failed,
    Warning,
    Info,
}
