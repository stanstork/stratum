use crate::plan::sample::{
    issue::SampleIssue, method::SamplingMethod, row::SampleRow, stats::SampleStats,
};
use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct SampleQuery {
    pub sql: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<String>,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct SampleDataPreview {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampled_at: Option<DateTime<Utc>>,
    pub sample_size: usize,
    pub sampling_method: SamplingMethod,
    /// How long the sampling took (milliseconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<SampleQuery>,
    pub rows: Vec<SampleRow>,
    pub stats: SampleStats,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<SampleIssue>,
}
