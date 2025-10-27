use data_model::records::row_data::RowData;
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct TransformationRecord {
    pub input: RowData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<RowData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warnings: Option<Vec<String>>,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct TransformationReport {
    pub ok: usize,
    pub failed: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sample: Vec<TransformationRecord>,
}
