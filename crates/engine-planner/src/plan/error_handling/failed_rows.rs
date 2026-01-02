use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FailedRowsConfig {
    Table {
        connection: String,
        table: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        schema: Option<String>,
    },
    File {
        path: String,
        format: FailedRowsFormat,
    },
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FailedRowsFormat {
    Jsonl,
    Csv,
    Parquet,
}
