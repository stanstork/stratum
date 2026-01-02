use serde::Serialize;

#[derive(Serialize, Debug, Clone, Default)]
pub struct PoolConfig {
    pub max_size: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_timeout: Option<String>,
}
