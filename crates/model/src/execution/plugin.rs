use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginDecl {
    pub name: String,
    pub path: PathBuf,
    pub allow_http: bool,
    pub allow_kv: bool,
    pub allow_log: bool, // default: true; the field exists so plugins can opt out
    pub allow_metrics: bool,
    pub allow_fs_read: Vec<PathBuf>,
    pub allow_fs_write: Vec<PathBuf>,
    pub allow_env: Vec<String>,
    pub memory_limit_bytes: Option<u64>,
    pub fuel_limit: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub config_json: Option<Vec<u8>>,
}
