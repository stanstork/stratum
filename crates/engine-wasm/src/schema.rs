use crate::{error::WasmError, exchange::ExchangeFormat, runtime::limits::ResourceLimits};
use model::core::types::{FloatSize, IntSize, Type};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginType {
    Transform,
    Filter,
    Source,
    Sink,
}

/// How the plugin executes. Set by the SDK in its metadata; the host uses it to
/// size resource limits (a JS plugin boots QuickJS and needs far more fuel than
/// a native one). Absent in metadata => `Native`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginRuntime {
    #[default]
    Native,
    Js,
}

/// Schema declaration for a single plugin field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginField {
    pub name: String,
    /// Type as a string tag (e.g., "string", "i64", "f64", "bool", "decimal", "date", "timestamp", "bytes", "json", "uuid").
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub nullable: bool,
}

impl PluginField {
    /// Convert the string type tag to a canonical Type.
    pub fn to_canonical_type(&self) -> Type {
        match self.field_type.as_str() {
            "bool" | "boolean" => Type::Boolean,
            "i64" | "int" | "integer" => Type::Int {
                bits: IntSize::I64,
                unsigned: false,
                auto_increment: false,
            },
            "u64" | "uint" => Type::Int {
                bits: IntSize::I64,
                unsigned: true,
                auto_increment: false,
            },
            "f64" | "float" | "double" => Type::Float {
                bits: FloatSize::F64,
            },
            "decimal" => Type::Decimal {
                precision: None,
                scale: None,
            },
            "string" | "text" => Type::Text { charset: None },
            "bytes" | "binary" => Type::Blob { max_bytes: None },
            "date" => Type::Date,
            "timestamp" => Type::Timestamp {
                precision: None,
                with_tz: true,
            },
            "time" => Type::Time {
                precision: None,
                with_tz: false,
            },
            "json" => Type::Json { binary: false },
            "uuid" => Type::Uuid,
            _ => Type::Unknown {
                source_name: self.field_type.clone(),
                fallback_ddl: String::new(),
            },
        }
    }
}

/// Plugin metadata, loaded from __stratum_metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginMetadata {
    pub name: String,
    pub version: String,
    #[serde(rename = "type")]
    pub plugin_type: PluginType,
    #[serde(default = "default_exchange_format")]
    pub exchange_format: ExchangeFormat,
    /// Execution runtime (native vs JS). Defaults to `Native` when absent.
    #[serde(default)]
    pub runtime: PluginRuntime,
    /// Input fields (transform, filter, sink).
    #[serde(default)]
    pub input_schema: Vec<PluginField>,
    /// Output fields (source).
    #[serde(default)]
    pub output_schema: Vec<PluginField>,
    /// Output type (transform only). String tag like "f64", "string".
    #[serde(default)]
    pub output_type: Option<String>,
}

impl PluginMetadata {
    pub fn from_json(bytes: &[u8], plugin: &str) -> Result<Self, WasmError> {
        serde_json::from_slice(bytes).map_err(|e| WasmError::InvalidOutput {
            plugin: plugin.to_string(),
            reason: format!("invalid metadata JSON: {}", e),
        })
    }

    /// Resource limits appropriate for this plugin's runtime and role. JS
    /// plugins (QuickJS boot) and IO roles (source/sink) get the generous IO
    /// budget; native transform/filter get the lean row budget.
    pub fn suggested_limits(&self) -> ResourceLimits {
        let io_role = matches!(self.plugin_type, PluginType::Source | PluginType::Sink);
        if self.runtime == PluginRuntime::Js || io_role {
            ResourceLimits::for_io_plugins()
        } else {
            ResourceLimits::for_row_plugins()
        }
    }

    /// Get the canonical output Type for transform plugins.
    pub fn canonical_output_type(&self) -> Option<Type> {
        self.output_type.as_ref().map(|t| {
            let field = PluginField {
                name: "output".to_string(),
                field_type: t.clone(),
                nullable: false,
            };
            field.to_canonical_type()
        })
    }
}

fn default_exchange_format() -> ExchangeFormat {
    ExchangeFormat::JsonV1
}
