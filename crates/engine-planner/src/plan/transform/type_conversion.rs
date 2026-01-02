use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct TypeConversion {
    pub from_type: String,
    pub to_type: String,
    /// Whether conversion is safe (no data loss)
    pub is_safe: bool,
    /// Warning message if conversion is unsafe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
    pub conversion_method: ConversionMethod,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ConversionMethod {
    None,
    Implicit,
    Explicit,
    Function { name: String },
}
