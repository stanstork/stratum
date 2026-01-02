use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ValueSource {
    /// Literal value in SMQL config
    Literal,

    /// From env("VAR") - required
    Environment { var_name: String },

    /// From env("VAR", default) - with fallback
    EnvironmentWithDefault { var_name: String, default: String },
}

#[derive(Serialize, Debug, Clone)]
pub struct EnvVarUsage {
    pub var_name: String,

    /// Whether the environment variable was set at plan time
    pub was_set: bool,

    /// Whether the default value was used (from env("VAR", default))
    pub used_default: bool,

    /// Masked value for display (e.g., "abc***" for secrets)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}
