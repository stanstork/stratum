use crate::plan::define::env_vars::{EnvVarUsage, ValueSource};
use serde::Serialize;

#[derive(Serialize, Debug, Clone, Default)]
pub struct ResolvedDefines {
    pub constants: Vec<ResolvedConstant>,
    pub env_vars_used: Vec<EnvVarUsage>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ResolvedConstant {
    pub name: String,
    pub value: String,
    pub source: ValueSource,
}
