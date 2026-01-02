use crate::plan::transform::type_conversion::TypeConversion;
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct ColumnMapping {
    /// Target column name
    pub target: String,

    /// Source expression
    pub source: MappingSource,

    pub mapping_type: MappingType,

    /// Data types
    pub source_type: Option<String>,
    pub target_type: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_conversion: Option<TypeConversion>,
    pub nullable: bool,
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MappingSource {
    /// Direct: `id = orders.id`
    Column { table: String, column: String },

    /// Renamed: `customer_name = users.name`
    Renamed {
        table: String,
        column: String,
        original_name: String,
    },

    /// Expression: `order_tax = orders.total * 1.4`
    Expression {
        expression: String,
        columns_referenced: Vec<String>,
        functions_used: Vec<String>,
    },

    /// Conditional: `when { ... }`
    Conditional {
        branches: Vec<ConditionalBranch>,
        else_value: Option<String>,
        sql_preview: String,
    },

    /// Lookup: `customer_email = users.email`
    Lookup { join_alias: String, column: String },

    /// Function: `synced_at = now()`
    Function { name: String, args: Vec<String> },

    /// Constant: `status = "active"`
    Constant { value: String, value_type: String },
}

#[derive(Serialize, Debug, Clone)]
pub struct ConditionalBranch {
    pub condition: String,
    pub value: String,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MappingType {
    Direct,
    Renamed,
    Computed,
    Conditional,
    Lookup,
    Generated,
    Constant,
}
