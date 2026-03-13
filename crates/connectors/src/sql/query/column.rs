use model::core::types::Type;

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: Type,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default: Option<String>,
    pub char_max_length: Option<usize>,
    pub generated_expression: Option<String>,
    pub is_stored: bool,
    pub is_generated: bool,
}

impl ColumnDef {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn from_computed(name: &str, data_type: &Type) -> Self {
        ColumnDef {
            name: name.to_string(),
            data_type: data_type.clone(),
            is_nullable: true,     // Computed considered nullable by default
            is_primary_key: false, // Computed columns cannot be primary keys
            default: None,         // Computed columns don't have default values
            char_max_length: None, // Char max length is not applicable for computed columns
            generated_expression: None,
            is_stored: false,
            is_generated: false,
        }
    }
}
