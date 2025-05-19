use crate::metadata::column::ColumnMetadata;

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default: Option<String>,
    pub char_max_length: Option<usize>,
}

impl ColumnDef {
    pub fn new(metadata: &ColumnMetadata) -> Self {
        Self {
            name: metadata.name.clone(),
            data_type: metadata.data_type.to_string(),
            is_nullable: metadata.is_nullable,
            is_primary_key: metadata.is_primary_key,
            default: metadata.default_value.as_ref().map(|v| v.to_string()),
            char_max_length: metadata.char_max_length,
        }
    }

    pub fn from_computed(name: &str, data_type: &str) -> Self {
        Self {
            name: name.to_string(),
            data_type: data_type.to_string(),
            is_nullable: true, // Assuming computed fields are nullable
            is_primary_key: false,
            default: None,
            char_max_length: None,
        }
    }

    pub fn with_type_convertor<T: Fn(&ColumnMetadata) -> (String, Option<usize>)>(
        name: &str,
        type_converter: &T,
        metadata: &ColumnMetadata,
    ) -> Self {
        let (data_type, char_max_length) = type_converter(metadata);
        Self {
            name: name.to_string(),
            data_type: data_type.to_string(),
            is_nullable: metadata.is_nullable,
            is_primary_key: metadata.is_primary_key,
            default: metadata.default_value.as_ref().map(|v| v.to_string()),
            char_max_length,
        }
    }

    pub fn set_name(mut self, name: &str) -> Self {
        self.name = name.to_owned();
        self
    }

    pub fn is_array(&self) -> bool {
        self.data_type.eq_ignore_ascii_case("ARRAY")
    }
}
