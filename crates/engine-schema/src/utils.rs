use connectors::sql::{metadata::column::ColumnMetadata, query::column::ColumnDef};
use model::core::types::Type;

pub fn create_column_def<T: Fn(&ColumnMetadata) -> (Type, Option<usize>)>(
    name: &str,
    type_converter: &T,
    metadata: &ColumnMetadata,
) -> ColumnDef {
    let (data_type, char_max_length) = type_converter(metadata);
    ColumnDef {
        name: name.to_string(),
        data_type: data_type.clone(),
        is_nullable: metadata.is_nullable,
        is_primary_key: metadata.is_primary_key,
        default: metadata.default_value.clone(),
        char_max_length,
        generated_expression: metadata.generated_expression.clone(),
        is_stored: metadata.is_stored,
        is_generated: metadata.is_generated,
    }
}
