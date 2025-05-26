use crate::metadata::field::FieldMetadata;
use sql_adapter::query::column::ColumnDef;

pub fn create_column_def<T: Fn(&FieldMetadata) -> (String, Option<usize>)>(
    name: &str,
    type_converter: &T,
    metadata: &FieldMetadata,
) -> ColumnDef {
    let (data_type, char_max_length) = type_converter(metadata);
    ColumnDef {
        name: name.to_string(),
        data_type: data_type.to_string(),
        is_nullable: metadata.is_nullable(),
        is_primary_key: metadata.is_primary_key(),
        default: metadata.default_value(),
        char_max_length,
    }
}
