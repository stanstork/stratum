use crate::{destination::Destination, report::finding::Finding};
use common::{
    mapping::EntityMapping,
    row_data::RowData,
    value::{FieldValue, Value},
};
use smql::statements::setting::Settings;
use sql_adapter::{
    error::db::DbError,
    metadata::{provider::MetadataProvider, table::TableMetadata},
};
use std::collections::{HashMap, HashSet};

pub struct DestinationSchemaValidator {
    mapping: EntityMapping,
    schemas: HashMap<String, TableMetadata>,
    findings: HashSet<Finding>,
    create_missing_tables: bool,
    create_missing_columns: bool,
}

impl DestinationSchemaValidator {
    pub async fn new(
        destination: &Destination,
        mapping: EntityMapping,
        settings: &Settings,
    ) -> Result<Self, DbError> {
        let adapter = destination.data_dest.adapter().await;
        let tables = [destination.name()];
        let meta_graph = MetadataProvider::build_metadata_graph(&*adapter, &tables).await?;

        Ok(Self {
            mapping,
            schemas: meta_graph,
            findings: HashSet::new(),
            create_missing_tables: settings.create_missing_tables,
            create_missing_columns: settings.create_missing_columns,
        })
    }

    pub fn validate(&mut self, row: &RowData) {
        let table_name = &row.entity;

        if let Some(table_metadata) = self.schemas.get(table_name) {
            let row_field_names: HashSet<_> = row.field_values.iter().map(|f| &f.name).collect();

            // Check fields present in the row against the schema
            for field in &row.field_values {
                let field_name = &field.name;
                let transformed_type = &field.data_type;

                match table_metadata.columns.get(field_name) {
                    Some(column_metadata) => {
                        let destination_type = &column_metadata.data_type;
                        if !destination_type.is_compatible(transformed_type) {
                            // Type Mismatch Finding
                            self.findings.insert(Finding::error(
                                "SCHEMA_TYPE_MISMATCH",
                                &format!(
                                    "Type mismatch for column '{}' in table '{}'. Transformed data has type {:?}, but destination expects {:?}.",
                                    field_name, table_name, transformed_type, destination_type
                                ),
                            ));
                        }

                        // Null value for a non-nullable column
                        if !column_metadata.is_nullable && field.value.is_none() {
                            self.findings.insert(Finding::error(
                                "SCHEMA_NULL_VIOLATION",
                                &format!(
                                    "Field '{}' in table '{}' is null, but the destination column is not nullable.",
                                    field_name, table_name
                                ),
                            ));
                        }

                        // Truncation Risk for character types
                        if let Some(max_len) = column_metadata.char_max_length {
                            if let Some(actual_len) = self.get_field_value_length(field) {
                                if actual_len > max_len {
                                    self.findings.insert(Finding::warning(
                                        "SCHEMA_TRUNCATION_RISK",
                                        &format!(
                                            "Data for column '{}' in table '{}' has length {} which exceeds the destination column's limit of {}. Data may be truncated.",
                                            field_name, table_name, actual_len, max_len
                                        ),
                                    ));
                                }
                            }
                        }

                        // TODO: Add more checks (e.g., numeric precision, enum values, etc.
                    }
                    None => {
                        // Column doesn't exist. Check if we are allowed to create it.
                        if self.create_missing_columns {
                            // If allowed, only create a finding if it's NOT a planned new column.
                            let is_computed = self
                                .mapping
                                .field_mappings
                                .computed_fields
                                .get(table_name)
                                .map_or(false, |computed_list| {
                                    computed_list.iter().any(|cf| &cf.name == field_name)
                                });

                            let is_renamed_target = self
                                .mapping
                                .field_mappings
                                .column_mappings
                                .get(table_name)
                                .map_or(false, |name_map| {
                                    name_map
                                        .source_to_target
                                        .values()
                                        .any(|target_name| target_name == field_name)
                                });

                            if !is_computed && !is_renamed_target {
                                self.findings.insert(Finding::error(
                                    "SCHEMA_COLUMN_MISSING",
                                    &format!(
                                        "Transformed data contains column '{}' which does not exist in destination table '{}' and is not a mapped or computed field.",
                                        field_name, table_name
                                    ),
                                ));
                            }
                        } else {
                            // If not allowed to create columns, it's always an error.
                            self.findings.insert(Finding::error(
                                "SCHEMA_COLUMN_MISSING",
                                &format!(
                                    "Transformed data contains column '{}' which does not exist in destination table '{}'. `create_missing_columns` is false.",
                                    field_name, table_name
                                ),
                            ));
                        }
                    }
                }
            }

            // Check for missing required columns in the row
            for (column_name, column_metadata) in &table_metadata.columns {
                // A column is required if it's not nullable AND it doesn't have a default value.
                if !column_metadata.is_nullable && !column_metadata.has_default {
                    if !row_field_names.contains(column_name) {
                        self.findings.insert(Finding::error(
                            "SCHEMA_MISSING_REQUIRED_COLUMN",
                            &format!(
                                "Required column '{}' is missing from the transformed data for table '{}'.",
                                column_name, table_name
                            ),
                        ));
                    }
                }
            }
        } else {
            // If the table doesn't exist in the destination, we can't validate.
            // This might be expected if `create_missing_tables` is true.
            // For now, we'll just return no findings for this case.
        }
    }

    pub fn findings(&self) -> Vec<Finding> {
        self.findings.iter().cloned().collect()
    }

    fn get_field_value_length(&self, field: &FieldValue) -> Option<usize> {
        match &field.value {
            Some(Value::String(s)) => Some(s.len()),
            _ => None, // Not a string, so no length check is applicable
        }
    }
}
