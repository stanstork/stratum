use common::name_map::NameMap;
use sql_adapter::metadata::{column::data_type::ColumnDataType, table::TableMetadata};
use std::collections::HashSet;
use tracing::error;

pub struct SchemaValidator<'a> {
    source_metadata: &'a TableMetadata,
    destination_metadata: &'a TableMetadata,
}

pub enum SchemaValidationMode<'a> {
    OneToOne,
    ContainsColumns(&'a [String]),
}

#[derive(Debug)]
struct InvalidColumn {
    table: String,
    column: String,
    source_type: ColumnDataType,
    destination_type: Option<ColumnDataType>, // None if column is missing
}

impl<'a> SchemaValidator<'a> {
    pub fn new(
        source_metadata: &'a TableMetadata,
        destination_metadata: &'a TableMetadata,
    ) -> Self {
        SchemaValidator {
            source_metadata,
            destination_metadata,
        }
    }

    pub fn validate(
        &self,
        mode: SchemaValidationMode,
        table_mapping: NameMap,
        column_mapping: NameMap,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match mode {
            SchemaValidationMode::OneToOne => {
                self.validate_one_to_one(table_mapping, column_mapping)?;
            }
            SchemaValidationMode::ContainsColumns(columns) => {
                self.validate_contains_columns(columns)?;
            }
        }

        Ok(())
    }

    fn validate_one_to_one(
        &self,
        table_mapping: NameMap,
        column_mapping: NameMap,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let source_tables = self.source_metadata.tables();
        let destination_tables = self.destination_metadata.tables();

        let mut invalid_columns = Vec::new();

        if source_tables.len() != destination_tables.len() {
            return Err(format!(
                "Table count mismatch: source has {}, destination has {}",
                source_tables.len(),
                destination_tables.len()
            )
            .into());
        }

        for source_table in &source_tables {
            let dest_table_name = table_mapping.resolve(&source_table.name);
            let destination_table = destination_tables
                .iter()
                .find(|t| t.name == *dest_table_name)
                .ok_or_else(|| {
                    let msg = format!("Destination table `{}` not found", dest_table_name);
                    error!("{}", msg);
                    msg
                })?;

            if source_table.columns.len() != destination_table.columns.len() {
                return Err(format!(
                    "Column count mismatch in table `{}`: source has {}, destination has {}",
                    source_table.name,
                    source_table.columns.len(),
                    destination_table.columns.len()
                )
                .into());
            }

            for (col_name, src_col_meta) in &source_table.columns {
                let mapped_col_name = column_mapping.resolve(col_name).to_ascii_lowercase();
                match destination_table.columns.get(&mapped_col_name) {
                    Some(dst_col_meta) => {
                        if !src_col_meta
                            .data_type
                            .is_compatible(&dst_col_meta.data_type)
                        {
                            invalid_columns.push(InvalidColumn {
                                table: source_table.name.clone(),
                                column: mapped_col_name.clone(),
                                source_type: src_col_meta.data_type,
                                destination_type: Some(dst_col_meta.data_type),
                            });
                        }
                    }
                    None => {
                        invalid_columns.push(InvalidColumn {
                            table: source_table.name.clone(),
                            column: mapped_col_name.clone(),
                            source_type: src_col_meta.data_type,
                            destination_type: None,
                        });
                    }
                }
            }
        }

        if invalid_columns.is_empty() {
            Ok(())
        } else {
            for col in &invalid_columns {
                match &col.destination_type {
                    Some(dest_type) => error!(
                        "Column `{}` in table `{}` has mismatched types: source `{:?}`, destination `{:?}`",
                        col.column, col.table, col.source_type, dest_type
                    ),
                    None => error!(
                        "Column `{}` in table `{}` is missing in destination (source type: `{}`)",
                        col.column, col.table, col.source_type
                    ),
                }
            }

            Err(format!("Found {} invalid column(s)", invalid_columns.len()).into())
        }
    }

    fn validate_contains_columns(
        &self,
        columns: &[String],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let required_set: HashSet<_> = columns.iter().collect();
        let destination_set: HashSet<_> = self.destination_metadata.columns.keys().collect();

        if required_set.is_subset(&destination_set) {
            Ok(())
        } else {
            Err("Destination schema is missing required columns".into())
        }
    }
}
