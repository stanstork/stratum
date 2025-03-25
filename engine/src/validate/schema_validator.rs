use sql_adapter::metadata::table::TableMetadata;
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

    pub fn validate(&self, mode: SchemaValidationMode) -> Result<(), Box<dyn std::error::Error>> {
        match mode {
            SchemaValidationMode::OneToOne => {
                self.validate_one_to_one()?;
            }
            SchemaValidationMode::ContainsColumns(columns) => {
                self.validate_contains_columns(columns)?;
            }
        }

        Ok(())
    }

    fn validate_one_to_one(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.source_metadata.columns.len() != self.destination_metadata.columns.len() {
            return Err("Source and destination column count mismatch".into());
        }

        for (source_column, source_column_metadata) in &self.source_metadata.columns {
            if let Some(destination_column_metadata) =
                self.destination_metadata.columns.get(source_column)
            {
                if source_column_metadata.data_type != destination_column_metadata.data_type {
                    error!("Trying to copy data from column {} with data type {} to column {} with data type {}", source_column, source_column_metadata.data_type, source_column, destination_column_metadata.data_type);
                    return Err("Source and destination column data type mismatch".into());
                }
            } else {
                return Err("Destination column missing".into());
            }
        }

        Ok(())
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
