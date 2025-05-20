use common::types::DataType;
use csv::metadata::CsvColumnMetadata;
use postgres::data_type::PgDataType;
use sql_adapter::metadata::column::ColumnMetadata;

use super::entity::EntityMetadata;

#[derive(Debug, Clone)]
pub enum FieldMetadata {
    Sql(ColumnMetadata),
    Csv(CsvColumnMetadata),
}

impl FieldMetadata {
    pub fn pg_type(&self) -> (String, Option<usize>) {
        match self {
            FieldMetadata::Sql(col) => DataType::to_pg_type(col),
            FieldMetadata::Csv(col) => (col.data_type.to_string(), None),
        }
    }

    pub fn ordinal(&self) -> usize {
        match self {
            FieldMetadata::Sql(col) => col.ordinal,
            FieldMetadata::Csv(col) => col.ordinal,
        }
    }

    pub fn name(&self) -> String {
        match self {
            FieldMetadata::Sql(col) => col.name.clone(),
            FieldMetadata::Csv(col) => col.name.clone(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        match self {
            FieldMetadata::Sql(col) => col.is_nullable,
            FieldMetadata::Csv(col) => col.is_nullable,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            FieldMetadata::Sql(col) => col.data_type.clone(),
            FieldMetadata::Csv(col) => col.data_type.clone(),
        }
    }

    pub fn is_primary_key(&self, meta: &EntityMetadata) -> bool {
        match self {
            FieldMetadata::Sql(col) => {
                if let EntityMetadata::Table(table) = meta {
                    table.primary_keys.contains(&col.name)
                } else {
                    false
                }
            }
            FieldMetadata::Csv(_col) => false,
        }
    }

    pub fn default_value(&self) -> Option<String> {
        match self {
            FieldMetadata::Sql(col) => col.default_value.as_ref().map(ToString::to_string),
            FieldMetadata::Csv(_col) => None,
        }
    }

    pub fn is_valid(&self) -> bool {
        match self {
            FieldMetadata::Sql(_) => true,
            FieldMetadata::Csv(col) => !col.name.is_empty(),
        }
    }
}
