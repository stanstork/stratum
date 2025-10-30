use crate::{
    file::csv::metadata::CsvColumnMetadata,
    sql::{base::metadata::column::ColumnMetadata, postgres::data_type::PgDataType},
};
use model::core::{data_type::DataType, value::Value};

#[derive(Debug, Clone)]
pub enum FieldMetadata {
    Sql(ColumnMetadata),
    Csv(CsvColumnMetadata),
}

impl FieldMetadata {
    pub fn pg_type(&self) -> (DataType, Option<usize>) {
        match self {
            FieldMetadata::Sql(col) => DataType::as_pg_type_info(col),
            FieldMetadata::Csv(col) => (col.data_type.clone(), None),
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

    pub fn is_primary_key(&self) -> bool {
        match self {
            FieldMetadata::Sql(col) => col.is_primary_key,
            FieldMetadata::Csv(col) => col.is_primary_key,
        }
    }

    pub fn default_value(&self) -> Option<Value> {
        match self {
            FieldMetadata::Sql(col) => col.default_value.clone(),
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
