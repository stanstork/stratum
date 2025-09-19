use crate::data_type::PgDataType;
use common::types::DataType;
use sql_adapter::{metadata::column::ColumnMetadata, row::DbRow};
use sqlx::postgres::PgRow;

pub trait ColumnMetadataMapper {
    fn from_pg_row(row: &PgRow) -> ColumnMetadata;
}

impl ColumnMetadataMapper for ColumnMetadata {
    fn from_pg_row(row: &PgRow) -> ColumnMetadata {
        let data_type = DataType::parse_from_row(row);
        ColumnMetadata::from_row(&DbRow::PostgresRow(row), data_type)
    }
}
