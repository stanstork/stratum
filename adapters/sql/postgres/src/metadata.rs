use crate::data_type::PgColumnDataType;
use sql_adapter::metadata::column::data_type::ColumnDataType;
use sql_adapter::metadata::column::metadata::ColumnMetadata;
use sql_adapter::row::db_row::DbRow;
use sqlx::postgres::PgRow;

pub trait ColumnMetadataMapper {
    fn from_pg_row(row: &PgRow) -> ColumnMetadata;
}

impl ColumnMetadataMapper for ColumnMetadata {
    fn from_pg_row(row: &PgRow) -> ColumnMetadata {
        let data_type = ColumnDataType::from_pg_row(row);
        ColumnMetadata::from_row(&DbRow::PostgresRow(row), data_type)
    }
}
