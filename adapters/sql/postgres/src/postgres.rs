use async_trait::async_trait;
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::{
        column::{
            data_type::ColumnDataType,
            metadata::{ColumnMetadata, COL_REFERENCING_TABLE},
        },
        provider::MetadataProvider,
        table::TableMetadata,
    },
    query::loader::QueryLoader,
    requests::FetchRowsRequest,
    row::{db_row::DbRow, row_data::RowData},
};
use sqlx::{Pool, Postgres, Row};
use std::collections::HashMap;

use crate::data_type::PgColumnDataType;

#[derive(Clone)]
pub struct PgAdapter {
    pool: Pool<Postgres>,
}

const QUERY_TABLE_EXISTS: &str = "queries/pg/table_exists.sql";
const QUERY_TRUNCATE_TABLE: &str = "queries/pg/truncate_table.sql";
const QUERY_TABLE_METADATA: &str = "queries/pg/table_metadata.sql";
const QUERY_TABLE_REFERENCING: &str = "queries/pg/table_referencing.sql";
const QUERY_COLUMN_TYPE: &str = "queries/pg/column_type.sql";

#[async_trait]
impl SqlAdapter for PgAdapter {
    async fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let pool = Pool::connect(url).await?;
        Ok(PgAdapter { pool })
    }

    async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let query = QueryLoader::load_query(QUERY_TABLE_EXISTS)?;
        let row = sqlx::query(&query)
            .bind(table)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get(0))
    }

    async fn truncate_table(&self, table: &str) -> Result<(), Box<dyn std::error::Error>> {
        let query = QueryLoader::load_query(QUERY_TRUNCATE_TABLE)?;
        sqlx::query(&query).bind(table).execute(&self.pool).await?;
        Ok(())
    }

    async fn execute(&self, query: &str) -> Result<(), Box<dyn std::error::Error>> {
        sqlx::query(query).execute(&self.pool).await?;
        Ok(())
    }

    async fn fetch_metadata(
        &self,
        table: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        let query = QueryLoader::load_query(QUERY_TABLE_METADATA)?.replace("{table}", table);
        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;
        let columns = rows
            .iter()
            .map(|row| {
                let data_type = ColumnDataType::from_pg_row(row);
                let column_metadata = ColumnMetadata::from_row(&DbRow::PostgresRow(row), data_type);
                Ok((column_metadata.name.clone(), column_metadata))
            })
            .collect::<Result<HashMap<_, _>, Box<dyn std::error::Error>>>()?;

        MetadataProvider::build_table_metadata(table, columns)
    }

    async fn fetch_referencing_tables(
        &self,
        table: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let query = QueryLoader::load_query(QUERY_TABLE_REFERENCING)?;
        let rows = sqlx::query(&query)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        let tables = rows
            .iter()
            .map(|row| row.try_get::<String, _>(COL_REFERENCING_TABLE))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(tables)
    }

    async fn fetch_rows(
        &self,
        _request: FetchRowsRequest,
    ) -> Result<Vec<RowData>, Box<dyn std::error::Error>> {
        todo!("Implement fetch_all for Postgres")
    }

    async fn fetch_column_type(
        &self,
        table: &str,
        column: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let query = QueryLoader::load_query(QUERY_COLUMN_TYPE)?;
        let row = sqlx::query(&query)
            .bind(table)
            .bind(column)
            .fetch_one(&self.pool)
            .await?;
        let data_type = row.try_get::<String, _>("column_type")?;
        Ok(data_type)
    }
}
