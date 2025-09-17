use crate::{bind_values, data_type::PgDataType};
use async_trait::async_trait;
use common::{row_data::RowData, types::DataType, value::Value};
use query_builder::dialect;
use sql_adapter::{
    adapter::SqlAdapter,
    error::{adapter::ConnectorError, db::DbError},
    metadata::{
        column::{ColumnMetadata, COL_REFERENCING_TABLE},
        provider::MetadataProvider,
        table::TableMetadata,
    },
    query::generator::QueryGenerator,
    requests::FetchRowsRequest,
    row::DbRow,
};
use sqlx::{Pool, Postgres, Row};
use std::collections::HashMap;

#[derive(Clone)]
pub struct PgAdapter {
    pool: Pool<Postgres>,
}

const QUERY_TABLE_EXISTS_SQL: &str = include_str!("../sql/table_exists.sql");
const QUERY_TRUNCATE_TABLE_SQL: &str = include_str!("../sql/table_truncate.sql");
const QUERY_TABLE_METADATA_SQL: &str = include_str!("../sql/table_metadata.sql");
const QUERY_TABLE_REFERENCING_SQL: &str = include_str!("../sql/table_referencing.sql");

#[async_trait]
impl SqlAdapter for PgAdapter {
    async fn connect(url: &str) -> Result<Self, ConnectorError> {
        let pool = Pool::connect(url).await?;
        Ok(PgAdapter { pool })
    }

    async fn table_exists(&self, table: &str) -> Result<bool, DbError> {
        let row = sqlx::query(QUERY_TABLE_EXISTS_SQL)
            .bind(table)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get(0))
    }

    async fn truncate_table(&self, table: &str) -> Result<(), DbError> {
        sqlx::query(QUERY_TRUNCATE_TABLE_SQL)
            .bind(table)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn execute(&self, query: &str) -> Result<(), DbError> {
        sqlx::query(query).execute(&self.pool).await?;
        Ok(())
    }

    async fn execute_with_params(&self, query: &str, params: Vec<Value>) -> Result<(), DbError> {
        let query = sqlx::query(query);
        let bound_query = bind_values(query, &params);
        bound_query.execute(&self.pool).await?;
        Ok(())
    }

    async fn fetch_metadata(&self, table: &str) -> Result<TableMetadata, DbError> {
        let query = QUERY_TABLE_METADATA_SQL.replace("{table}", table);
        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;
        let columns = rows
            .iter()
            .map(|row| {
                let data_type = DataType::from_pg_row(row);
                let column_metadata = ColumnMetadata::from_row(&DbRow::PostgresRow(row), data_type);
                Ok((column_metadata.name.clone(), column_metadata))
            })
            .collect::<Result<HashMap<_, _>, DbError>>()?;

        MetadataProvider::construct_table_metadata(table, columns)
    }

    async fn fetch_referencing_tables(&self, table: &str) -> Result<Vec<String>, DbError> {
        let rows = sqlx::query(QUERY_TABLE_REFERENCING_SQL)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        let tables = rows
            .iter()
            .map(|row| row.try_get::<String, _>(COL_REFERENCING_TABLE))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(tables)
    }

    async fn find_existing_keys(
        &self,
        table: &str,
        key_columns: &[String],
        keys_batch: &[Vec<Value>],
    ) -> Result<Vec<RowData>, DbError> {
        let dialect = dialect::Postgres;
        let generator = QueryGenerator::new(&dialect);
        let sql = generator.key_existence(table, key_columns, keys_batch.len());

        let mut query = sqlx::query(&sql);
        for key_value in keys_batch {
            query = bind_values(query, key_value);
        }

        let rows = query.fetch_all(&self.pool).await?;
        let result = rows
            .into_iter()
            .map(|row| DbRow::PostgresRow(&row).get_row_data(table))
            .collect();

        Ok(result)
    }

    async fn fetch_rows(&self, _request: FetchRowsRequest) -> Result<Vec<RowData>, DbError> {
        todo!("Implement fetch_all for Postgres")
    }

    async fn fetch_column_type(&self, _table: &str, _column: &str) -> Result<String, DbError> {
        todo!("Implement fetch_column_type for Postgres");
    }

    async fn list_tables(&self) -> Result<Vec<String>, DbError> {
        todo!("Implement list_tables for Postgres");
    }
}
