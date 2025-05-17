use crate::data_type::MySqlColumnDataType;
use async_trait::async_trait;
use common::{row_data::RowData, types::DataType};
use sql_adapter::{
    adapter::SqlAdapter,
    error::{adapter::ConnectorError, db::DbError},
    metadata::{
        column::{ColumnMetadata, COL_REFERENCING_TABLE},
        provider::MetadataProvider,
        table::TableMetadata,
    },
    query::builder::SqlQueryBuilder,
    requests::FetchRowsRequest,
    row::DbRow,
};
use sqlx::{MySql, Pool, Row};
use std::collections::HashMap;
use tracing::info;

#[derive(Clone)]
pub struct MySqlAdapter {
    pool: Pool<MySql>,
}

const QUERY_TABLE_EXISTS_SQL: &str = include_str!("../sql/table_exists.sql");
const QUERY_TRUNCATE_TABLE_SQL: &str = include_str!("../sql/table_truncate.sql");
const QUERY_TABLE_METADATA_SQL: &str = include_str!("../sql/table_metadata.sql");
const QUERY_TABLE_REFERENCING_SQL: &str = include_str!("../sql/table_referencing.sql");
const QUERY_COLUMN_TYPE_SQL: &str = include_str!("../sql/column_type.sql");

#[async_trait]
impl SqlAdapter for MySqlAdapter {
    async fn connect(url: &str) -> Result<Self, ConnectorError> {
        let pool = Pool::connect(url).await?;
        Ok(MySqlAdapter { pool })
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

    async fn fetch_metadata(&self, table: &str) -> Result<TableMetadata, DbError> {
        let rows = sqlx::query(QUERY_TABLE_METADATA_SQL)
            .bind(table)
            .bind(table)
            .bind(table)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;
        let columns = rows
            .iter()
            .map(|row| {
                let data_type = DataType::from_mysql_row(row);
                let column_metadata = ColumnMetadata::from_row(&DbRow::MySqlRow(row), data_type);
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

    async fn fetch_rows(&self, request: FetchRowsRequest) -> Result<Vec<RowData>, DbError> {
        let alias = request.alias.as_deref().unwrap_or(&request.table);
        let query = SqlQueryBuilder::new()
            .select(&request.columns)
            .from(&request.table, alias)
            .join(&request.joins)
            .where_clause(&request.filter)
            .limit(request.limit)
            .offset(request.offset.unwrap_or(0))
            .build();

        // Log the generated SQL query for debugging
        info!("Generated SQL query: {:#?}", query.0);

        // Execute the query and fetch the rows
        let rows = sqlx::query(&query.0).fetch_all(&self.pool).await?;
        let result = rows
            .into_iter()
            .map(|row| DbRow::MySqlRow(&row).get_row_data(&request.table))
            .collect();

        Ok(result)
    }

    async fn fetch_column_type(&self, table: &str, column: &str) -> Result<String, DbError> {
        let row = sqlx::query(QUERY_COLUMN_TYPE_SQL)
            .bind(table)
            .bind(column)
            .fetch_one(&self.pool)
            .await?;
        let data_type = row.try_get::<Vec<u8>, _>("column_type")?;
        String::from_utf8(data_type).map_err(|err| err.into())
    }
}
