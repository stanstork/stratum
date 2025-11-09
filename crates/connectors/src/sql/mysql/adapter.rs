use crate::sql::base::probe::CapabilityProbe;
use crate::sql::mysql::params::MySqlParamStore;
use crate::sql::{
    base::{
        adapter::{DatabaseKind, SqlAdapter},
        capabilities::DbCapabilities,
        error::{ConnectorError, DbError},
        metadata::{
            column::{COL_REFERENCING_TABLE, ColumnMetadata},
            provider::MetadataProvider,
            table::TableMetadata,
        },
        query::generator::QueryGenerator,
        requests::FetchRowsRequest,
        row::DbRow,
    },
    mysql::{data_type::MySqlColumnDataType, probe::MySqlCapabilityProbe},
};
use async_trait::async_trait;
use model::{
    core::{data_type::DataType, value::Value},
    records::row::RowData,
};
use mysql_async::{Pool, Row as MySqlRow, prelude::Queryable};
use planner::query::dialect;
use std::collections::HashMap;
use tracing::info;

#[derive(Clone)]
pub struct MySqlAdapter {
    pool: Pool,
    dialect: dialect::MySql,
}

const QUERY_TABLE_EXISTS_SQL: &str = include_str!("sql/table_exists.sql");
const QUERY_TRUNCATE_TABLE_SQL: &str = include_str!("sql/table_truncate.sql");
const QUERY_TABLE_METADATA_SQL: &str = include_str!("sql/table_metadata.sql");
const QUERY_TABLE_REFERENCING_SQL: &str = include_str!("sql/table_referencing.sql");
const QUERY_COLUMN_TYPE_SQL: &str = include_str!("sql/column_type.sql");

#[async_trait]
impl SqlAdapter for MySqlAdapter {
    async fn connect(url: &str) -> Result<Self, ConnectorError> {
        let pool = Pool::from_url(url)?;
        Ok(MySqlAdapter {
            pool,
            dialect: dialect::MySql,
        })
    }

    async fn exec(&self, query: &str) -> Result<(), DbError> {
        let mut conn = self.pool.get_conn().await?;
        conn.query_drop(query).await?;
        Ok(())
    }

    async fn exec_params(&self, query: &str, params: Vec<Value>) -> Result<(), DbError> {
        let params = MySqlParamStore::from_values(&params).params();
        let mut conn = self.pool.get_conn().await?;
        conn.exec_drop(query, params).await?;
        Ok(())
    }

    async fn query_rows(&self, sql: &str) -> Result<Vec<RowData>, DbError> {
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<MySqlRow> = conn.query(sql).await?;
        Ok(rows
            .iter()
            .map(|row| DbRow::MySqlRow(row).to_row_data(""))
            .collect())
    }

    async fn fetch_rows(&self, request: FetchRowsRequest) -> Result<Vec<RowData>, DbError> {
        let generator = QueryGenerator::new(&self.dialect);
        let (sql, params) = generator.select(&request);

        info!("Generated SQL: {}", sql);
        info!("Parameters: {:?}", params);

        let mut conn = self.pool.get_conn().await?;
        let params = MySqlParamStore::from_values(&params).params();
        let rows: Vec<MySqlRow> = conn.exec(sql, params).await?;
        Ok(rows
            .iter()
            .map(|row| DbRow::MySqlRow(row).to_row_data(&request.table))
            .collect())
    }

    async fn fetch_existing_keys(
        &self,
        _table: &str,
        _key_columns: &[String],
        _keys_batch: &[Vec<Value>],
    ) -> Result<Vec<RowData>, DbError> {
        todo!("Implement find_existing_keys for MySQL")
    }

    async fn table_exists(&self, table: &str) -> Result<bool, DbError> {
        let mut conn = self.pool.get_conn().await?;
        let exists: Option<(bool,)> = conn.exec_first(QUERY_TABLE_EXISTS_SQL, (table,)).await?;
        Ok(exists.map(|row| row.0).unwrap_or(false))
    }

    async fn list_tables(&self) -> Result<Vec<String>, DbError> {
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<MySqlRow> = conn.query("SHOW TABLES").await?;

        rows.into_iter()
            .map(|row| {
                row.get_opt::<String, _>(0)
                    .and_then(|res| res.ok())
                    .ok_or_else(|| DbError::Unknown("failed to read table name".to_string()))
            })
            .collect()
    }

    async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DbError> {
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<MySqlRow> = conn
            .exec(QUERY_TABLE_METADATA_SQL, (table, table, table, table))
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

    async fn referencing_tables(&self, table: &str) -> Result<Vec<String>, DbError> {
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<MySqlRow> = conn.exec(QUERY_TABLE_REFERENCING_SQL, (table,)).await?;

        rows.into_iter()
            .map(|row| {
                row.get_opt::<String, _>(COL_REFERENCING_TABLE)
                    .and_then(|res| res.ok())
                    .ok_or_else(|| {
                        DbError::Unknown("missing referencing_table column in metadata".to_string())
                    })
            })
            .collect()
    }

    async fn column_db_type(&self, table: &str, column: &str) -> Result<String, DbError> {
        let mut conn = self.pool.get_conn().await?;
        let row = conn
            .exec_first::<MySqlRow, _, _>(QUERY_COLUMN_TYPE_SQL, (table, column))
            .await?
            .ok_or_else(|| {
                DbError::Unknown(format!("column `{column}` not found for table `{table}`"))
            })?;

        row.get_opt::<String, _>("column_type")
            .and_then(|res| res.ok())
            .ok_or_else(|| DbError::Unknown("column_type missing in metadata".into()))
    }

    async fn truncate_table(&self, table: &str) -> Result<(), DbError> {
        let sql = QUERY_TRUNCATE_TABLE_SQL.replace("$1", &escape_identifier(table));
        let mut conn = self.pool.get_conn().await?;
        conn.query_drop(sql).await?;
        Ok(())
    }

    fn kind(&self) -> DatabaseKind {
        DatabaseKind::MySql
    }

    async fn capabilities(&self) -> Result<DbCapabilities, DbError> {
        MySqlCapabilityProbe::detect(self).await
    }
}

fn escape_identifier(name: &str) -> String {
    let escaped = name.replace('`', "``");
    format!("`{escaped}`")
}
