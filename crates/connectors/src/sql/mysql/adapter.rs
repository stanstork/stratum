use crate::sql::{
    base::{
        adapter::SqlAdapter,
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
    mysql::data_type::MySqlColumnDataType,
};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use model::{
    core::{data_type::DataType, value::Value},
    records::row::RowData,
};
use planner::query::dialect;
use sqlx::{MySql, Pool, Row, query::Query};
use std::collections::HashMap;
use tracing::{debug, info, trace};

fn bind_values<'q>(
    mut query: Query<'q, MySql, sqlx::mysql::MySqlArguments>,
    params: &'q [Value],
) -> Query<'q, MySql, sqlx::mysql::MySqlArguments> {
    for p in params {
        query = match p {
            Value::Int(i) => query.bind(*i),
            Value::Uint(u) => query.bind(*u),
            Value::Usize(u) => query.bind(*u as u64),
            Value::Float(f) => query.bind(*f),
            Value::String(s) => query.bind(s),
            Value::Boolean(b) => query.bind(*b),
            Value::Json(j) => query.bind(j),
            Value::Uuid(u) => query.bind(*u),
            Value::Bytes(b) => query.bind(b),
            Value::Date(d) => query.bind(*d),
            Value::Timestamp(t) => query.bind(*t),
            Value::Null => query.bind(None::<String>),
            Value::Enum(_, v) => query.bind(v),
            Value::StringArray(v) => query.bind(format!("{v:?}")), // Bind as a string representation of the array
        };
    }
    query
}

#[derive(Clone)]
pub struct MySqlAdapter {
    pool: Pool<MySql>,
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
        let pool = Pool::connect(url).await?;
        Ok(MySqlAdapter {
            pool,
            dialect: dialect::MySql,
        })
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
        let generator = QueryGenerator::new(&self.dialect);
        let (sql, params) = generator.select(&request);

        // Log the generated SQL query for debugging
        info!("Generated SQL: {}", sql);
        info!("Parameters: {:?}", params);

        // Bind parameters and execute
        let query = sqlx::query(&sql);
        let bound_query = bind_values(query, &params);

        let result = bound_query
            .fetch(&self.pool) // returns a stream of results
            .map_ok(|row| DbRow::MySqlRow(&row).get_row_data(&request.table))
            .try_collect::<Vec<RowData>>() // gathers the items into a collection, stopping at the first error
            .await?;

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

    async fn list_tables(&self) -> Result<Vec<String>, DbError> {
        let rows = sqlx::query("SHOW TABLES").fetch_all(&self.pool).await?;

        // extract each row's first column as Vec<u8> and then utf8‐decode
        let tables = rows
            .into_iter()
            .map(|row| {
                // get the VARBINARY column as Vec<u8>
                let raw: Vec<u8> = row.try_get(0)?;
                // convert to String
                String::from_utf8(raw)
                    .map_err(|e| DbError::Unknown(format!("invalid UTF-8 in table name: {e}")))
            })
            .collect::<Result<_, _>>()?;

        Ok(tables)
    }

    async fn fetch_existing_keys(
        &self,
        _table: &str,
        _key_columns: &[String],
        _keys_batch: &[Vec<Value>],
    ) -> Result<Vec<RowData>, DbError> {
        todo!("Implement find_existing_keys for MySQL")
    }
}
