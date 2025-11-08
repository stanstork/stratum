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
        probe::CapabilityProbe,
        query::generator::QueryGenerator,
        requests::FetchRowsRequest,
        row::DbRow,
    },
    postgres::{data_type::PgDataType, probe::PgCapabilityProbe},
};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use model::{
    core::{data_type::DataType, value::Value},
    records::row::RowData,
};
use planner::query::dialect;
use sqlx::{Pool, Postgres, Row, query::Query};
use std::collections::HashMap;

fn bind_values<'q>(
    mut query: Query<'q, Postgres, sqlx::postgres::PgArguments>,
    params: &'q [Value],
) -> Query<'q, Postgres, sqlx::postgres::PgArguments> {
    for p in params {
        query = match p {
            Value::Int(i) => query.bind(*i),
            Value::Uint(u) => query.bind(*u as i64),
            Value::Usize(u) => query.bind(*u as i64),
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
            Value::StringArray(arr) => query.bind(arr),
        };
    }
    query
}

#[derive(Clone)]
pub struct PgAdapter {
    pool: Pool<Postgres>,
    dialect: dialect::Postgres,
}

const QUERY_TABLE_EXISTS_SQL: &str = include_str!("sql/table_exists.sql");
const QUERY_TRUNCATE_TABLE_SQL: &str = include_str!("sql/table_truncate.sql");
const QUERY_TABLE_METADATA_SQL: &str = include_str!("sql/table_metadata.sql");
const QUERY_TABLE_REFERENCING_SQL: &str = include_str!("sql/table_referencing.sql");

#[async_trait]
impl SqlAdapter for PgAdapter {
    async fn connect(url: &str) -> Result<Self, ConnectorError> {
        let pool = Pool::connect(url).await?;
        Ok(PgAdapter {
            pool,
            dialect: dialect::Postgres,
        })
    }

    async fn exec(&self, query: &str) -> Result<(), DbError> {
        sqlx::query(query).execute(&self.pool).await?;
        Ok(())
    }

    async fn exec_params(&self, query: &str, params: Vec<Value>) -> Result<(), DbError> {
        let query = sqlx::query(query);
        let bound_query = bind_values(query, &params);
        bound_query.execute(&self.pool).await?;
        Ok(())
    }

    async fn query_rows(&self, sql: &str) -> Result<Vec<RowData>, DbError> {
        let rows = sqlx::query(sql).fetch_all(&self.pool).await?;
        let result = rows
            .iter()
            .map(|row| DbRow::PostgresRow(row).to_row_data(""))
            .collect();
        Ok(result)
    }

    async fn fetch_rows(&self, _request: FetchRowsRequest) -> Result<Vec<RowData>, DbError> {
        todo!("Implement fetch_all for Postgres")
    }

    async fn fetch_existing_keys(
        &self,
        table: &str,
        key_columns: &[String],
        keys_batch: &[Vec<Value>],
    ) -> Result<Vec<RowData>, DbError> {
        let generator = QueryGenerator::new(&self.dialect);
        let sql = generator.key_existence(table, key_columns, keys_batch.len());

        let mut query = sqlx::query(&sql);
        for key_value in keys_batch {
            query = bind_values(query, key_value);
        }

        let rows_stream = query.fetch(&self.pool);
        let result = rows_stream
            .map_ok(|row| DbRow::PostgresRow(&row).to_row_data(table))
            .try_collect::<Vec<RowData>>()
            .await?;

        Ok(result)
    }

    async fn table_exists(&self, table: &str) -> Result<bool, DbError> {
        let row = sqlx::query(QUERY_TABLE_EXISTS_SQL)
            .bind(table)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get(0))
    }

    async fn list_tables(&self) -> Result<Vec<String>, DbError> {
        todo!("Implement list_tables for Postgres");
    }

    async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DbError> {
        let query = QUERY_TABLE_METADATA_SQL.replace("{table}", table);
        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;
        let columns = rows
            .iter()
            .map(|row| {
                let data_type = DataType::parse_from_row(row);
                let column_metadata = ColumnMetadata::from_row(&DbRow::PostgresRow(row), data_type);
                Ok((column_metadata.name.clone(), column_metadata))
            })
            .collect::<Result<HashMap<_, _>, DbError>>()?;

        MetadataProvider::construct_table_metadata(table, columns)
    }

    async fn referencing_tables(&self, table: &str) -> Result<Vec<String>, DbError> {
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

    async fn column_db_type(&self, _table: &str, _column: &str) -> Result<String, DbError> {
        todo!("Implement fetch_column_type for Postgres");
    }

    async fn truncate_table(&self, table: &str) -> Result<(), DbError> {
        sqlx::query(QUERY_TRUNCATE_TABLE_SQL)
            .bind(table)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    fn kind(&self) -> DatabaseKind {
        DatabaseKind::Postgres
    }

    async fn capabilities(&self) -> Result<DbCapabilities, DbError> {
        PgCapabilityProbe::detect(self).await
    }
}
