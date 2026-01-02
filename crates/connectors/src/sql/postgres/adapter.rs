use crate::sql::{
    base::{
        adapter::{DatabaseKind, SqlAdapter},
        capabilities::DbCapabilities,
        encoder::CopyValueEncoder,
        error::{ConnectorError, DbError},
        filter::SqlFilter,
        metadata::{
            column::{COL_REFERENCING_TABLE, ColumnMetadata},
            index::IndexMetadata,
            provider::MetadataProvider,
            table::TableMetadata,
        },
        probe::CapabilityProbe,
        query::generator::QueryGenerator,
        requests::FetchRowsRequest,
        row::DbRow,
        transaction::Transaction,
    },
    postgres::{
        coercion,
        data_type::PgDataType,
        encoder::PgCopyValueEncoder,
        params::PgParamStore,
        probe::PgCapabilityProbe,
        utils::{connect_client, flatten_values},
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, pin_mut};
use model::{
    core::{data_type::DataType, value::Value},
    records::row::RowData,
};
use query_builder::dialect::{self};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock, RwLockWriteGuard};
use tokio_postgres::Client;
use tracing::debug;

#[derive(Clone)]
pub struct PgAdapter {
    client: Arc<RwLock<Client>>,
    dialect: dialect::Postgres,
}

const QUERY_TABLE_EXISTS_SQL: &str = include_str!("sql/table_exists.sql");
const QUERY_TRUNCATE_TABLE_SQL: &str = include_str!("sql/table_truncate.sql");
const QUERY_TABLE_METADATA_SQL: &str = include_str!("sql/table_metadata.sql");
const QUERY_TABLE_REFERENCING_SQL: &str = include_str!("sql/table_referencing.sql");
const QUERY_INDEX_METADATA_SQL: &str = include_str!("sql/index_metadata.sql");
const QUERY_COUNT_ROWS_FAST_SQL: &str = include_str!("sql/count_rows_fast.sql");
const QUERY_COUNT_APPROXIMATE_SQL: &str = include_str!("sql/count_approximate.sql");

impl PgAdapter {
    pub async fn lock_client(&self) -> RwLockWriteGuard<'_, Client> {
        self.client.write().await
    }

    fn transaction<'a>(
        &self,
        tx: &'a Transaction<'a>,
    ) -> Result<&'a tokio_postgres::Transaction<'a>, DbError> {
        match tx {
            Transaction::PgTransaction(pg_tx) => Ok(pg_tx),
            _ => Err(DbError::Unknown(
                "Provided transaction is not a Postgres transaction".to_string(),
            )),
        }
    }
}

#[async_trait]
impl SqlAdapter for PgAdapter {
    async fn connect(url: &str) -> Result<Self, ConnectorError> {
        let client = Arc::new(RwLock::new(connect_client(url).await?));
        Ok(PgAdapter {
            client,
            dialect: dialect::Postgres,
        })
    }

    async fn exec(&self, query: &str) -> Result<(), DbError> {
        let client = self.client.read().await;
        client.batch_execute(query).await?;
        Ok(())
    }

    async fn exec_params(&self, query: &str, params: Vec<Value>) -> Result<(), DbError> {
        let bindings = PgParamStore::from_values(params);
        let client = self.client.write().await;
        client.execute(query, &bindings.as_refs()).await?;
        Ok(())
    }

    async fn exec_tx(&self, tx: &Transaction<'_>, query: &str) -> Result<(), DbError> {
        let tx = self.transaction(tx)?;
        tx.batch_execute(query).await?;
        Ok(())
    }

    async fn exec_params_tx(
        &self,
        tx: &Transaction<'_>,
        query: &str,
        params: Vec<Value>,
    ) -> Result<(), DbError> {
        let tx = self.transaction(tx)?;
        let bindings = PgParamStore::from_values(params);
        tx.execute(query, &bindings.as_refs()).await?;
        Ok(())
    }

    async fn query_rows(&self, sql: &str) -> Result<Vec<RowData>, DbError> {
        let client = self.client.read().await;
        let rows = client.query(sql, &[]).await?;
        let result = rows
            .iter()
            .map(|row| DbRow::PostgresRow(row).to_row_data(""))
            .collect();
        Ok(result)
    }

    async fn query_rows_params(
        &self,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<Vec<RowData>, DbError> {
        let bindings = PgParamStore::from_values(params);
        let client = self.client.read().await;
        let rows = client.query(sql, &bindings.as_refs()).await?;
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

        let flat_values = flatten_values(keys_batch);
        let bindings = PgParamStore::from_values(flat_values);
        let refs = bindings.as_refs();

        let client = self.client.read().await;
        let rows = client.query(&sql, &refs).await?;
        let result = rows
            .iter()
            .map(|row| DbRow::PostgresRow(row).to_row_data(table))
            .collect();

        Ok(result)
    }

    async fn table_exists(&self, table: &str) -> Result<bool, DbError> {
        let client = self.client.read().await;
        let row = client.query_one(QUERY_TABLE_EXISTS_SQL, &[&table]).await?;
        Ok(row.get(0))
    }

    async fn list_tables(&self) -> Result<Vec<String>, DbError> {
        todo!("Implement list_tables for Postgres");
    }

    async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DbError> {
        let query = QUERY_TABLE_METADATA_SQL.replace("{table}", table);
        let client = self.client.read().await;
        let rows = client.query(&query, &[]).await?;
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

    async fn index_metadata(&self, table: &str) -> Result<Vec<IndexMetadata>, DbError> {
        let query = QUERY_INDEX_METADATA_SQL.replace("{table}", table);
        let client = self.client.read().await;
        let rows = client.query(&query, &[]).await?;
        let indexes = rows
            .iter()
            .map(|row| {
                let index_metadata = IndexMetadata::from_row(&DbRow::PostgresRow(row));
                Ok(index_metadata)
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        Ok(indexes)
    }

    async fn referencing_tables(&self, table: &str) -> Result<Vec<String>, DbError> {
        let client = self.client.read().await;
        let rows = client.query(QUERY_TABLE_REFERENCING_SQL, &[&table]).await?;

        let tables = rows
            .iter()
            .map(|row| row.try_get::<_, String>(COL_REFERENCING_TABLE))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(tables)
    }

    async fn column_db_type(&self, _table: &str, _column: &str) -> Result<String, DbError> {
        todo!("Implement fetch_column_type for Postgres");
    }

    async fn truncate_table(&self, table: &str) -> Result<(), DbError> {
        let client = self.client.read().await;
        client.execute(QUERY_TRUNCATE_TABLE_SQL, &[&table]).await?;
        Ok(())
    }

    async fn count_rows(
        &self,
        table: &str,
        schema: Option<&str>,
        filter: Option<&SqlFilter>,
    ) -> Result<u64, DbError> {
        let fqn = schema
            .map(|s| format!("{}.{}", s, table))
            .unwrap_or_else(|| table.to_string());

        let query = match filter {
            Some(f) => format!("SELECT COUNT(*) AS cnt FROM {} {}", fqn, f.to_sql()),
            None => format!("SELECT COUNT(*) AS cnt FROM {}", fqn),
        };

        let client = self.client.read().await;
        let row = client.query_one(&query, &[]).await?;
        let count: i64 = row.get("cnt");
        Ok(count as u64)
    }

    async fn count_rows_fast(&self, table: &str, _schema: Option<&str>) -> Result<u64, DbError> {
        let client = self.client.read().await;
        let row = client
            .query_one(QUERY_COUNT_ROWS_FAST_SQL, &[&table])
            .await?;
        let estimate: i64 = row.get("estimate");
        match estimate {
            n if n >= 0 => Ok(n as u64),
            _ => Err(DbError::Unknown(
                "Negative row count estimate received".to_string(),
            )),
        }
    }

    async fn count_approximate(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> Result<(u64, u64), DbError> {
        let fqn = schema
            .map(|s| format!("{}.{}", s, table))
            .unwrap_or_else(|| table.to_string());
        let row = {
            let client = self.client.read().await;
            client
                .query_one(&QUERY_COUNT_APPROXIMATE_SQL.replace("{table}", &fqn), &[])
                .await?
        };

        let sampled_estimate: i64 = row.get("sampled_estimate");
        let stats_estimate: f32 = row.get("stats_estimate");

        match stats_estimate {
            n if n < 0.0 => {
                return Err(DbError::Unknown(
                    "Negative approximate row count estimate received".to_string(),
                ));
            }
            _ => Ok((sampled_estimate as u64, stats_estimate as u64)),
        }
    }

    async fn table_size_bytes(&self, table: &str) -> Result<u64, DbError> {
        let query = format!("SELECT pg_total_relation_size('{}') AS size_bytes;", table);
        let client = self.client.read().await;
        let row = client.query_one(&query, &[]).await?;
        let size_bytes: i64 = row.get("size_bytes");
        Ok(size_bytes as u64)
    }

    fn kind(&self) -> DatabaseKind {
        DatabaseKind::Postgres
    }

    async fn capabilities(&self) -> Result<DbCapabilities, DbError> {
        PgCapabilityProbe::detect(self).await
    }

    async fn copy_rows(
        &self,
        tx: &Transaction<'_>,
        table: &str,
        columns: &[ColumnMetadata],
        rows: &[RowData],
    ) -> Result<(), DbError> {
        if rows.is_empty() {
            return Ok(());
        }

        let generator = QueryGenerator::new(&self.dialect);
        let statement = generator.copy_from_stdin(table, columns);
        let encoder = PgCopyValueEncoder::new();

        debug!("COPY statement: {}", statement);

        let tx = self.transaction(tx)?;
        let sink = tx.copy_in(&statement).await?;
        pin_mut!(sink);

        for row in rows {
            let mut line = String::new();
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    line.push(',');
                }
                let field = row.get(&col.name);
                let prepared = coercion::prepare_value(col, field);
                let encoded = match prepared {
                    Some(ref value) => encoder.encode_value(value),
                    None => encoder.encode_null(),
                };
                line.push_str(&encoded);
            }
            line.push('\n');
            sink.as_mut().send(Bytes::from(line)).await?;
        }

        sink.as_mut().close().await?;
        Ok(())
    }
}
