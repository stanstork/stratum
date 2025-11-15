use crate::sql::{
    base::{
        adapter::{DatabaseKind, SqlAdapter},
        capabilities::DbCapabilities,
        encoder::CopyValueEncoder,
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
use planner::query::dialect::{self};
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
        columns: &Vec<ColumnMetadata>,
        rows: &Vec<RowData>,
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
