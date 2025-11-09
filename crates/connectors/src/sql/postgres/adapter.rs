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
    postgres::{
        data_type::PgDataType,
        params::PgParamStore,
        probe::PgCapabilityProbe,
        utils::{connect_client, flatten_values},
    },
};
use async_trait::async_trait;
use model::{
    core::{data_type::DataType, value::Value},
    records::row::RowData,
};
use planner::query::dialect;
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::Client;

#[derive(Clone)]
pub struct PgAdapter {
    client: Arc<Client>,
    dialect: dialect::Postgres,
}

const QUERY_TABLE_EXISTS_SQL: &str = include_str!("sql/table_exists.sql");
const QUERY_TRUNCATE_TABLE_SQL: &str = include_str!("sql/table_truncate.sql");
const QUERY_TABLE_METADATA_SQL: &str = include_str!("sql/table_metadata.sql");
const QUERY_TABLE_REFERENCING_SQL: &str = include_str!("sql/table_referencing.sql");

#[async_trait]
impl SqlAdapter for PgAdapter {
    async fn connect(url: &str) -> Result<Self, ConnectorError> {
        let client = Arc::new(connect_client(url).await?);
        Ok(PgAdapter {
            client,
            dialect: dialect::Postgres,
        })
    }

    async fn exec(&self, query: &str) -> Result<(), DbError> {
        self.client.batch_execute(query).await?;
        Ok(())
    }

    async fn exec_params(&self, query: &str, params: Vec<Value>) -> Result<(), DbError> {
        let bindings = PgParamStore::from_values(params);
        self.client.execute(query, &bindings.as_refs()).await?;
        Ok(())
    }

    async fn query_rows(&self, sql: &str) -> Result<Vec<RowData>, DbError> {
        let rows = self.client.query(sql, &[]).await?;
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

        let rows = self.client.query(&sql, &refs).await?;
        let result = rows
            .iter()
            .map(|row| DbRow::PostgresRow(row).to_row_data(table))
            .collect();

        Ok(result)
    }

    async fn table_exists(&self, table: &str) -> Result<bool, DbError> {
        let row = self
            .client
            .query_one(QUERY_TABLE_EXISTS_SQL, &[&table])
            .await?;
        Ok(row.get(0))
    }

    async fn list_tables(&self) -> Result<Vec<String>, DbError> {
        todo!("Implement list_tables for Postgres");
    }

    async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DbError> {
        let query = QUERY_TABLE_METADATA_SQL.replace("{table}", table);
        let rows = self.client.query(&query, &[]).await?;
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
        let rows = self
            .client
            .query(QUERY_TABLE_REFERENCING_SQL, &[&table])
            .await?;

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
        self.client
            .execute(QUERY_TRUNCATE_TABLE_SQL, &[&table])
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
