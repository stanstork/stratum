use crate::adapter::MySqlAdapter;
use async_trait::async_trait;
use sql_adapter::{
    adapter::SqlAdapter,
    error::db::DbError,
    filter::filter::SqlFilter,
    join::source::JoinSource,
    metadata::{provider::MetadataHelper, table::TableMetadata},
    requests::{FetchRowsRequest, FetchRowsRequestBuilder},
    row::row_data::RowData,
    source::DbDataSource,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct MySqlDataSource {
    adapter: MySqlAdapter,
    meta: Option<TableMetadata>,
    join: Option<JoinSource>,
    filter: Option<SqlFilter>,
}

impl MySqlDataSource {
    pub fn new(adapter: MySqlAdapter, join: Option<JoinSource>, filter: Option<SqlFilter>) -> Self {
        Self {
            adapter,
            join,
            filter,
            meta: None,
        }
    }

    fn build_fetch_request(&self, batch_size: usize, offset: Option<usize>) -> FetchRowsRequest {
        let meta = self
            .meta
            .as_ref()
            .expect("MySqlDataSource: Metadata is not set");

        // Precompute the JOIN clauses (if any) and any extra fields they bring in
        let join_clauses = self
            .join
            .as_ref()
            .map(|j| j.clauses.clone())
            .unwrap_or_default();
        let extra_fields = self.join.as_ref().map(|j| j.fields()).unwrap_or_default();

        // Base table name and its own metadata‐driven columns
        let table_name = meta.name.clone();
        let mut columns = meta.select_fields();
        // merge in the JOIN‐generated columns
        columns.extend(extra_fields);

        // Build the optional filter for this table
        let filter = self
            .filter
            .as_ref()
            .map(|f| f.for_table(&table_name, &join_clauses));

        FetchRowsRequestBuilder::new(table_name.clone())
            .alias(table_name.clone())
            .columns(columns)
            .joins(join_clauses)
            .filter(filter)
            .limit(batch_size)
            .offset(offset)
            .build()
    }
}

#[async_trait]
impl DbDataSource for MySqlDataSource {
    type Error = DbError;

    async fn fetch(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<RowData>, DbError> {
        // Build fetch request
        let request = self.build_fetch_request(batch_size, offset);
        self.adapter.fetch_rows(request).await
    }
}

impl MetadataHelper for MySqlDataSource {
    fn get_metadata(&self) -> &Option<TableMetadata> {
        &self.meta
    }

    fn set_metadata(&mut self, meta: TableMetadata) {
        self.meta = Some(meta);
    }

    fn adapter(&self) -> Arc<(dyn SqlAdapter + Send + Sync)> {
        Arc::new(self.adapter.clone())
    }

    fn get_tables(&self) -> Vec<TableMetadata> {
        self.meta
            .as_ref()
            .map(|meta| vec![meta.clone()])
            .unwrap_or_default()
    }
}
