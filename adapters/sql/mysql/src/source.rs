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
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[derive(Clone)]
pub struct MySqlDataSource {
    adapter: MySqlAdapter,
    meta: HashMap<String, TableMetadata>,
    join: Option<JoinSource>,
    filter: Option<SqlFilter>,
}

impl MySqlDataSource {
    pub fn new(adapter: MySqlAdapter, join: Option<JoinSource>, filter: Option<SqlFilter>) -> Self {
        Self {
            adapter,
            join,
            filter,
            meta: HashMap::new(),
        }
    }

    fn build_fetch_requests(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Vec<FetchRowsRequest> {
        let mut seen_tables = HashSet::new();

        // Precompute join clauses and joined fields
        let join_clauses = self
            .join
            .as_ref()
            .map(|j| j.clauses.clone())
            .unwrap_or_default();
        let joined_fields = self.join.as_ref().map(|j| j.fields()).unwrap_or_default();

        // For each table‐metadata, extract (table_name, base_fields),
        // dedupe on table_name, extend with joined_fields, build request.
        self.meta
            .values()
            .flat_map(|table_meta| table_meta.select_fields())
            .filter_map(|(table_name, mut base_fields)| {
                // Skip duplicates
                if !seen_tables.insert(table_name.clone()) {
                    return None;
                }

                // Merge in any join‐generated fields
                base_fields.extend(joined_fields.clone());

                // Build optional filter expression
                let filter = self
                    .filter
                    .as_ref()
                    .map(|f| f.for_table(&table_name, &join_clauses));

                // Create the FetchRowsRequest
                Some(
                    FetchRowsRequestBuilder::new(table_name.clone())
                        .alias(table_name.clone())
                        .columns(base_fields)
                        .joins(join_clauses.clone())
                        .filter(filter)
                        .limit(batch_size)
                        .offset(offset)
                        .build(),
                )
            })
            .collect()
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
        // Build fetch requests for each table
        let requests = self.build_fetch_requests(batch_size, offset);

        let mut rows = Vec::new();
        for request in requests {
            // Execute each request and collect results
            let result = self.adapter.fetch_rows(request).await?;
            rows.extend(result);
        }

        Ok(rows)
    }
}

impl MetadataHelper for MySqlDataSource {
    fn get_metadata(&self, table: &str) -> &TableMetadata {
        self.meta
            .get(table)
            .unwrap_or_else(|| panic!("Metadata for table {} not found", table))
    }

    fn set_metadata(&mut self, metadata: HashMap<String, TableMetadata>) {
        self.meta = metadata;
    }

    fn adapter(&self) -> Arc<(dyn SqlAdapter + Send + Sync)> {
        Arc::new(self.adapter.clone())
    }

    fn get_tables(&self) -> Vec<TableMetadata> {
        self.meta.values().cloned().collect()
    }
}
