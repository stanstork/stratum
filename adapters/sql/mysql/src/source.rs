use crate::adapter::MySqlAdapter;
use async_trait::async_trait;
use sql_adapter::{
    adapter::SqlAdapter,
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
    joins: Vec<JoinSource>,
    filter: Option<SqlFilter>,
}

impl MySqlDataSource {
    pub fn new(adapter: MySqlAdapter, joins: Vec<JoinSource>, filter: Option<SqlFilter>) -> Self {
        Self {
            adapter,
            joins,
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
        let mut requests = Vec::new();

        for tbl_name in self.meta.keys() {
            for (table_name, mut base_fields) in self.get_metadata(tbl_name).select_fields() {
                // Skip already processed tables
                if !seen_tables.insert(table_name.clone()) {
                    continue;
                }

                // Extract join info for this table, if any
                // let (joins, joined_fields) = JoinSource::filter_joins(&table_name, &self.joins);
                // base_fields.extend(joined_fields);

                // // Build filter for this table+joins
                // let filter = self
                //     .filter
                //     .as_ref()
                //     .map(|f| f.for_table(&table_name, &joins));

                // let request = FetchRowsRequestBuilder::new(table_name.clone())
                //     .alias(table_name.clone())
                //     .columns(base_fields)
                //     .joins(joins)
                //     .filter(filter)
                //     .limit(batch_size)
                //     .offset(offset)
                //     .build();

                // requests.push(request);

                todo!("Implement the request building logic");
            }
        }

        requests
    }
}

#[async_trait]
impl DbDataSource for MySqlDataSource {
    async fn fetch(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<RowData>, Box<dyn std::error::Error>> {
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
