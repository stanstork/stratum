use crate::adapter::MySqlAdapter;
use async_trait::async_trait;
use sql_adapter::{
    adapter::SqlAdapter,
    filter::SqlFilter,
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

    pub fn set_metadata(&mut self, metadata: HashMap<String, TableMetadata>) {
        self.meta = metadata;
    }

    pub fn set_joins(&mut self, joins: Vec<JoinSource>) {
        self.joins = joins;
    }

    pub fn set_filter(&mut self, filter: Option<SqlFilter>) {
        self.filter = filter;
    }
}

#[async_trait]
impl DbDataSource for MySqlDataSource {
    async fn fetch(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<RowData>, Box<dyn std::error::Error>> {
        let mut records = Vec::new();
        let mut processed_tables = HashSet::new();

        for table in self.meta.keys() {
            let grouped_fields = self.get_metadata(table).select_fields();

            for (tbl_name, base_fields) in grouped_fields {
                // Skip already processed tables
                if !processed_tables.insert(tbl_name.clone()) {
                    continue;
                }

                // Extract join info for this table, if any
                let (join_clause, joined_fields) = JoinSource::filter_joins(&tbl_name, &self.joins);

                let mut all_fields = base_fields;
                all_fields.extend(joined_fields);

                let request_builder = FetchRowsRequestBuilder::new(tbl_name.clone())
                    .alias(tbl_name.clone())
                    .columns(all_fields.clone())
                    .joins(join_clause)
                    .filter(self.filter.clone())
                    .limit(batch_size)
                    .offset(offset);

                let rows = self.adapter.fetch_rows(request_builder.build()).await?;
                records.extend(rows);
            }
        }

        Ok(records)
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
