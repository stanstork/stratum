use crate::adapter::MySqlAdapter;
use async_trait::async_trait;
use sql_adapter::{
    adapter::SqlAdapter, filter::SqlFilter, join::source::JoinSource,
    metadata::table::TableMetadata, requests::FetchRowsRequest, row::row_data::RowData,
    source::DbDataSource,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[derive(Clone)]
pub struct MySqlDataSource {
    adapter: MySqlAdapter,
    metadata: HashMap<String, TableMetadata>,
    joins: Vec<JoinSource>,
    filter: Option<SqlFilter>,
}

impl MySqlDataSource {
    pub fn new(adapter: MySqlAdapter, joins: Vec<JoinSource>) -> Self {
        Self {
            adapter,
            joins,
            metadata: HashMap::new(),
            filter: None,
        }
    }

    pub fn set_metadata(&mut self, metadata: HashMap<String, TableMetadata>) {
        self.metadata = metadata;
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

        for table in self.metadata.keys() {
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

                let request = FetchRowsRequest::new(
                    tbl_name.clone(),
                    Some(tbl_name.clone()), // alias is same as table name for now
                    all_fields,
                    join_clause,
                    batch_size,
                    offset,
                );

                let rows = self.adapter.fetch_rows(request).await?;
                records.extend(rows);
            }
        }

        Ok(records)
    }

    fn get_metadata(&self, table: &str) -> &TableMetadata {
        self.metadata
            .get(table)
            .unwrap_or_else(|| panic!("Metadata for table {} not found", table))
    }

    fn set_metadata(&mut self, metadata: HashMap<String, TableMetadata>) {
        self.metadata = metadata;
    }

    fn adapter(&self) -> Arc<(dyn SqlAdapter + Send + Sync)> {
        Arc::new(self.adapter.clone())
    }
}
