use crate::{record::Record, source::data_source::DbDataSource};
use async_trait::async_trait;
use mysql::mysql::MySqlAdapter;
use sql_adapter::adapter::SqlAdapter;
use sql_adapter::join::source::JoinSource;
use sql_adapter::{metadata::table::TableMetadata, requests::FetchRowsRequest};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct MySqlDataSource {
    metadata: HashMap<String, TableMetadata>,
    adapter: MySqlAdapter,
}

impl MySqlDataSource {
    pub fn new(adapter: MySqlAdapter) -> Self {
        Self {
            metadata: HashMap::new(),
            adapter,
        }
    }
}

#[async_trait]
impl DbDataSource for MySqlDataSource {
    async fn fetch_data(
        &self,
        batch_size: usize,
        joins: &Vec<JoinSource>,
        offset: Option<usize>,
    ) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
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
                let (join_clause, joined_fields) = JoinSource::filter_joins(&tbl_name, joins);

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
                records.extend(rows.into_iter().map(Record::RowData));
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
