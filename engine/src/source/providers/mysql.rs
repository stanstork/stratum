use crate::{record::Record, source::data_source::DbDataSource};
use async_trait::async_trait;
use mysql::mysql::MySqlAdapter;
use sql_adapter::adapter::SqlAdapter;
use sql_adapter::join::{self, Join, JoinClause};
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
        join: Option<Join>,
        offset: Option<usize>,
    ) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
        let mut records = Vec::new();
        let mut processed_tables = HashSet::new();
        let joins = join.map_or_else(Vec::new, |j| vec![j]);

        for table in self.metadata.keys() {
            let grouped_fields = self.get_metadata(table).select_fields();
            for (tbl, fields) in grouped_fields {
                if !processed_tables.insert(tbl.clone()) {
                    continue;
                }

                let tbl_join = joins
                    .iter()
                    .filter(|j| j.join_clause.right.table.eq_ignore_ascii_case(&tbl))
                    .cloned()
                    .collect::<Vec<_>>();
                let mut joined_fields = Vec::new();
                for j in &tbl_join {
                    let fields = j.source_metadata.select_fields();
                    let mut fields = fields
                        .get(&j.source_metadata.name)
                        .map(|f| f.clone())
                        .unwrap_or_default();

                    for field in fields.iter_mut() {
                        field.table = j.join_clause.left.alias.clone();
                        field.alias =
                            Some(format!("{}_{}", j.join_clause.left.table, field.column));
                    }

                    joined_fields.extend(fields);
                }

                let mut fields = fields.clone();
                fields.extend(joined_fields);

                let join_clause = tbl_join
                    .iter()
                    .map(|j| j.join_clause.clone())
                    .collect::<Vec<_>>();

                let request = FetchRowsRequest::new(
                    tbl.clone(),
                    Some(tbl.clone()),
                    fields,
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
