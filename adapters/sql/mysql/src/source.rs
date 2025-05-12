use crate::adapter::MySqlAdapter;
use async_trait::async_trait;
use sql_adapter::{
    adapter::SqlAdapter,
    error::db::DbError,
    filter::SqlFilter,
    join::{
        clause::{JoinClause, JoinColumn, JoinCondition, JoinType, JoinedTable},
        join_path_clauses,
        source::JoinSource,
    },
    metadata::{fk::ForeignKeyMetadata, provider::MetadataHelper, table::TableMetadata},
    requests::{FetchRowsRequest, FetchRowsRequestBuilder},
    row::row_data::RowData,
    source::DbDataSource,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

#[derive(Clone)]
pub struct MySqlDataSource {
    /// The MySQL adapter used to interact with the database.
    adapter: MySqlAdapter,

    /// The metadata for the primary source table
    primary_meta: Option<TableMetadata>,

    /// Metadata for any child tables (via FKs) when cascading
    related_meta: HashMap<String, TableMetadata>,

    /// Optional JOIN graph to be applied to the primary table
    join: Option<JoinSource>,

    /// Optional row‐filter pushed down to the source
    filter: Option<SqlFilter>,
}

impl MySqlDataSource {
    pub fn new(adapter: MySqlAdapter, join: Option<JoinSource>, filter: Option<SqlFilter>) -> Self {
        Self {
            adapter,
            join,
            filter,
            primary_meta: None,
            related_meta: HashMap::new(),
        }
    }

    fn build_fetch_request(&self, batch_size: usize, offset: Option<usize>) -> FetchRowsRequest {
        let meta = self
            .primary_meta
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

    fn build_related_fetch_requests(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Vec<FetchRowsRequest> {
        let join_src = match &self.join {
            Some(js) => js,
            None => return vec![],
        };

        // For each related table
        self.related_meta
            .iter()
            .map(|(tbl_name, meta)| {
                let cols = meta.select_fields();
                let table_joins = join_src.related_joins(tbl_name.clone());
                let filter = self
                    .filter
                    .as_ref()
                    .map(|f| f.for_table(&tbl_name, &table_joins));
                FetchRowsRequestBuilder::new(tbl_name.clone())
                    .alias(tbl_name.clone())
                    .columns(cols)
                    .joins(table_joins)
                    .filter(filter)
                    .limit(batch_size)
                    .offset(offset)
                    .build()
            })
            .collect()
    }

    fn collect_tables(graph: &HashMap<String, TableMetadata>, root: &str) -> HashSet<String> {
        let mut seen = HashSet::new();
        let mut queue = VecDeque::new();
        seen.insert(root.to_string());
        queue.push_back(root.to_string());

        while let Some(tbl) = queue.pop_front() {
            for fk in &graph[&tbl].foreign_keys {
                let child = fk.referenced_table.clone();
                if seen.insert(child.clone()) {
                    queue.push_back(child);
                }
            }
            for (t, meta) in graph {
                if meta
                    .foreign_keys
                    .iter()
                    .any(|fk| fk.referenced_table.eq_ignore_ascii_case(&tbl))
                {
                    if seen.insert(t.clone()) {
                        queue.push_back(t.clone());
                    }
                }
            }
        }
        seen
    }

    fn join_path(
        graph: &HashMap<String, TableMetadata>,
        root: &str,
        target: &str,
    ) -> HashSet<String> {
        let mut seen = HashSet::new();
        let mut queue = VecDeque::new();
        seen.insert(root.to_string());
        queue.push_back(root.to_string());

        while let Some(tbl) = queue.pop_front() {
            if tbl.eq_ignore_ascii_case(target) {
                return seen;
            }
            for fk in &graph[&tbl].foreign_keys {
                let child = fk.referenced_table.clone();
                if seen.insert(child.clone()) {
                    queue.push_back(child);
                }
            }
            for (t, meta) in graph {
                if meta
                    .foreign_keys
                    .iter()
                    .any(|fk| fk.referenced_table.eq_ignore_ascii_case(&tbl))
                {
                    if seen.insert(t.clone()) {
                        queue.push_back(t.clone());
                    }
                }
            }
        }
        seen
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
        for meta in self.related_meta.values() {
            let tables = Self::collect_tables(&self.related_meta, &meta.name);
            println!("Related tables: {:?} for {}", tables, meta.name);

            let filter_tables = self.filter.as_ref().map(|f| f.tables()).unwrap_or_default();
            println!("Filter tables: {:?}", filter_tables);

            let joins = filter_tables
                .iter()
                .map(|t| join_path_clauses(&self.related_meta, &meta.name, t))
                .filter(|jp| jp.is_some())
                .map(|jp| jp.unwrap())
                .flatten()
                .collect::<Vec<JoinClause>>();

            let mut seen = HashSet::new();
            let deduped: Vec<JoinClause> = joins
                .into_iter()
                .filter(|jc| seen.insert(jc.clone()))
                .collect();

            println!("Join clauses: {:?}", deduped);

            let select_fields = meta.select_fields();
            let request = FetchRowsRequestBuilder::new(meta.name.clone())
                .alias(meta.name.clone())
                .columns(select_fields)
                .joins(deduped)
                .filter(self.filter.clone())
                .limit(batch_size)
                .offset(offset)
                .build();

            let mut rows = self.adapter.fetch_rows(request).await?;
        }

        todo!("Implement fetch method for MySqlDataSource");

        // Build fetch request
        let request = self.build_fetch_request(batch_size, offset);
        // Build related fetch requests
        let related_requests = self.build_related_fetch_requests(batch_size, offset);

        let mut rows = self.adapter.fetch_rows(request).await?;

        for related_request in related_requests {
            let related_rows = self.adapter.fetch_rows(related_request).await?;
            rows.extend(related_rows);
        }

        Ok(rows)
    }
}

impl MetadataHelper for MySqlDataSource {
    fn get_metadata(&self) -> &Option<TableMetadata> {
        &self.primary_meta
    }

    fn set_metadata(&mut self, meta: TableMetadata) {
        self.primary_meta = Some(meta);
    }

    fn adapter(&self) -> Arc<(dyn SqlAdapter + Send + Sync)> {
        Arc::new(self.adapter.clone())
    }

    fn get_tables(&self) -> Vec<TableMetadata> {
        self.primary_meta
            .as_ref()
            .map(|meta| vec![meta.clone()])
            .unwrap_or_default()
    }

    fn set_related_meta(&mut self, meta: HashMap<String, TableMetadata>) {
        self.related_meta = meta;
    }
}
