use crate::adapter::MySqlAdapter;
use async_trait::async_trait;
use data_model::{pagination::cursor::Cursor, records::row_data::RowData};
use futures_util::future;
use sql_adapter::{
    adapter::SqlAdapter,
    error::db::DbError,
    filter::SqlFilter,
    join::{clause::JoinClause, source::JoinSource},
    metadata::{provider::MetadataHelper, table::TableMetadata},
    requests::{FetchRowsRequest, FetchRowsRequestBuilder},
    source::DbDataSource,
};
use std::{
    collections::{HashMap, HashSet},
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

    /// Optional JOIN graph to be applied to the related tables
    /// (if any) when cascading
    cascade_joins: HashMap<String, Vec<JoinClause>>,
}

impl MySqlDataSource {
    pub fn new(adapter: MySqlAdapter, join: Option<JoinSource>, filter: Option<SqlFilter>) -> Self {
        Self {
            adapter,
            join,
            filter,
            primary_meta: None,
            related_meta: HashMap::new(),
            cascade_joins: HashMap::new(),
        }
    }

    /// Build a request for ANY table.  If `include_join_fields` is true,
    /// we also merge in `join.fields()` (used only for the primary table).
    fn build_request_for(
        &self,
        table_name: &str,
        meta: &TableMetadata,
        join_clauses: &[JoinClause],
        batch_size: usize,
        cursor: Cursor,
        include_join_fields: bool,
    ) -> FetchRowsRequest {
        // base columns
        let mut columns = meta.select_fields();

        // optionally merge in the JoinSource's extra fields
        if include_join_fields {
            if let Some(join_source) = &self.join {
                columns.extend(join_source.fields());
            }
        }

        // optional filter scoped to this table + these clauses
        let filter_clause = self
            .filter
            .as_ref()
            .map(|f| f.for_table(table_name, join_clauses));

        FetchRowsRequestBuilder::new(table_name.to_string())
            .alias(table_name.to_string())
            .columns(columns)
            .joins(join_clauses.to_vec())
            .filter(filter_clause)
            .limit(batch_size)
            .cursor(cursor)
            .build()
    }
}

#[async_trait]
impl DbDataSource for MySqlDataSource {
    type Error = DbError;

    async fn fetch(&self, batch_size: usize, cursor: Cursor) -> Result<Vec<RowData>, DbError> {
        let requests = self.build_fetch_rows_requests(batch_size, cursor);
        let futures = requests.into_iter().map(|req| self.adapter.fetch_rows(req));

        // Run all futures concurrently
        let results = future::join_all(futures).await;

        let mut all_rows = Vec::new();
        for result in results {
            let mut fetched_rows = result?;
            all_rows.append(&mut fetched_rows);
        }

        Ok(all_rows)
    }

    /// Build all requests: primary with join-fields, then the related ones without them.
    fn build_fetch_rows_requests(
        &self,
        batch_size: usize,
        cursor: Cursor,
    ) -> Vec<FetchRowsRequest> {
        let mut reqs = Vec::new();
        let mut processed_tables = HashSet::new();

        // primary table
        if let Some(meta) = &self.primary_meta {
            let joins = self
                .join
                .as_ref()
                .map(|j| j.clauses.clone())
                .unwrap_or_default();

            reqs.push(self.build_request_for(
                &meta.name,
                meta,
                &joins,
                batch_size,
                cursor.clone(),
                true,
            ));
            processed_tables.insert(meta.name.clone());
        }

        // related tables (cascade_joins)
        for (table, meta) in &self.related_meta {
            // skip any tables already processed
            if !processed_tables.insert(table.clone()) {
                continue;
            }

            let joins = self
                .cascade_joins
                .get(table)
                .unwrap_or(&Vec::new())
                .to_vec();
            reqs.push(self.build_request_for(
                table,
                meta,
                &joins,
                batch_size,
                cursor.clone(),
                false,
            ));
        }

        reqs
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

    fn tables(&self) -> Vec<TableMetadata> {
        self.primary_meta
            .as_ref()
            .map(|meta| vec![meta.clone()])
            .unwrap_or_default()
    }

    fn set_related_meta(&mut self, meta: HashMap<String, TableMetadata>) {
        self.related_meta = meta;
    }

    fn set_cascade_joins(&mut self, table: String, joins: Vec<JoinClause>) {
        self.cascade_joins.insert(table, joins);
    }
}
