use crate::sql::{
    base::{
        adapter::SqlAdapter,
        error::DbError,
        filter::SqlFilter,
        join::{
            clause::{JoinClause, JoinType},
            source::JoinSource,
            utils::{build_join_clauses, find_join_path},
        },
        metadata::{provider::MetadataStore, table::TableMetadata},
        requests::{FetchRowsRequest, FetchRowsRequestBuilder},
        source::DbDataSource,
    },
    mysql::adapter::MySqlAdapter,
};
use async_trait::async_trait;
use futures_util::future;
use model::{
    pagination::{cursor::Cursor, page::FetchResult},
    records::row::RowData,
};
use planner::query::offsets::OffsetStrategy;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
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

    /// The offset strategy to use for pagination.
    offset_strategy: Arc<dyn OffsetStrategy>,
}

impl MySqlDataSource {
    pub fn new(
        adapter: MySqlAdapter,
        join: Option<JoinSource>,
        filter: Option<SqlFilter>,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Self {
        Self {
            adapter,
            join,
            filter,
            primary_meta: None,
            related_meta: HashMap::new(),
            cascade_joins: HashMap::new(),
            offset_strategy,
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
        if include_join_fields && let Some(join_source) = &self.join {
            columns.extend(join_source.fields());
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
            .strategy(self.offset_strategy.clone())
            .build()
    }
}

#[async_trait]
impl DbDataSource for MySqlDataSource {
    type Error = DbError;

    async fn fetch(&self, batch_size: usize, cursor: Cursor) -> Result<FetchResult, DbError> {
        let start = Instant::now();
        let requests = self.build_fetch_rows_requests(batch_size, cursor.clone());
        let futures = requests.into_iter().map(|req| self.adapter.fetch_rows(req));

        // Run all futures concurrently
        let results = future::join_all(futures).await;

        let mut rows: Vec<RowData> = Vec::new();
        let mut primary_rows_count: Option<usize> = None;
        let mut primary_last_row: Option<RowData> = None;

        for (idx, result) in results.into_iter().enumerate() {
            let mut fetched_rows = result?;

            if idx == 0 {
                primary_rows_count = Some(fetched_rows.len());
                if let Some(last_row) = fetched_rows.last() {
                    primary_last_row = Some(last_row.clone());
                }
            }

            rows.append(&mut fetched_rows);
        }

        let reference_count = primary_rows_count.unwrap_or(rows.len());
        let reached_end = reference_count < batch_size;

        let next_cursor = if !reached_end {
            primary_last_row
                .or_else(|| rows.last().cloned())
                .map(|row| {
                    let next = self.offset_strategy.next_cursor(&row);

                    // update offset if using Default cursor
                    // it's a hack to keep track of how many rows we've read so far
                    // TODO: improve this by having the strategy manage its own state
                    match (&cursor, &next) {
                        (Cursor::None, Cursor::Default { offset }) => Cursor::Default {
                            offset: offset + batch_size,
                        },
                        (Cursor::Default { offset }, Cursor::Default { .. }) => Cursor::Default {
                            offset: offset + batch_size,
                        },
                        _ => next,
                    }
                })
        } else {
            None
        };

        let took_ms = start.elapsed().as_millis();
        let row_count = rows.len();

        Ok(FetchResult {
            rows,
            next_cursor,
            reached_end,
            row_count,
            took_ms,
        })
    }

    /// Build all requests: primary with join-fields, then the related ones without them.
    fn build_fetch_rows_requests(
        &self,
        batch_size: usize,
        cursor: Cursor,
    ) -> Vec<FetchRowsRequest> {
        let mut reqs = Vec::new();
        let mut processed_tables = HashSet::new();
        let primary_table_name = self.primary_meta.as_ref().map(|meta| meta.name.clone());

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

            let mut joins = Vec::new();

            if let Some(primary_name) = primary_table_name.as_ref()
                && !table.eq_ignore_ascii_case(primary_name)
                    && let Some(path) =
                        find_join_path(&self.related_meta, table.as_str(), primary_name.as_str())
                    {
                        let join_path: Vec<String> = path.into_iter().skip(1).collect();
                        if !join_path.is_empty() {
                            joins.extend(build_join_clauses(
                                table.as_str(),
                                &join_path,
                                &self.related_meta,
                                JoinType::Inner,
                            ));
                        }
                    }

            if let Some(extra_joins) = self.cascade_joins.get(table) {
                for clause in extra_joins {
                    if !joins.iter().any(|existing| existing == clause) {
                        joins.push(clause.clone());
                    }
                }
            }

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

impl MetadataStore for MySqlDataSource {
    fn metadata(&self) -> &Option<TableMetadata> {
        &self.primary_meta
    }

    fn set_metadata(&mut self, meta: TableMetadata) {
        self.primary_meta = Some(meta);
    }

    fn adapter(&self) -> Arc<dyn SqlAdapter + Send + Sync> {
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
