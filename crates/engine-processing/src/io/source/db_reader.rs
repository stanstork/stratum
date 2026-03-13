use crate::io::source::reader::SourceReader;
use async_trait::async_trait;
use connectors::{
    error::DriverError,
    sql::{
        filter::SqlFilter,
        join::{
            clause::{JoinClause, JoinType},
            source::JoinSource,
            utils::{build_join_clauses, find_join_path},
        },
        metadata::table::TableMetadata,
        request::{FetchRowsRequest, FetchRowsRequestBuilder},
    },
    traits::reader::DataReader,
};
use futures::future;
use model::{
    core::value::Value,
    pagination::{cursor::Cursor, page::FetchResult},
    records::Record,
};
use query_builder::offsets::OffsetStrategy;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

pub struct DbSourceReader {
    /// The underlying DataReader for fetching rows from the source database.
    reader: Arc<dyn DataReader>,

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

impl DbSourceReader {
    pub fn new(
        reader: Arc<dyn DataReader>,
        join: Option<JoinSource>,
        filter: Option<SqlFilter>,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Self {
        DbSourceReader {
            reader,
            primary_meta: None,
            related_meta: HashMap::new(),
            join,
            filter,
            cascade_joins: HashMap::new(),
            offset_strategy,
        }
    }

    pub fn has_primary_meta(&self) -> bool {
        self.primary_meta.is_some()
    }

    /// Set the primary table metadata (for cascade data fetching).
    pub fn set_primary_meta(&mut self, meta: TableMetadata) {
        self.primary_meta = Some(meta);
    }

    /// Populate cascade metadata for related tables.
    pub fn set_related_meta(&mut self, related: HashMap<String, TableMetadata>) {
        self.related_meta = related;
    }

    /// Build a request for ANY table.  If `include_join_fields` is true,
    /// we also merge in `join.fields()` (used only for the primary table).
    fn build_request_for(
        &self,
        table: &str,
        meta: &TableMetadata,
        joins: &[JoinClause],
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
        let filter_clause = self.filter.as_ref().map(|f| f.for_table(table, joins));

        FetchRowsRequestBuilder::new(table.to_string())
            .alias(table.to_string())
            .columns(columns)
            .joins(joins.to_vec())
            .filter(filter_clause)
            .limit(batch_size)
            .cursor(cursor)
            .strategy(self.offset_strategy.clone())
            .build()
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

    /// Build a request for only the primary table.
    fn build_primary_only_request(&self, batch_size: usize, cursor: Cursor) -> FetchRowsRequest {
        let meta = self
            .primary_meta
            .as_ref()
            .expect("primary_meta must be set");
        let joins = self
            .join
            .as_ref()
            .map(|j| j.clauses.clone())
            .unwrap_or_default();
        self.build_request_for(&meta.name, meta, &joins, batch_size, cursor, true)
    }

    /// Get metadata for any table - primary or related.
    fn meta_for(&self, table: &str) -> Option<&TableMetadata> {
        if let Some(m) = &self.primary_meta
            && m.name.eq_ignore_ascii_case(table)
        {
            return Some(m);
        }
        self.related_meta.get(table)
    }

    /// Determine the IN clause column and values to scope a related table's fetch,
    /// searching across ALL already-fetched tables (not just the primary).
    ///
    /// Handles two FK directions for every fetched table:
    /// - Fetched -> Related: FK on a fetched table pointing to the related table
    /// - Related -> Fetched: FK on the related table pointing to a fetched table
    fn extract_fk_in_clause(
        &self,
        related_table: &str,
        related_meta: &TableMetadata,
        all_fetched: &HashMap<String, Vec<Record>>,
    ) -> Option<(String, Vec<Value>)> {
        // Case 1: FK on related table pointing to a fetched table
        for fk in &related_meta.foreign_keys {
            if fk.columns.len() != 1 || fk.referenced_columns.len() != 1 {
                continue;
            }
            if let Some(rows) = all_fetched
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(&fk.referenced_table))
                .map(|(_, v)| v)
            {
                let ref_col = &fk.referenced_columns[0];
                let fk_col = &fk.columns[0];
                let values: Vec<Value> = rows
                    .iter()
                    .map(|row| row.get_value(ref_col))
                    .filter(|v| !matches!(v, Value::Null))
                    .collect();
                if !values.is_empty() {
                    return Some((fk_col.clone(), values));
                }
            }
        }

        // Case 2: FK on a fetched table pointing to the related table.
        // Union values from ALL matching fetched tables - multiple tables may reference the same
        // column (e.g. customer.address_id, staff.address_id, and store.address_id all point to
        // address.address_id; missing any one causes a FK violation).
        let mut ref_col: Option<String> = None;
        let mut union_values: Vec<Value> = Vec::new();

        for (fetched_name, rows) in all_fetched {
            let fetched_meta = match self.meta_for(fetched_name) {
                Some(m) => m,
                None => continue,
            };
            for fk in &fetched_meta.foreign_keys {
                if !fk.referenced_table.eq_ignore_ascii_case(related_table) {
                    continue;
                }
                if fk.columns.len() != 1 || fk.referenced_columns.len() != 1 {
                    continue;
                }
                let col = &fk.referenced_columns[0];
                // Only union values that scope the same target column.
                if let Some(existing) = &ref_col {
                    if !existing.eq_ignore_ascii_case(col) {
                        continue;
                    }
                } else {
                    ref_col = Some(col.clone());
                }
                let fk_col = &fk.columns[0];
                union_values.extend(
                    rows.iter()
                        .map(|row| row.get_value(fk_col))
                        .filter(|v| !matches!(v, Value::Null)),
                );
            }
        }

        if let Some(col) = ref_col
            && !union_values.is_empty()
        {
            return Some((col, union_values));
        }

        None
    }

    /// Build a fetch request for a related table, optionally scoped by an IN clause.
    fn build_related_request(
        &self,
        table: &str,
        meta: &TableMetadata,
        in_clause: Option<(String, Vec<Value>)>,
    ) -> FetchRowsRequest {
        let columns = meta.select_fields();
        let extra_joins = self.cascade_joins.get(table).cloned().unwrap_or_default();

        let filter_clause = self
            .filter
            .as_ref()
            .map(|f| f.for_table(table, &extra_joins));

        // Related table fetches in cascade mode must retrieve ALL rows that satisfy the
        // IN-clause - not just batch_size rows. Using batch_size as the limit would silently
        // truncate the related result when more rows match than the primary batch size.
        let mut builder = FetchRowsRequestBuilder::new(table.to_string())
            .alias(table.to_string())
            .columns(columns)
            .joins(extra_joins)
            .filter(filter_clause)
            .limit(i64::MAX as usize)
            .cursor(Cursor::None)
            .strategy(self.offset_strategy.clone());

        if let Some((col, values)) = in_clause {
            builder = builder.in_clause(col, values);
        }

        builder.build()
    }

    /// Topologically sort `ready` tables so FK children (referencing tables) come before their
    /// FK parents (referenced tables) within the ready set. This ensures that when we fetch a
    /// table, any same-round table whose FK values we need to scope it has already been fetched.
    ///
    /// Example: store.address_id -> address, both ready -> store fetched before address so that
    /// store's address_id values can be included in the IN-clause for address.
    fn topo_sort_ready<'a>(&'a self, ready: &[&'a str]) -> Vec<&'a str> {
        let ready_set: HashSet<&str> = ready.iter().copied().collect();

        // In-degree = number of ready tables that THIS table is referenced by (FK parents have
        // higher in-degree because FK children point to them).
        let mut in_degree: HashMap<&str, usize> = ready.iter().map(|&t| (t, 0usize)).collect();

        for &table in ready {
            if let Some(meta) = self.related_meta.get(table) {
                for fk in &meta.foreign_keys {
                    // table -> fk.referenced_table: referenced_table is a FK parent inside ready
                    if ready_set.contains(fk.referenced_table.as_str()) {
                        *in_degree.entry(fk.referenced_table.as_str()).or_default() += 1;
                    }
                }
            }
        }

        // Kahn's: start with in-degree 0 (FK children - nobody inside the ready set points to
        // them as a parent). This gives FK-child-first order, which is what we want.
        let mut queue: Vec<&str> = in_degree
            .iter()
            .filter(|&(_, &d)| d == 0)
            .map(|(&t, _)| t)
            .collect();
        queue.sort(); // deterministic within a level

        let mut result: Vec<&str> = Vec::with_capacity(ready.len());
        while !queue.is_empty() {
            queue.sort(); // keep deterministic
            let t = queue.remove(0);
            result.push(t);
            if let Some(meta) = self.related_meta.get(t) {
                for fk in &meta.foreign_keys {
                    let ref_table = fk.referenced_table.as_str();
                    if ready_set.contains(ref_table) {
                        let deg = in_degree.entry(ref_table).or_default();
                        *deg = deg.saturating_sub(1);
                        if *deg == 0 {
                            queue.push(ref_table);
                        }
                    }
                }
            }
        }

        // Append any tables not reached by Kahn's (cycles, shouldn't happen)
        for &t in ready {
            if !result.contains(&t) {
                result.push(t);
            }
        }
        result
    }

    async fn fetch_cascaded(
        &self,
        batch_size: usize,
        cursor: Cursor,
    ) -> Result<(Vec<Record>, Option<Cursor>, bool), DriverError> {
        let primary_req = self.build_primary_only_request(batch_size, cursor.clone());
        let primary_rows = self.reader.fetch(primary_req).await?;

        let reached_end = primary_rows.len() < batch_size;
        let primary_last_row = primary_rows.last().cloned();

        let primary_name = self
            .primary_meta
            .as_ref()
            .map(|m| m.name.clone())
            .unwrap_or_default();

        // BFS: fetch related tables in FK dependency order so that each table
        // is scoped to the FK values of the tables already fetched.
        //
        // Within each BFS round, tables are topologically sorted by FK relationships
        // among themselves (FK children first). This ensures that when store -> address,
        // store is fetched before address, allowing store's address_id values to be
        // included in address's IN-clause.
        let mut all_fetched: HashMap<String, Vec<Record>> = HashMap::new();
        all_fetched.insert(primary_name, primary_rows);

        let mut remaining: HashSet<String> = self.related_meta.keys().cloned().collect();

        while !remaining.is_empty() {
            // Collect tables that can be scoped (have a FK neighbor already fetched)
            let ready_owned: Vec<String> = remaining
                .iter()
                .filter(|table| {
                    let meta = &self.related_meta[table.as_str()];
                    self.extract_fk_in_clause(table, meta, &all_fetched)
                        .is_some()
                })
                .cloned()
                .collect();

            if ready_owned.is_empty() {
                // No FK connections to already-fetched tables - fetch remaining unscoped.
                for table in remaining.iter() {
                    let meta = &self.related_meta[table.as_str()];
                    let req = self.build_related_request(table, meta, None);
                    let rows = self.reader.fetch(req).await?;
                    all_fetched.insert(table.clone(), rows);
                }
                break;
            }

            // Sort ready tables so FK children come first (they provide scoping values
            // for the FK parents that follow in the same round).
            let ready_refs: Vec<&str> = ready_owned.iter().map(|s| s.as_str()).collect();
            let sorted = self.topo_sort_ready(&ready_refs);

            // Fetch sequentially in sorted order so each table benefits from the
            // already-fetched results of the tables processed before it.
            for table in sorted {
                let meta = &self.related_meta[table];
                let in_clause = self.extract_fk_in_clause(table, meta, &all_fetched);
                let req = self.build_related_request(table, meta, in_clause);
                let rows = self.reader.fetch(req).await?;
                all_fetched.insert(table.to_string(), rows);
                remaining.remove(table);
            }
        }

        let rows: Vec<Record> = all_fetched.into_values().flatten().collect();

        let next_cursor =
            self.compute_next_cursor(primary_last_row.as_ref(), &cursor, batch_size, reached_end);

        Ok((rows, next_cursor, reached_end))
    }

    async fn fetch_single(
        &self,
        batch_size: usize,
        cursor: Cursor,
    ) -> Result<(Vec<Record>, Option<Cursor>, bool), DriverError> {
        let requests = self.build_fetch_rows_requests(batch_size, cursor.clone());
        let futures = requests.into_iter().map(|req| self.reader.fetch(req));
        let results = future::join_all(futures).await;

        let mut rows = Vec::new();
        let mut primary_rows_count = None;
        let mut primary_last_row = None;

        for (idx, result) in results.into_iter().enumerate() {
            let mut fetched_rows = result?;
            if idx == 0 {
                primary_rows_count = Some(fetched_rows.len());
                primary_last_row = fetched_rows.last().cloned();
            }
            rows.append(&mut fetched_rows);
        }

        let reference_count = primary_rows_count.unwrap_or(rows.len());
        let reached_end = reference_count < batch_size;
        let last_row = primary_last_row.or_else(|| rows.last().cloned());

        let next_cursor =
            self.compute_next_cursor(last_row.as_ref(), &cursor, batch_size, reached_end);

        Ok((rows, next_cursor, reached_end))
    }

    fn compute_next_cursor(
        &self,
        last_row: Option<&Record>,
        current_cursor: &Cursor,
        batch_size: usize,
        reached_end: bool,
    ) -> Option<Cursor> {
        if reached_end {
            return None;
        }

        last_row.map(|row| {
            let next = self.offset_strategy.next_cursor(row);
            // Hack to keep track of how many rows we've read so far when using Default
            // TODO: improve this by having the strategy manage its own state
            match (current_cursor, &next) {
                (Cursor::None, Cursor::Default { offset }) => Cursor::Default {
                    offset: offset + batch_size,
                },
                (Cursor::Default { offset }, Cursor::Default { .. }) => Cursor::Default {
                    offset: offset + batch_size,
                },
                _ => next,
            }
        })
    }
}

#[async_trait]
impl SourceReader for DbSourceReader {
    async fn fetch(&self, batch_size: usize, cursor: Cursor) -> Result<FetchResult, DriverError> {
        let start = Instant::now();

        let (rows, next_cursor, reached_end) =
            if !self.related_meta.is_empty() && self.primary_meta.is_some() {
                // Two-phase fetch when cascade is active
                self.fetch_cascaded(batch_size, cursor).await?
            } else {
                // Single-phase fetch: no cascade
                self.fetch_single(batch_size, cursor).await?
            };

        Ok(FetchResult {
            row_count: rows.len(),
            took_ms: start.elapsed().as_millis(),
            rows,
            next_cursor,
            reached_end,
        })
    }
}
