use connectors::{adapter::Adapter, error::AdapterError, file::csv::settings::CsvSettings};
use futures::lock::Mutex;
use planner::plan::MigrationPlan;
use smql_syntax::ast::{connection::ConnectionPair, migrate::SpecKind};
use std::{collections::HashMap, sync::Arc};

use crate::{error::ContextError, state::sled_store::SledStateStore};

/// Holds connections and file adapters for the duration of a migration.
#[derive(Clone)]
pub struct GlobalContext {
    /// Shared SQL adapters (or None if not configured)
    pub src_conn: Option<Adapter>,
    pub dst_conn: Option<Adapter>,
    /// Number of records to process per batch
    pub batch_size: usize,
    /// Cache of file-backed adapters (e.g. CSV readers) by file path
    file_adapters: Arc<Mutex<HashMap<String, Adapter>>>,

    pub state: Arc<SledStateStore>,
}

impl GlobalContext {
    /// Build a new context, connecting to all SQL endpoints and
    /// pre-creating adapters for every CSV-based migrate item.
    pub async fn new(
        plan: &MigrationPlan,
        state: Arc<SledStateStore>,
    ) -> Result<Self, AdapterError> {
        let src_conn = Self::create_sql_adapter(&plan.connections.source).await?;
        let dst_conn = Self::create_sql_adapter(&plan.connections.dest).await?;
        let batch_size = plan.migration.settings.batch_size;

        // Pre-build file adapters for all CSV sources
        let initial_adapters = Self::build_file_adapters(plan)?;
        let file_adapters = Arc::new(Mutex::new(initial_adapters));

        Ok(GlobalContext {
            src_conn,
            dst_conn,
            batch_size,
            file_adapters,
            state,
        })
    }

    /// Get a file adapter for the given path.
    pub async fn get_file_adapter(&self, path: &str) -> Result<Adapter, ContextError> {
        let cache = self.file_adapters.lock().await;
        cache
            .get(path)
            .cloned()
            .ok_or_else(|| ContextError::AdapterNotFound(path.to_string()))
    }

    /// Create an SQL adapter if configured, or None otherwise.
    pub async fn create_sql_adapter(
        conn: &Option<ConnectionPair>,
    ) -> Result<Option<Adapter>, AdapterError> {
        match conn {
            Some(c) => Adapter::sql(c.format, &c.conn_str).await.map(Some),
            None => Ok(None),
        }
    }

    /// Scan the migration plan and build adapters for every CSV source.
    fn build_file_adapters(plan: &MigrationPlan) -> Result<HashMap<String, Adapter>, AdapterError> {
        plan.migration
            .migrate_items
            .iter()
            .filter(|mi| mi.source.kind == SpecKind::Csv)
            .map(|mi| {
                let path = mi.source.name().clone();
                let settings = CsvSettings::new(
                    mi.settings.csv_delimiter,
                    mi.settings.csv_header,
                    mi.settings.csv_id_column.clone(),
                );
                Adapter::file(&path, settings).map(|adapter| (path, adapter))
            })
            .collect()
    }
}
