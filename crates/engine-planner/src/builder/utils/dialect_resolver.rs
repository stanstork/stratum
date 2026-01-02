use connectors::adapter::Adapter;
use query_builder::dialect::{self, Dialect};
use std::sync::Arc;

/// Resolve the SQL dialect for an adapter
pub fn dialect_for_adapter(adapter: &Adapter) -> Arc<dyn Dialect> {
    match adapter {
        Adapter::Postgres(_) => Arc::new(dialect::Postgres),
        Adapter::MySql(_) => Arc::new(dialect::MySql),
        Adapter::Csv(_) => {
            // CSV doesn't have a SQL dialect, use Postgres as default
            Arc::new(dialect::Postgres)
        }
    }
}
