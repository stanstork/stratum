use crate::settings::{driver::SchemaDriver, error::SettingsError};
use connectors::error::DriverError;
use connectors::traits::executor::QueryExecutor;
use engine_core::schema::schema_ops::SchemaOp;
use tracing::{debug, error, info};

/// Execute a sequence of schema operations against a destination driver.
///
/// Idempotent operations silently skip "already exists" errors (e.g., enum types).
pub async fn apply_schema_ops(
    driver: &dyn QueryExecutor,
    ops: &[SchemaOp],
) -> Result<(), SettingsError> {
    for op in ops {
        info!("Executing: {}", op.description);
        debug!("SQL: {}", op.sql);

        if let Err(err) = driver.execute(&op.sql).await {
            if op.idempotent && is_type_already_exists_error(&err) {
                info!("Already exists, skipping: {}", op.description);
                continue;
            }
            error!(
                "Failed to execute schema op: {}\nSQL: {}\nError: {:?}",
                op.description, op.sql, err
            );
            return Err(SettingsError::Driver(err));
        }
    }
    Ok(())
}

/// Check if the error is a "type already exists" or "relation already exists" error.
/// SQL state 42710 = duplicate_object, 42P07 = duplicate_table.
/// Safe to ignore for idempotent schema ops like CREATE TYPE / CREATE TABLE IF NOT EXISTS.
fn is_type_already_exists_error(err: &DriverError) -> bool {
    match err {
        DriverError::PgError(pg_err) => {
            if let Some(db_err) = pg_err.as_db_error() {
                let code = db_err.code().code();
                code == "42710" || code == "42P07"
            } else {
                false
            }
        }
        // QueryError wraps the error as a string — check for known duplicate codes
        DriverError::QueryError(msg) => {
            msg.contains("42710") || msg.contains("42P07") || msg.contains("already exists")
        }
        _ => false,
    }
}

#[derive(Clone)]
pub struct SchemaManager<D: SchemaDriver> {
    pub driver: std::sync::Arc<D>,
}
