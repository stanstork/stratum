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
        info!(op = %op.description, "executing schema operation");
        debug!(sql = %op.sql, "schema operation SQL");

        if let Err(err) = driver.execute(&op.sql).await {
            if op.idempotent && is_type_already_exists_error(&err) {
                info!(op = %op.description, "schema object already exists, skipping");
                continue;
            }
            if op.skip_if_missing_ref && is_relation_not_found_error(&err) {
                info!(op = %op.description, "referenced table missing in destination, skipping");
                continue;
            }
            error!(
                op = %op.description,
                sql = %op.sql,
                error = ?err,
                "failed to execute schema operation"
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

/// Check if the error is a "relation does not exist" error (SQL state 42P01).
/// This occurs when a FK references a table that is not in the destination,
/// e.g. because it was not part of this migration.
fn is_relation_not_found_error(err: &DriverError) -> bool {
    match err {
        DriverError::PgError(pg_err) => {
            if let Some(db_err) = pg_err.as_db_error() {
                db_err.code().code() == "42P01"
            } else {
                false
            }
        }
        DriverError::QueryError(msg) => msg.contains("42P01"),
        _ => false,
    }
}

#[derive(Clone)]
pub struct SchemaManager<D: SchemaDriver> {
    pub driver: std::sync::Arc<D>,
}
