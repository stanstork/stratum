use crate::settings::error::SettingsError;
use connectors::sql::base::{error::DbError, query::column::ColumnDef};
use engine_core::{
    connectors::destination::{DataDestination, Destination},
    schema::plan::SchemaPlan,
};
use futures::lock::Mutex;
use std::sync::Arc;
use tracing::{error, info};

pub struct SchemaManager {
    pub destination: Arc<Mutex<Destination>>,
}

impl SchemaManager {
    pub async fn add_column(
        &mut self,
        table: &str,
        column: &ColumnDef,
    ) -> Result<(), SettingsError> {
        let dest = self.destination.lock().await;
        let DataDestination::Database(db) = &dest.data_dest;
        let result = db.data.lock().await.add_column(table, column).await;

        match result {
            Ok(_) => {
                info!(
                    "Successfully added missing column '{}' to destination table '{}'.",
                    column.name, table
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to add column '{}' to table '{}': {}",
                    column.name, table, e
                );
                Err(SettingsError::Database(e))
            }
        }
    }

    pub async fn infer_schema(&mut self, schema_plan: &SchemaPlan) -> Result<(), SettingsError> {
        let dest = self.destination.lock().await;
        info!("Applying inferred schema to destination: {}", dest.name);

        let enum_queries = schema_plan.enum_queries().await?;
        let table_queries = schema_plan.table_queries().await;
        let fk_queries = schema_plan.fk_queries();

        let all_queries = enum_queries
            .iter()
            .chain(&table_queries)
            .chain(&fk_queries)
            .cloned();

        for query in all_queries {
            info!("Executing schema change: {}", query.0);
            if let Err(err) = dest.data_dest.adapter().await.exec(&query.0).await {
                // Check if this is a "type already exists" error (SQL state 42710)
                // This is safe to ignore as it means the enum type is already defined
                if Self::is_type_already_exists_error(&err) {
                    info!("Type '{}' already exists, skipping creation", query.1);
                    continue;
                }

                error!(
                    "Failed to apply schema change: {}\nError: {:?}",
                    query.0, err
                );
                // Stop immediately on the first error to prevent partial schema application.
                return Err(SettingsError::Database(err));
            }
        }

        info!("Schema inference completed and applied successfully.");
        Ok(())
    }

    /// Check if the error is a "type already exists" error (SQL state 42710)
    /// This is safe to ignore when creating enum types.
    fn is_type_already_exists_error(err: &DbError) -> bool {
        match err {
            DbError::PgError(pg_err) => {
                if let Some(db_err) = pg_err.as_db_error() {
                    // SQL state 42710 = duplicate_object (type already exists)
                    db_err.code().code() == "42710"
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}
