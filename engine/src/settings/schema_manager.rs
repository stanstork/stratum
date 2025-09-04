use crate::{
    destination::{data::DataDestination, Destination},
    report::validation::{SchemaAction, ValidationReport},
    settings::error::SettingsError,
};
use query_builder::dialect;
use sql_adapter::query::{column::ColumnDef, generator::QueryGenerator};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

#[async_trait::async_trait]
pub trait SchemaManager: Send + Sync {
    async fn add_column(
        &mut self,
        dest: &Destination,
        table: &str,
        column: &ColumnDef,
    ) -> Result<(), SettingsError>;
}

pub struct LiveSchemaManager;

#[async_trait::async_trait]
impl SchemaManager for LiveSchemaManager {
    async fn add_column(
        &mut self,
        dest: &Destination,
        table: &str,
        column: &ColumnDef,
    ) -> Result<(), SettingsError> {
        let DataDestination::Database(db) = &dest.data_dest;
        let result = db.lock().await.add_column(table, column).await;

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
}

pub struct ValidationSchemaManager {
    pub report: Arc<Mutex<ValidationReport>>,
}

#[async_trait::async_trait]
impl SchemaManager for ValidationSchemaManager {
    async fn add_column(
        &mut self,
        _dest: &Destination,
        table: &str,
        column: &ColumnDef,
    ) -> Result<(), SettingsError> {
        let mut report = self.report.lock().await;

        // Generate the SQL that would be executed.
        // TODO: The dialect should be determined from the destination connection.
        let (sql, _) = QueryGenerator::new(&dialect::Postgres).add_column(table, column.clone());

        report.generated_queries.ddl.push((sql, None));

        // Add an informational message to the report about the action.
        report.schema_analysis.actions.push(SchemaAction {
            code: "ACTION_ADD_COLUMN".to_string(),
            message: format!(
                "A new column '{}' will be added to the destination table '{}'.",
                column.name, table
            ),
            column: Some(column.name.clone()),
        });

        Ok(())
    }
}
