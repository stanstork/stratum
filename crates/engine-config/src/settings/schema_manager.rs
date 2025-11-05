use crate::{
    report::{
        dry_run::DryRunReport,
        schema::SchemaAction,
        sql::{SqlKind, SqlStatement},
    },
    settings::error::SettingsError,
};
use async_trait::async_trait;
use connectors::sql::base::query::{column::ColumnDef, generator::QueryGenerator};
use engine_core::{
    connectors::destination::{DataDestination, Destination},
    migration_state::MigrationSettings,
    schema::plan::SchemaPlan,
};
use futures::lock::Mutex;
use planner::query::dialect::{self, Dialect};
use std::sync::Arc;
use tracing::{error, info};

#[async_trait]
pub trait SchemaManager: Send + Sync {
    async fn add_column(&mut self, table: &str, column: &ColumnDef) -> Result<(), SettingsError>;
    async fn infer_schema(&mut self, schema_plan: &SchemaPlan) -> Result<(), SettingsError>;
}

pub struct LiveSchemaManager {
    pub destination: Arc<Mutex<Destination>>,
}

#[async_trait]
impl SchemaManager for LiveSchemaManager {
    async fn add_column(&mut self, table: &str, column: &ColumnDef) -> Result<(), SettingsError> {
        let dest = self.destination.lock().await;
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

    async fn infer_schema(&mut self, schema_plan: &SchemaPlan) -> Result<(), SettingsError> {
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
            if let Err(err) = dest.data_dest.adapter().await.execute(&query.0).await {
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
}

pub struct ValidationSchemaManager {
    pub report: Arc<Mutex<DryRunReport>>,
    pub settings: MigrationSettings,
}

#[async_trait::async_trait]
impl SchemaManager for ValidationSchemaManager {
    async fn add_column(&mut self, table: &str, column: &ColumnDef) -> Result<(), SettingsError> {
        if self.settings.infer_schema() {
            info!(
                "Skipping add_column for '{}' on table '{}' due to infer_schema being enabled.",
                column.name, table
            );
            return Ok(());
        }

        let dialect = dialect::Postgres; // TODO: derive from destination connection
        let (sql, params) = QueryGenerator::new(&dialect).add_column(table, column.clone());

        {
            let mut report = self.report.lock().await;
            report
                .generated_sql
                .add_statement(&dialect.name(), SqlKind::Schema, &sql, params);
            report
                .schema
                .actions
                .push(SchemaAction::add_column(table, column.name()));
        }

        Ok(())
    }

    async fn infer_schema(&mut self, schema_plan: &SchemaPlan) -> Result<(), SettingsError> {
        let dialect = dialect::Postgres; // TODO: Determine dialect from destination connection

        let enum_queries = schema_plan.enum_queries().await?;
        let table_queries = schema_plan.table_queries().await;
        let fk_queries = schema_plan.fk_queries();

        let enum_actions = enum_queries
            .iter()
            .map(|query| (SchemaAction::create_enum(&query.1), query));
        let table_actions = table_queries
            .iter()
            .map(|query| (SchemaAction::create_table(&query.1), query));
        let fk_actions = fk_queries
            .iter()
            .map(|query| (SchemaAction::add_foreign_key(&query.1), query));

        let mut statements =
            Vec::with_capacity(enum_queries.len() + table_queries.len() + fk_queries.len());
        let mut actions = Vec::with_capacity(statements.capacity());

        for (action, query) in enum_actions.chain(table_actions).chain(fk_actions) {
            statements.push(SqlStatement::schema_action(&dialect.name(), &query.0));
            actions.push(action);
        }

        {
            let mut report = self.report.lock().await;
            report.generated_sql.statements.extend(statements);
            report.schema.actions.extend(actions);
        }

        // Mark that schema inference has been performed.
        {
            let mut settings = self.settings.clone();
            settings.set_infer_schema(true);
        }

        Ok(())
    }
}
