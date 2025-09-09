use crate::{
    destination::{data::DataDestination, Destination},
    report::validation::{DryRunReport, SchemaAction, SqlKind, SqlStatement},
    schema::plan::SchemaPlan,
    settings::error::SettingsError,
};
use query_builder::dialect::{self, Dialect};
use sql_adapter::{
    error::db::DbError,
    query::{column::ColumnDef, generator::QueryGenerator},
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

#[async_trait::async_trait]
pub trait SchemaManager: Send + Sync {
    async fn add_column(&mut self, table: &str, column: &ColumnDef) -> Result<(), SettingsError>;
    async fn infer_schema(&mut self, schema_plan: &SchemaPlan) -> Result<(), DbError>;
}

pub struct LiveSchemaManager {
    pub destination: Arc<Mutex<Destination>>,
}

#[async_trait::async_trait]
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

    async fn infer_schema(&mut self, schema_plan: &SchemaPlan) -> Result<(), DbError> {
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
                return Err(err);
            }
        }

        info!("Schema inference completed and applied successfully.");
        Ok(())
    }
}

pub struct ValidationSchemaManager {
    pub report: Arc<Mutex<DryRunReport>>,
}

#[async_trait::async_trait]
impl SchemaManager for ValidationSchemaManager {
    async fn add_column(&mut self, table: &str, column: &ColumnDef) -> Result<(), SettingsError> {
        let dialect = dialect::Postgres; // TODO: derive from destination connection
        let (sql, params) = QueryGenerator::new(&dialect).add_column(table, column.clone());

        let stmt = SqlStatement {
            dialect: dialect.name(),
            kind: SqlKind::Schema,
            sql,
            params,
        };
        let action = SchemaAction {
            code: "ACTION_ADD_COLUMN".to_string(),
            message: format!(
                "A new column '{}' will be added to the destination table '{}'.",
                column.name, table
            ),
            entity: Some(format!("{}.{}", table, column.name)), // slightly richer context
        };

        {
            let mut report = self.report.lock().await;
            report.generated_sql.statements.push(stmt);
            report.schema.actions.push(action);
        }

        Ok(())
    }

    async fn infer_schema(&mut self, schema_plan: &SchemaPlan) -> Result<(), DbError> {
        let dialect = dialect::Postgres; // TODO: Determine dialect from destination connection

        let enum_queries = schema_plan.enum_queries().await?;
        let table_queries = schema_plan.table_queries().await;
        let fk_queries = schema_plan.fk_queries();

        let enum_actions = enum_queries
            .iter()
            .map(|query| ("ACTION_CREATE_ENUM", "An ENUM type will be created.", query));
        let table_actions = table_queries
            .iter()
            .map(|query| ("ACTION_CREATE_TABLE", "A new table will be created.", query));
        let fk_actions = fk_queries.iter().map(|query| {
            (
                "ACTION_ADD_FOREIGN_KEY",
                "A foreign key constraint will be added.",
                query,
            )
        });

        let mut statements =
            Vec::with_capacity(enum_queries.len() + table_queries.len() + fk_queries.len());
        let mut actions = Vec::with_capacity(statements.capacity());

        for (code, message, query) in enum_actions.chain(table_actions).chain(fk_actions) {
            statements.push(SqlStatement {
                dialect: dialect.name(),
                kind: SqlKind::Schema,
                sql: query.0.clone(),
                params: vec![],
            });
            actions.push(SchemaAction {
                code: code.to_string(),
                message: message.to_string(),
                entity: Some(query.1.clone()),
            });
        }

        {
            let mut report = self.report.lock().await;
            report.generated_sql.statements.extend(statements);
            report.schema.actions.extend(actions);
        }

        Ok(())
    }
}
