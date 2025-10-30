use crate::error::ConsumerError;
use connectors::sql::base::metadata::table::TableMetadata;
use engine_core::connectors::destination::Destination;
use tracing::{error, info};

/// # RAII Guard for Database Triggers
/// This struct ensures that triggers are re-enabled when it goes out of scope.
pub(crate) struct TriggerGuard {
    destination: Destination,
    tables: Vec<String>,
    target_state: bool,
}

impl TriggerGuard {
    pub async fn new(
        destination: &Destination,
        tables: &[TableMetadata],
        enable: bool,
    ) -> Result<Self, ConsumerError> {
        info!(enable, "Setting triggers for all tables.");
        for table in tables {
            destination
                .toggle_trigger(&table.name, enable)
                .await
                .map_err(|e| ConsumerError::ToggleTrigger {
                    table: table.name.clone(),
                    source: Box::new(e),
                })?;
        }
        Ok(Self {
            destination: destination.clone(),
            tables: tables.iter().map(|t| t.name.clone()).collect(),
            target_state: !enable,
        })
    }
}

impl Drop for TriggerGuard {
    fn drop(&mut self) {
        let destination = self.destination.clone();
        let target_state = self.target_state;
        let tables = self.tables.clone();

        info!(enable = target_state, "Restoring triggers on drop.");

        // Since `drop` cannot be async, we must spawn a task to run the async code.
        tokio::spawn(async move {
            for table in tables.iter() {
                if let Err(e) = destination.toggle_trigger(table, target_state).await {
                    error!(
                        table = %table,
                        error = %e,
                        "Failed to restore trigger state for table."
                    );
                }
            }
        });
    }
}
