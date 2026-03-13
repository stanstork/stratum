use crate::hooks::error::HookError;
use connectors::traits::{executor::QueryExecutor, transaction::Transactional};
use model::execution::pipeline::LifecycleHooks;
use std::sync::Arc;
use tracing::{info, warn};

/// Executes lifecycle hooks (before/after) for pipeline operations.
///
/// Each hook is executed within its own transaction. On success, the transaction
/// is committed. On failure, the transaction is rolled back and execution stops.
pub struct HookExecutor<D: QueryExecutor + Transactional> {
    driver: Arc<D>,
    hooks: LifecycleHooks,
    /// Tracks successfully executed before hooks
    executed_before_hooks: Vec<String>,
    /// Tracks successfully executed after hooks
    executed_after_hooks: Vec<String>,
}

impl<D: QueryExecutor + Transactional> HookExecutor<D> {
    pub fn new(driver: Arc<D>, hooks: LifecycleHooks) -> Self {
        Self {
            driver,
            hooks,
            executed_before_hooks: Vec::new(),
            executed_after_hooks: Vec::new(),
        }
    }

    /// Executes all before hooks sequentially.
    pub async fn execute_before(&mut self) -> Result<(), HookError> {
        if self.hooks.before.is_empty() {
            info!("No before hooks configured, skipping execution");
            return Ok(());
        }

        info!(
            "Starting execution of {} before hook(s)",
            self.hooks.before.len()
        );
        let hooks = self.hooks.before.clone();
        for (index, sql) in hooks.iter().enumerate() {
            info!(
                "Executing before hook {}/{}: {}",
                index + 1,
                hooks.len(),
                sql
            );
            self.execute_sql(sql, index, true).await?;
            info!(
                "Before hook {}/{} completed successfully",
                index + 1,
                hooks.len()
            );
        }

        info!("All {} before hook(s) executed successfully", hooks.len());
        Ok(())
    }

    /// Executes all after hooks sequentially.
    pub async fn execute_after(&mut self) -> Result<(), HookError> {
        if self.hooks.after.is_empty() {
            info!("No after hooks configured, skipping execution");
            return Ok(());
        }

        info!(
            "Starting execution of {} after hook(s)",
            self.hooks.after.len()
        );
        let hooks = self.hooks.after.clone();
        for (index, sql) in hooks.iter().enumerate() {
            info!(
                "Executing after hook {}/{}: {}",
                index + 1,
                hooks.len(),
                sql
            );
            self.execute_sql(sql, index, false).await?;
            info!(
                "After hook {}/{} completed successfully",
                index + 1,
                hooks.len()
            );
        }

        info!("All {} after hook(s) executed successfully", hooks.len());
        Ok(())
    }

    /// Executes a single SQL hook within a transaction.
    ///
    /// The hook is executed in its own transaction. On success, the transaction is committed
    /// and the hook is recorded in the executed hooks list. On failure, the transaction is
    /// rolled back and an error is returned.
    async fn execute_sql(
        &mut self,
        sql: &str,
        index: usize,
        is_before: bool,
    ) -> Result<(), HookError> {
        let tx = self.driver.begin().await?;

        match self.driver.execute(sql).await {
            Ok(_) => {
                // Hook executed successfully - track it and commit the transaction
                if is_before {
                    self.executed_before_hooks.push(sql.to_string());
                } else {
                    self.executed_after_hooks.push(sql.to_string());
                }

                tx.commit().await.map_err(|e| HookError::ExecutionFailed {
                    index,
                    sql: sql.to_string(),
                    source: e,
                })?;
                Ok(())
            }
            Err(e) => {
                // Hook execution failed - attempt rollback
                let hook_type = if is_before { "before" } else { "after" };
                warn!(
                    "Hook execution failed, attempting rollback of {} hook #{}",
                    hook_type,
                    index + 1
                );

                if let Err(rollback_err) = tx.rollback().await {
                    warn!(
                        "Failed to rollback {} hook #{}: {}",
                        hook_type,
                        index + 1,
                        rollback_err
                    );
                } else {
                    info!("Successfully rolled back {} hook #{}", hook_type, index + 1);
                }

                Err(HookError::ExecutionFailed {
                    index,
                    sql: sql.to_string(),
                    source: e,
                })
            }
        }
    }
}
