use crate::hooks::error::HookError;
use connectors::traits::{executor::QueryExecutor, transaction::Transactional};
use model::execution::pipeline::LifecycleHooks;
use std::sync::Arc;
use tracing::{debug, info, warn};

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
            debug!("no before hooks configured");
            return Ok(());
        }

        info!(count = self.hooks.before.len(), "running before hooks");
        let hooks = self.hooks.before.clone();
        for (index, sql) in hooks.iter().enumerate() {
            debug!(hook = index + 1, total = hooks.len(), sql = %sql, "executing before hook");
            self.execute_sql(sql, index, true).await?;
            debug!(
                hook = index + 1,
                total = hooks.len(),
                "before hook completed"
            );
        }

        info!(count = hooks.len(), "before hooks completed");
        Ok(())
    }

    /// Executes all after hooks sequentially.
    pub async fn execute_after(&mut self) -> Result<(), HookError> {
        if self.hooks.after.is_empty() {
            debug!("no after hooks configured");
            return Ok(());
        }

        info!(count = self.hooks.after.len(), "running after hooks");
        let hooks = self.hooks.after.clone();
        for (index, sql) in hooks.iter().enumerate() {
            debug!(hook = index + 1, total = hooks.len(), sql = %sql, "executing after hook");
            self.execute_sql(sql, index, false).await?;
            debug!(
                hook = index + 1,
                total = hooks.len(),
                "after hook completed"
            );
        }

        info!(count = hooks.len(), "after hooks completed");
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
                    hook = hook_type,
                    index = index + 1,
                    "hook execution failed, rolling back"
                );

                if let Err(rollback_err) = tx.rollback().await {
                    warn!(
                        hook = hook_type,
                        index = index + 1,
                        error = %rollback_err,
                        "failed to roll back hook"
                    );
                } else {
                    debug!(hook = hook_type, index = index + 1, "rolled back hook");
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
