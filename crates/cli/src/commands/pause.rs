use super::state_dir;
use crate::{config, error::CliError};
use engine_processing::EnvContext;
use std::sync::Arc;

/// Requests a pause by creating a sentinel file that the running migration watches for.
pub async fn execute(config_path: Option<String>, env: Arc<EnvContext>) -> Result<(), CliError> {
    let config_path = config::resolve_path(config_path)?;
    let plan = config::load_plan(&config_path, false, env).await?;
    let run_id = plan.run_id();

    let pause_path = state_dir()?.join(format!("{run_id}.pause"));

    if let Err(e) = std::fs::create_dir_all(pause_path.parent().unwrap()) {
        return Err(CliError::UserMessage(format!(
            "Failed to create state directory: {e}"
        )));
    }

    std::fs::write(&pause_path, "")
        .map_err(|e| CliError::UserMessage(format!("Failed to write pause file: {e}")))?;

    println!("Pause requested for migration (run_id: {run_id})");
    println!("The migration will pause at the next batch boundary.");
    println!("Check status with: stratum status -c {config_path}");

    Ok(())
}
