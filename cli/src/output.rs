use crate::error::CliError;
use engine::{report::dry_run::DryRunReport, state::MigrationState};
use futures_util::future::join_all;
use std::collections::HashMap;

async fn collect_reports(states: HashMap<String, MigrationState>) -> HashMap<String, DryRunReport> {
    let report_futures = states.into_iter().map(|(name, state)| async move {
        let binding = state.dry_run_report();
        let report = binding.lock().await;
        (name, report.clone())
    });

    join_all(report_futures).await.into_iter().collect()
}

async fn generate_report_json(states: HashMap<String, MigrationState>) -> Result<String, CliError> {
    let report = collect_reports(states).await;
    let json = serde_json::to_string_pretty(&report)?;
    Ok(json)
}

pub async fn write_report(
    states: HashMap<String, MigrationState>,
    path: String,
) -> Result<(), CliError> {
    let report_json = generate_report_json(states).await?;
    tokio::fs::write(path, report_json).await?;
    Ok(())
}

pub async fn print_report(states: HashMap<String, MigrationState>) -> Result<(), CliError> {
    let report_json = generate_report_json(states).await?;
    println!("{}", report_json);
    Ok(())
}
