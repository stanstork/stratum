use super::open_state_store;
use crate::{config, error::CliError};
use chrono::{DateTime, Utc};
use engine_processing::EnvContext;
use engine_state::{
    error::StateStoreError,
    models::{PipelineStatus, RunState, RunStatus},
    sled_store::SledStateStore,
    store::StateStore,
};
use std::sync::Arc;

const DATE_FORMAT: &str = "%Y-%m-%d %H:%M:%S UTC";

pub async fn execute(config_path: Option<String>, env: Arc<EnvContext>) -> Result<(), CliError> {
    let state = open_state_store()?;

    if let Some(path) = config_path {
        show_config_status(&state, &path, env).await
    } else {
        show_all_runs(&state).await
    }
}

async fn show_config_status(
    state: &SledStateStore,
    config_path: &str,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    let resolved = config::resolve_path(Some(config_path.to_string()))?;
    let plan = config::load_plan(&resolved, false, env).await?;
    let run_id = plan.run_id();

    match state.load_run_state(&run_id).await.map_err(state_err)? {
        Some(run) => print_run_detail(&run),
        None => println!("No run found for config '{resolved}' (run_id: {run_id})"),
    }

    Ok(())
}

async fn show_all_runs(state: &SledStateStore) -> Result<(), CliError> {
    let mut runs = state.list_runs().await.map_err(state_err)?;

    if runs.is_empty() {
        println!("No migration runs found.");
        return Ok(());
    }

    // Sort: paused first, then running, failed, completed. Then by started_at descending.
    runs.sort_by(|a, b| {
        status_rank(&a.status)
            .cmp(&status_rank(&b.status))
            .then_with(|| b.started_at.cmp(&a.started_at))
    });

    println!(
        "{:<20} {:<12} {:<10} {:<12} {:<24} CONFIG",
        "RUN ID", "STATUS", "PIPELINES", "DURATION", "STARTED"
    );
    println!("{}", "-".repeat(100));

    for run in &runs {
        let completed = run
            .pipelines
            .iter()
            .filter(|p| p.status == PipelineStatus::Completed)
            .count();

        let config = if run.config_path.is_empty() {
            "-"
        } else {
            &run.config_path
        };

        println!(
            "{:<20} {:<12} {:<10} {:<12} {:<24} {}",
            run.run_id,
            format_status(&run.status),
            format!("{completed}/{}", run.total_pipelines),
            format_duration(&run.status, run.started_at),
            run.started_at.format(DATE_FORMAT),
            config,
        );
    }

    Ok(())
}

fn print_run_detail(run: &RunState) {
    println!("Run:      {}", run.run_id);
    println!("Status:   {}", format_status(&run.status));
    if !run.config_path.is_empty() {
        println!("Config:   {}", run.config_path);
    }
    println!("Started:  {}", run.started_at.format(DATE_FORMAT));
    println!("Duration: {}", format_duration(&run.status, run.started_at));

    match &run.status {
        RunStatus::Paused {
            paused_at, reason, ..
        } => {
            println!("Paused:   {} ({reason:?})", paused_at.format(DATE_FORMAT));
        }
        RunStatus::Completed { completed_at } => {
            println!("Finished: {}", completed_at.format(DATE_FORMAT));
        }
        RunStatus::Failed { error, failed_at } => {
            println!("Failed:   {} — {error}", failed_at.format(DATE_FORMAT));
        }
        RunStatus::Running => {}
    }

    println!("\n  {:<30} {:<12} {:>10}", "PIPELINE", "STATUS", "ROWS");
    println!("  {}", "-".repeat(54));

    for p in &run.pipelines {
        let status_str = match p.status {
            PipelineStatus::Pending => "pending",
            PipelineStatus::Running => "running",
            PipelineStatus::Completed => "completed",
            PipelineStatus::Failed { .. } => "failed",
            PipelineStatus::Blocked => "blocked",
        };

        let rows = match p.rows_done {
            0 => "-".to_string(),
            n => n.to_string(),
        };

        println!("  {:<30} {status_str:<12} {rows:>10}", p.name);
    }
}

fn format_status(status: &RunStatus) -> &'static str {
    match status {
        RunStatus::Running => "running",
        RunStatus::Paused { .. } => "paused",
        RunStatus::Completed { .. } => "completed",
        RunStatus::Failed { .. } => "failed",
    }
}

fn format_duration(status: &RunStatus, started_at: DateTime<Utc>) -> String {
    let end = match status {
        RunStatus::Paused { paused_at, .. } => *paused_at,
        RunStatus::Completed { completed_at } => *completed_at,
        RunStatus::Failed { failed_at, .. } => *failed_at,
        RunStatus::Running => Utc::now(),
    };

    let secs = (end - started_at).num_seconds().max(0);
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

fn state_err(e: StateStoreError) -> CliError {
    CliError::Unknown(format!("State store error: {e}"))
}

fn status_rank(status: &RunStatus) -> u8 {
    match status {
        RunStatus::Paused { .. } => 0,
        RunStatus::Running => 1,
        RunStatus::Failed { .. } => 2,
        RunStatus::Completed { .. } => 3,
    }
}
