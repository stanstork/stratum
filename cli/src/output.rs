use engine::{report::validation::DryRunReport, state::MigrationState};
use std::collections::HashMap;

async fn collect_reports(states: HashMap<String, MigrationState>) -> HashMap<String, DryRunReport> {
    let mut report = HashMap::new();
    for (name, state) in states {
        let dry_run_report = state.dry_run_report.lock().await;
        report.insert(name, dry_run_report.clone());
    }
    report
}

pub async fn write_report(
    states: HashMap<String, MigrationState>,
    path: String,
) -> Result<(), std::io::Error> {
    let report = collect_reports(states).await;
    let json = serde_json::to_string_pretty(&report).expect("Failed to serialize report");
    std::fs::write(path, json)?;
    Ok(())
}

pub async fn print_report(states: HashMap<String, MigrationState>) -> Result<(), std::io::Error> {
    let report = collect_reports(states).await;
    let json = serde_json::to_string_pretty(&report).expect("Failed to serialize report");
    println!("{}", json);
    Ok(())
}
