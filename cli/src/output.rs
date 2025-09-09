use engine::state::MigrationState;
use std::collections::HashMap;

pub async fn write_report(
    states: HashMap<String, MigrationState>,
    path: String,
) -> Result<(), std::io::Error> {
    let mut report = HashMap::new();
    for (name, state) in states {
        let dry_run_report = state.dry_run_report.lock().await;
        report.insert(name, dry_run_report.clone());
    }

    let json = serde_json::to_string_pretty(&report).expect("Failed to serialize report");
    std::fs::write(path, json)?;
    Ok(())
}
