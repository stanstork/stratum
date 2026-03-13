use crate::error::CliError;
use engine_planner::plan::execution::migration_report::MigrationReport;

async fn generate_report_json(plan: MigrationReport) -> Result<String, CliError> {
    let json = serde_json::to_string_pretty(&plan)?;
    Ok(json)
}

pub async fn write_report(plan: MigrationReport, path: String) -> Result<(), CliError> {
    let report_json = generate_report_json(plan).await?;
    tokio::fs::write(path, report_json).await?;
    Ok(())
}

pub async fn print_report(plan: MigrationReport) -> Result<(), CliError> {
    let report_json = generate_report_json(plan).await?;
    println!("{report_json}");
    Ok(())
}
