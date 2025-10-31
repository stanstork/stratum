use crate::error::CliError;
use engine_config::report::summary::SummaryReport;
use std::collections::HashMap;

async fn generate_report_json(states: HashMap<String, SummaryReport>) -> Result<String, CliError> {
    let json = serde_json::to_string_pretty(&states)?;
    Ok(json)
}

pub async fn write_report(
    states: HashMap<String, SummaryReport>,
    path: String,
) -> Result<(), CliError> {
    let report_json = generate_report_json(states).await?;
    tokio::fs::write(path, report_json).await?;
    Ok(())
}

pub async fn print_report(states: HashMap<String, SummaryReport>) -> Result<(), CliError> {
    let report_json = generate_report_json(states).await?;
    println!("{report_json}");
    Ok(())
}
