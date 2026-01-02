use crate::error::CliError;
use engine_planner::plan::execution::execution_plan::ExecutionPlan;

async fn generate_report_json(plan: ExecutionPlan) -> Result<String, CliError> {
    let json = serde_json::to_string_pretty(&plan)?;
    Ok(json)
}

pub async fn write_report(plan: ExecutionPlan, path: String) -> Result<(), CliError> {
    let report_json = generate_report_json(plan).await?;
    tokio::fs::write(path, report_json).await?;
    Ok(())
}

pub async fn print_report(plan: ExecutionPlan) -> Result<(), CliError> {
    let report_json = generate_report_json(plan).await?;
    println!("{report_json}");
    Ok(())
}
