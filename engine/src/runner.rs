use smql::plan::MigrationPlan;
use sql_adapter::{get_db_adapter, DbEngine};

pub async fn run(plan: MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
    let source_adapter = get_db_adapter(
        DbEngine::from_data_format(plan.connections.source.data_format),
        &plan.connections.source.con_str,
    )
    .await?;
    let settings = plan.migration.settings.clone();

    println!("Running migration");

    todo!()
}
