use smql::plan::MigrationPlan;

pub fn run(plan: MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
    println!("{:#?}", plan);
    todo!()
}
