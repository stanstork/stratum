use crate::statements::statement::Statement;

#[derive(Debug, Clone)]
pub struct MigrationPlan {}

impl MigrationPlan {
    pub fn from_statements(statements: Vec<Statement>) -> Self {
        // let mut connections = None;
        // let mut migration = None;
        // let mut filter = None;
        // let mut mapping = vec![];
        // let mut aggregations = vec![];
        // let mut loads = vec![];

        todo!("Implement logic to parse and organize statements into a migration plan");
    }
}
