use crate::statements::{
    connection::Connection, migrate::MigrateBlock, setting::Settings, statement::Statement,
};

#[derive(Debug, Clone)]
pub struct MigrationPlan {
    pub connections: Connection,
    pub migration: MigrateBlock,
    pub global_settings: Option<Settings>,
}

impl MigrationPlan {
    pub fn from_statements(statements: Vec<Statement>) -> Self {
        let mut connections = None;
        let mut migration = None;
        let mut global_settings = None;

        for statement in statements {
            match statement {
                Statement::Connection(c) => connections = Some(c),
                Statement::Migrate(m) => migration = Some(m),
                Statement::GlobalSettings(s) => global_settings = Some(s),
                _ => {}
            }
        }

        MigrationPlan {
            connections: connections
                .expect("`CONNECTIONS` statement is required to build a MigrationPlan"),
            migration: migration.expect("`MIGRATE` statement is required to build a MigrationPlan"),
            global_settings,
        }
    }
}
