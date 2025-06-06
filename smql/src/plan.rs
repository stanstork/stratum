use crate::statements::{connection::Connection, migrate::MigrateBlock, statement::Statement};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub connections: Connection,
    pub migration: MigrateBlock,
}

impl MigrationPlan {
    pub fn from_statements(statements: Vec<Statement>) -> Self {
        let mut connections = None;
        let mut migration = None;

        for statement in statements {
            match statement {
                Statement::Connection(c) => connections = Some(c),
                Statement::Migrate(m) => migration = Some(m),
                _ => {}
            }
        }

        MigrationPlan {
            connections: connections
                .expect("`CONNECTIONS` statement is required to build a MigrationPlan"),
            migration: migration.expect("`MIGRATE` statement is required to build a MigrationPlan"),
        }
    }
}
