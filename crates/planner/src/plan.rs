use pest::Parser;
use serde::{Deserialize, Serialize};
use smql_syntax::{
    ast::{connection::Connection, migrate::MigrateBlock, statement::Statement},
    error::SmqlError,
    parser_v2::{Rule, SmqlParserV02, StatementParser},
};

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

    pub fn hash(&self) -> String {
        let serialized = serde_json::to_string(self).expect("Failed to serialize MigrationPlan");
        format!("{:x}", md5::compute(serialized))
    }
}

pub fn parse(source: &str) -> Result<MigrationPlan, SmqlError> {
    let pairs =
        SmqlParserV02::parse(Rule::program, source).map_err(|e| SmqlError::Parse(e.to_string()))?;

    let mut statements = vec![];
    for pair in pairs {
        let statement = Statement::parse(pair);
        statements.push(statement);
    }

    Ok(MigrationPlan::from_statements(statements))
}
