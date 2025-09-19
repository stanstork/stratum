use common::value::Value;
use serde::Serialize;

/// The type of SQL statement generated.
#[derive(Serialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum SqlKind {
    Schema,
    #[default]
    Data,
}

/// A single generated SQL statement.
#[derive(Serialize, Debug, Clone)]
pub struct SqlStatement {
    pub dialect: String, // "MySQL", "Postgres", ...
    pub kind: SqlKind,   // Schema | Data
    pub sql: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<Value>, // normalized; empty if none
}

impl SqlStatement {
    pub fn schema_action(dialect: &str, sql: &str) -> Self {
        SqlStatement {
            dialect: dialect.to_string(),
            kind: SqlKind::Schema,
            sql: sql.to_string(),
            params: vec![],
        }
    }
}

/// A collection of generated SQL statements for the dry run.
#[derive(Serialize, Debug, Default, Clone)]
pub struct GeneratedSql {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub statements: Vec<SqlStatement>,
}

impl GeneratedSql {
    pub fn add_statement(&mut self, dialect: &str, kind: SqlKind, sql: &str, params: Vec<Value>) {
        self.statements.push(SqlStatement {
            dialect: dialect.to_string(),
            kind,
            sql: sql.to_string(),
            params,
        });
    }
}
