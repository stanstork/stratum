use common::value::Value;
use serde::Serialize;

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub enum SqlKind {
    Schema,
    Data,
}

impl Default for SqlKind {
    fn default() -> Self {
        SqlKind::Data
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct SqlStatement {
    pub dialect: String, // "MySQL", "Postgres", ...
    pub kind: SqlKind,   // Schema | Data
    pub sql: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<Value>, // normalized; empty if none
}

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
