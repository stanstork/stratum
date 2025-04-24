#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbType {
    MySql,
    Postgres,
    Other(String),
}
