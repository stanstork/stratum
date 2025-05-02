use super::{connection::Connection, migrate::MigrateBlock};
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

#[derive(Debug)]
pub enum Statement {
    Connection(Connection),
    Migrate(MigrateBlock),
    EOI,
}

impl StatementParser for Statement {
    fn parse(pair: Pair<Rule>) -> Self {
        match pair.as_rule() {
            Rule::connections => {
                let connection = Connection::parse(pair);
                Statement::Connection(connection)
            }
            Rule::migrate => Statement::Migrate(MigrateBlock::parse(pair)),
            _ => {
                println!("Unexpected rule: {:?}", pair.as_rule());
                Statement::EOI
            }
        }
    }
}
