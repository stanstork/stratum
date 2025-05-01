use super::{connection::Connection, migrate::MigrateBlock};
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

#[derive(Debug)]
pub enum Statement {
    Connection(Vec<Connection>),
    Migrate(MigrateBlock),
    EOI,
}

impl StatementParser for Statement {
    fn parse(pair: Pair<Rule>) -> Self {
        match pair.as_rule() {
            Rule::connections => {
                let inner = pair.into_inner();
                let mut connections = Vec::new();
                for connection_pair in inner {
                    if connection_pair.as_rule() == Rule::connection_pair {
                        connections.push(Connection::parse(connection_pair));
                    }
                }
                Statement::Connection(connections)
            }
            Rule::migrate => Statement::Migrate(MigrateBlock::parse(pair)),
            _ => {
                println!("Unexpected rule: {:?}", pair.as_rule());
                Statement::EOI
            }
        }
    }
}
