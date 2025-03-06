use crate::statements::{
    aggregate::Aggregate, connection::Connection, filter::Filter, load::Load, mapping::Map,
    migrate::Migrate, statement::Statement,
};
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "smql.pest"]
pub struct SmqlParser;

pub trait StatementParser {
    fn parse(pair: Pair<Rule>) -> Self;
}

pub fn parse(source: &str) -> Result<Vec<Statement>, Box<dyn std::error::Error>> {
    let mut statements = vec![];
    let pairs =
        SmqlParser::parse(Rule::program, source).map_err(|e| format!("Parsing failed: {}", e))?;

    for pair in pairs {
        let statement = match pair.as_rule() {
            Rule::connections => Statement::Connections(Connection::parse(pair)),
            Rule::migrate => Statement::Migrate(Migrate::parse(pair)),
            Rule::filter => Statement::Filter(Filter::parse(pair)),
            Rule::load => Statement::Load(Load::parse(pair)),
            Rule::map => Statement::Map(Map::parse(pair)),
            Rule::aggregate => Statement::Aggregate(Aggregate::parse(pair)),
            Rule::EOI => continue,
            _ => return Err(format!("Unexpected rule: {:?}", pair.as_rule()).into()),
        };

        statements.push(statement);
    }

    Ok(statements)
}
