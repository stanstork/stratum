use crate::{error::SmqlError, plan::MigrationPlan, statements::statement::Statement};
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "smql_v0.2.pest"]
pub struct SmqlParserV02;

pub trait StatementParser {
    fn parse(pair: Pair<Rule>) -> Self;
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
