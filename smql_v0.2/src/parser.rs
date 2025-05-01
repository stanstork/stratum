use crate::{plan::MigrationPlan, statements::statement::Statement};
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "smql_v0.2.pest"]
pub struct SmqlParserV02;

pub trait StatementParser {
    fn parse(pair: Pair<Rule>) -> Self;
}

pub fn parse(source: &str) -> Result<MigrationPlan, Box<dyn std::error::Error>> {
    let pairs = SmqlParserV02::parse(Rule::program, source)
        .map_err(|e| format!("Parsing failed: {}", e))?;

    for pair in pairs {
        let statement = Statement::parse(pair);
        println!("Parsed statement: {:#?}", statement);
    }

    todo!("Implement parsing logic for SMQL v0.2");
}
