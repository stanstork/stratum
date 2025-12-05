use pest::iterators::Pair;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grammar/smql_v0.2.pest"]
pub struct SmqlParserV02;

pub trait StatementParser {
    fn parse(pair: Pair<Rule>) -> Self;
}
