use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grammar/smql_v2.1.pest"]
pub struct SmqlParser;
