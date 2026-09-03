use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grammar/ppl.pest"]
pub struct PplParser;
