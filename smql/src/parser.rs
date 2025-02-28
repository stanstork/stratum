use pest::Parser;
use pest_derive::Parser;

use crate::statements::connection::Connection;

#[derive(Parser)]
#[grammar = "smql.pest"]
pub struct SmqlParser;

pub fn parse(source: &str) -> Result<(), Box<dyn std::error::Error>> {
    let pairs = SmqlParser::parse(Rule::program, source)?;

    println!("Pairs: {:#?}", pairs);
    for pair in pairs.clone() {
        println!("Rule: {:#?}", pair.as_rule());
        println!("Span: {:#?}", pair.as_span());
        println!("As String: {:#?}", pair.as_str());
    }

    for pair in pairs {
        match pair.as_rule() {
            Rule::connections => {
                let connections = Connection::parse(pair);
                println!("{:#?}", connections);
            }
            _ => {}
        }
    }

    Ok(())
}
