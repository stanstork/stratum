use crate::connection::Connection;
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "smql.pest"]
pub struct SmqlParser;

pub fn parse(source: &str) -> Result<(), Box<dyn std::error::Error>> {
    let pairs = SmqlParser::parse(Rule::program, source)?;

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
