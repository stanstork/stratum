use crate::parser::Rule;
use pest::iterators::Pair;

#[derive(Debug, Clone)]
pub struct Connection {
    pub source: String,
    pub destination: String,
}

impl Connection {
    pub fn parse(pair: Pair<Rule>) -> Self {
        let mut source = String::new();
        let mut destination = String::new();

        for pair in pair.into_inner() {
            if let Rule::connection_pair = pair.as_rule() {
                // Extract key (SOURCE or DESTINATION) from the span text
                let span_text = pair.as_span().as_str();
                let key = span_text.split_whitespace().next().unwrap().to_lowercase();

                // Extract value (database URL)
                let mut inner_rules = pair.into_inner();
                let value = inner_rules
                    .next()
                    .unwrap()
                    .as_str()
                    .trim_matches('"')
                    .to_string();

                match key.as_str() {
                    "source" => source = value,
                    "destination" => destination = value,
                    _ => unreachable!(),
                }
            }
        }

        Connection {
            source,
            destination,
        }
    }
}
