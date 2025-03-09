use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

#[derive(Debug, Clone)]
pub struct Setting {
    pub key: String,
    pub value: SettingValue,
}

#[derive(Debug, Clone)]
pub enum SettingValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}

impl StatementParser for Setting {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut inner = pair.into_inner();

        let key = inner
            .next()
            .expect("Expected setting key")
            .as_str()
            .to_string();

        let value_pair = inner.next().expect("Expected setting value");

        let value = match value_pair.as_rule() {
            Rule::boolean => {
                SettingValue::Boolean(value_pair.as_str().eq_ignore_ascii_case("true"))
            }
            Rule::string => SettingValue::String(value_pair.as_str().to_string()),
            Rule::integer => {
                SettingValue::Integer(value_pair.as_str().parse().expect("Invalid integer"))
            }
            Rule::decimal => {
                SettingValue::Float(value_pair.as_str().parse().expect("Invalid float"))
            }
            _ => panic!("Invalid setting value: {:?}", value_pair),
        };

        Setting { key, value }
    }
}
