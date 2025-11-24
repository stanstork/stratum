use crate::{
    ast::expr::Expression,
    parser::{Rule, StatementParser},
};
use pest::iterators::Pair;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Offset {
    pub pairs: Vec<OffsetPair>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OffsetPair {
    pub key: OffsetKey,
    pub value: Expression,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[derive(Default)]
pub enum OffsetKey {
    #[default]
    Strategy,
    Cursor,
    TieBreaker,
    TimeZone,
}


impl Default for Offset {
    fn default() -> Self {
        Offset {
            pairs: vec![OffsetPair {
                key: OffsetKey::Strategy,
                value: Expression::Identifier("default".to_string()),
            }],
        }
    }
}

const OFFSET_KEY_STRATEGY: &str = "strategy";
const OFFSET_KEY_CURSOR: &str = "cursor";
const OFFSET_KEY_TIEBREAKER: &str = "tiebreaker";
const OFFSET_KEY_TIMEZONE: &str = "timezone";

impl StatementParser for OffsetPair {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut parts = pair.into_inner();
        let key_pair = parts.next().unwrap(); // Rule::offset_key
        let value_pair = parts.next().unwrap(); // Rule::offset_value

        let key = match key_pair.as_str().to_lowercase().as_str() {
            OFFSET_KEY_STRATEGY => OffsetKey::Strategy,
            OFFSET_KEY_CURSOR => OffsetKey::Cursor,
            OFFSET_KEY_TIEBREAKER => OffsetKey::TieBreaker,
            OFFSET_KEY_TIMEZONE => OffsetKey::TimeZone,
            _ => panic!("Unknown offset key: {}", key_pair.as_str()),
        };

        let value = Expression::parse(value_pair);

        OffsetPair { key, value }
    }
}

impl StatementParser for Offset {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut pairs = vec![];

        for inner_pair in pair.into_inner() {
            if inner_pair.as_rule() == Rule::offset_pair {
                let offset_pair = OffsetPair::parse(inner_pair);
                pairs.push(offset_pair);
            }
        }

        Offset { pairs }
    }
}
