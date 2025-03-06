use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

// ─────────────────────────────────────────────────────────────
// LOAD statement
// Example: LOAD users FROM users USING user_id
// ─────────────────────────────────────────────────────────────
#[derive(Debug)]
pub struct Load {
    pub name: String,
    pub source: String,
    pub key: String,
}

impl StatementParser for Load {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut inner = pair.into_inner();

        let name = inner.next().expect("Expected name").as_str().to_string();
        let source = inner
            .next()
            .expect("Expected source table name")
            .as_str()
            .to_string();
        let key = inner
            .next()
            .expect("Expected table key")
            .as_str()
            .to_string();

        Load { name, source, key }
    }
}
