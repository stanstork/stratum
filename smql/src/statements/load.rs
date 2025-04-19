use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

// ─────────────────────────────────────────────────────────────
// LOAD statement
// Example: LOAD customers FROM TABLE customers JOIN orders (id -> customer_id);
// ─────────────────────────────────────────────────────────────
#[derive(Debug, Clone)]
pub struct Load {
    pub name: String,
    pub source: String,
    pub join: String,
    pub mappings: Vec<(String, String)>,
}

impl StatementParser for Load {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut inner = pair.into_inner();

        let name = inner.next().expect("Expected name").as_str().to_string();
        let source = inner.next().expect("Expected source").as_str().to_string();
        let join = inner.next().expect("Expected join").as_str().to_string();

        let mut mappings = Vec::new();

        for mapping_pair in inner {
            if mapping_pair.as_rule() == Rule::load_mapping {
                let mut parts = mapping_pair.into_inner();
                let from = parts.next().unwrap().as_str().to_string();
                let to = parts.next().unwrap().as_str().to_string();
                mappings.push((from, to));
            }
        }

        Load {
            name,
            source,
            join,
            mappings,
        }
    }
}
