use super::expr::Expression;
use crate::parser_v2::{Rule, StatementParser};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapSpec {
    pub mappings: Vec<Mapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mapping {
    pub source: Expression,
    pub target: String,
}

impl StatementParser for MapSpec {
    fn parse(pair: pest::iterators::Pair<crate::parser_v2::Rule>) -> Self {
        let mappings = pair
            .into_inner()
            .filter(|p| p.as_rule() == Rule::mapping)
            .map(|mapping_pair| {
                let mut inner = mapping_pair.into_inner();
                // left side: any Expression
                let expr = Expression::parse(inner.next().unwrap());
                // right side: target field name
                let target = inner.next().unwrap().as_str().to_string();
                Mapping::new(expr, target)
            })
            .collect();

        MapSpec { mappings }
    }
}

impl Mapping {
    pub fn new(source: Expression, target: String) -> Self {
        Mapping { source, target }
    }
}
