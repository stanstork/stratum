use super::expr::Expression;
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

#[derive(Debug, Clone)]
pub struct Load {
    pub entities: Vec<String>,
    pub matches: Vec<MatchPair>,
}

#[derive(Debug, Clone)]
pub struct MatchPair {
    pub left: Expression,
    pub right: Expression,
}

impl StatementParser for Load {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut entities = vec![];
        let mut matches = vec![];

        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::table_list => {
                    for table_pair in inner_pair
                        .into_inner()
                        .filter(|p| p.as_rule() == Rule::ident)
                    {
                        entities.push(table_pair.as_str().to_string());
                    }
                }
                Rule::match_clause => {
                    let mappings = inner_pair
                        .into_inner()
                        .filter(|p| p.as_rule() == Rule::on_mapping)
                        .map(|mapping_pair| {
                            let mut parts = mapping_pair.into_inner();
                            let left_pair = parts.next().unwrap(); // Rule::lookup_expression
                            let right_pair = parts.next().unwrap(); // Rule::lookup_expression
                            let left = Expression::parse(left_pair);
                            let right = Expression::parse(right_pair);
                            MatchPair::new(left, right)
                        })
                        .collect::<Vec<_>>();
                    matches.extend(mappings);
                }
                _ => {}
            }
        }

        Load { entities, matches }
    }
}

impl MatchPair {
    pub fn new(left: Expression, right: Expression) -> Self {
        MatchPair { left, right }
    }
}
