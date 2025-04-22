use super::expr::Expression;
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

// ─────────────────────────────────────────────────────────────
// FILTER statement
// Example: FILTER (status = "active", age > 18)
// ─────────────────────────────────────────────────────────────
#[derive(Debug, Clone)]
pub struct Filter {
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone)]
pub struct Condition {
    pub field: Expression,
    pub comparator: Comparator,
    pub value: Expression,
}

#[derive(Debug, Clone)]
pub enum Comparator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

impl StatementParser for Filter {
    fn parse(pair: Pair<Rule>) -> Self {
        let conditions = pair
            .into_inner()
            .filter(|p| p.as_rule() == Rule::condition)
            .map(Condition::parse)
            .collect();

        Filter { conditions }
    }
}

impl StatementParser for Condition {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut inner = pair.clone().into_inner();

        let field = Expression::parse(inner.next().expect("Expected field identifier"));
        let comparator = Comparator::parse(inner.next().expect("Expected comparator"));
        let value = Expression::parse(inner.next().expect("Expected value"));

        Condition {
            field,
            comparator,
            value,
        }
    }
}

impl StatementParser for Comparator {
    fn parse(pair: Pair<Rule>) -> Self {
        match pair.as_str() {
            "=" => Comparator::Equal,
            "!=" => Comparator::NotEqual,
            ">" => Comparator::GreaterThan,
            ">=" => Comparator::GreaterThanOrEqual,
            "<" => Comparator::LessThan,
            "<=" => Comparator::LessThanOrEqual,
            _ => panic!("Invalid comparator: {:?}", pair.as_str()),
        }
    }
}
