use super::expr::Expression;
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

// ─────────────────────────────────────────────────────────────
// FILTER statement
// Example: FILTER (status = "active", age > 18)
// ─────────────────────────────────────────────────────────────
#[derive(Debug)]
pub struct Filter {
    pub conditions: Vec<Condition>,
}

#[derive(Debug)]
pub struct Condition {
    pub field: String,
    pub comparator: Comparator,
    pub value: Expression,
}

#[derive(Debug)]
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

        let field = inner
            .next()
            .expect("Expected field identifier")
            .as_str()
            .to_string();
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
