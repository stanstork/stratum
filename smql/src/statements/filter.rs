use super::expr::Expression;
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    pub expression: FilterExpression,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterExpression {
    /// e.g. table[col] > 3
    Condition(Condition),

    /// e.g. AND(expr1, expr2, …)
    FunctionCall(String, Vec<FilterExpression>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    pub left: Expression,
    pub op: Comparator,
    pub right: Expression,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        let mut inner = pair.into_inner();
        let expression = inner.next().expect("Expected filter expression");

        Filter {
            expression: FilterExpression::parse(expression),
        }
    }
}

impl StatementParser for FilterExpression {
    fn parse(pair: Pair<Rule>) -> Self {
        match pair.as_rule() {
            Rule::condition => FilterExpression::Condition(Condition::parse(pair)),
            Rule::filter_func_call => {
                let mut inner = pair.into_inner();
                let function_name = inner
                    .next()
                    .expect("Expected function name")
                    .as_str()
                    .to_string();
                let args = inner.map(FilterExpression::parse).collect();
                FilterExpression::FunctionCall(function_name, args)
            }
            _ => panic!("Unexpected rule: {:?}", pair.as_rule()),
        }
    }
}

impl StatementParser for Condition {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut inner = pair.clone().into_inner();

        let left = Expression::parse(inner.next().expect("Expected field identifier"));
        let op = Comparator::parse(inner.next().expect("Expected comparator"));
        let right = Expression::parse(inner.next().expect("Expected value"));

        Condition { left, op, right }
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

impl Display for Comparator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op = match self {
            Comparator::Equal => "=",
            Comparator::NotEqual => "!=",
            Comparator::GreaterThan => ">",
            Comparator::GreaterThanOrEqual => ">=",
            Comparator::LessThan => "<",
            Comparator::LessThanOrEqual => "<=",
        };
        write!(f, "{op}")
    }
}
