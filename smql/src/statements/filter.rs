use super::expr::Expression;
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

// ─────────────────────────────────────────────────────────────
// FILTER statement
// Example: FILTER (status = "active", age > 18)
// ─────────────────────────────────────────────────────────────
#[derive(Debug, Clone)]
pub struct Filter {
    pub expresssion: FilterExpression,
}

#[derive(Debug, Clone)]
pub enum FilterExpression {
    /// e.g. table[col] > 3
    Condition(Condition),

    /// e.g. AND(expr1, expr2, …)
    FunctionCall(String, Vec<FilterExpression>),
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
        let mut inner = pair.into_inner();
        let expression = inner.next().expect("Expected filter expression");

        Filter {
            expresssion: FilterExpression::parse(expression),
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
