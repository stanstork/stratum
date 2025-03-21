use super::expr::Expression;
use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;

// ─────────────────────────────────────────────────────────────
// AGGREGATE statement
// Example: AGGREGATE (COUNT(*) -> total_orders, SUM(price) -> total_revenue)
// ─────────────────────────────────────────────────────────────
#[derive(Debug)]
pub struct Aggregate {
    pub aggregations: Vec<Aggregation>,
}

#[derive(Debug, Clone)]
pub struct Aggregation {
    pub function: AggregateFunction,
    pub column: Expression,
    pub target: String,
}

#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count,
    Sum,
    Average,
    Min,
    Max,
}

impl StatementParser for Aggregate {
    fn parse(pair: Pair<Rule>) -> Self {
        let aggregations = pair
            .into_inner()
            .filter(|p| p.as_rule() == Rule::aggregation)
            .map(Aggregation::parse)
            .collect();

        Aggregate { aggregations }
    }
}

impl StatementParser for Aggregation {
    fn parse(pair: Pair<Rule>) -> Self {
        let mut inner = pair.into_inner();

        let function_pair = inner.next().expect("Expected aggregate function");
        let function = AggregateFunction::parse(function_pair);

        let arg_pair = inner.next().expect("Expected arg name");
        let column = Expression::Identifier(arg_pair.as_str().to_string());

        let target = inner
            .next()
            .expect("Expected target column name")
            .as_str()
            .to_string();

        Aggregation {
            function,
            column,
            target,
        }
    }
}

impl StatementParser for AggregateFunction {
    fn parse(pair: Pair<Rule>) -> Self {
        match pair.as_str().to_uppercase().as_str() {
            "COUNT" => AggregateFunction::Count,
            "SUM" => AggregateFunction::Sum,
            "AVG" => AggregateFunction::Average,
            "MIN" => AggregateFunction::Min,
            "MAX" => AggregateFunction::Max,
            _ => panic!("Invalid aggregate function: {:?}", pair.as_str()),
        }
    }
}
