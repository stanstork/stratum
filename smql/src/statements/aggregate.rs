use super::expr::Expression;

#[derive(Debug)]
pub struct Aggregate {
    pub aggregations: Vec<Aggregation>,
}

#[derive(Debug)]
pub struct Aggregation {
    pub function: AggregateFunction,
    pub column: Expression,
    pub target: String,
}

#[derive(Debug)]
pub enum AggregateFunction {
    Count,
    Sum,
    Average,
    Min,
    Max,
}
