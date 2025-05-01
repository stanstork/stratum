use super::expr::Expression;

#[derive(Debug, Clone)]
pub enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Condition(Condition),
}

#[derive(Debug, Clone)]
pub struct Condition {
    pub left: Expression,
    pub op: Comparator,
    pub right: Expression,
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
