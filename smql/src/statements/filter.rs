use super::expr::Expression;

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
