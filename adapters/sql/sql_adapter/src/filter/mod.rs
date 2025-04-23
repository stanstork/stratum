#[derive(Debug, Clone)]
pub struct Condition {
    pub table: String,
    pub column: String,
    pub comparator: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct SqlFilter {
    pub conditions: Vec<Condition>,
}
