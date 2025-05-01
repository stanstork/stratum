#[derive(Debug, Clone)]
pub enum Expression {
    Lookup(String, String),
    BinaryOp(Box<Expression>, Operator, Box<Expression>),
    FunctionCall(String, Vec<Expression>),
    Literal(Literal),
    Identifier(String),
}

#[derive(Debug, Clone)]
pub enum Operator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}
