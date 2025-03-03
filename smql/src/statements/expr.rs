#[derive(Debug)]
pub enum Expression {
    Arithmetic {
        left: Box<Expression>,
        operator: Operator,
        right: Box<Expression>,
    },
    FunctionCall {
        name: String,
        arguments: Vec<Expression>,
    },
    Lookup {
        table: String,
        key: String,
        field: Option<String>,
    },
    Literal(Literal),
    Identifier(String),
}

#[derive(Debug)]
pub enum Operator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}
