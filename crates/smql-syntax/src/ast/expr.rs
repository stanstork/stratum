use crate::parser::{Rule, StatementParser};
use pest::iterators::Pair;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        entity: String,
        key: String,
        field: Option<String>,
    },
    Literal(Literal),
    Identifier(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}

impl StatementParser for Expression {
    fn parse(pair: Pair<Rule>) -> Self {
        match pair.as_rule() {
            Rule::expression => {
                let inner_pair = pair.into_inner().next().expect("Expected inner expression");
                Expression::parse(inner_pair) // Recursively parse the actual expression inside
            }
            Rule::arithmetic_expression => {
                let mut inner = pair.into_inner();
                let left = Expression::parse(inner.next().expect("Expected left operand"));
                let operator = Operator::parse(inner.next().expect("Expected operator"));
                let right = Expression::parse(inner.next().expect("Expected right operand"));

                Expression::Arithmetic {
                    left: Box::new(left),
                    operator,
                    right: Box::new(right),
                }
            }
            Rule::function_call => {
                let mut inner = pair.into_inner();
                let name = inner
                    .next()
                    .expect("Expected function name")
                    .as_str()
                    .to_string();

                // Extract function arguments
                let args_pair = inner.next().expect("Expected function arguments");
                if args_pair.as_rule() != Rule::function_args {
                    panic!("Unexpected function argument structure: {args_pair:?}");
                }

                let arguments = args_pair.into_inner().map(Expression::parse).collect();

                Expression::FunctionCall { name, arguments }
            }
            Rule::lookup_expression => {
                let mut inner = pair.into_inner();
                let entity = inner
                    .next()
                    .expect("Expected lookup table name")
                    .as_str()
                    .to_string();
                let key = inner
                    .next()
                    .expect("Expected lookup key")
                    .as_str()
                    .to_string();
                let field = inner.next().map(|p| p.as_str().to_string());

                Expression::Lookup { entity, key, field }
            }
            Rule::ident => Expression::Identifier(pair.as_str().to_string()),
            Rule::string => Expression::Literal(Literal::String(
                pair.as_str()
                    .trim_start_matches('"')
                    .trim_end_matches('"')
                    .to_string(),
            )),
            Rule::integer => Expression::Literal(Literal::Integer(
                pair.as_str().parse().expect("Invalid integer"),
            )),
            Rule::decimal => Expression::Literal(Literal::Float(
                pair.as_str().parse().expect("Invalid float"),
            )),
            Rule::boolean => {
                Expression::Literal(Literal::Boolean(pair.as_str().eq_ignore_ascii_case("true")))
            }
            _ => panic!("Unexpected expression type: {pair:?}"),
        }
    }
}

impl StatementParser for Operator {
    fn parse(pair: Pair<Rule>) -> Self {
        match pair.as_str() {
            "+" => Operator::Add,
            "-" => Operator::Subtract,
            "*" => Operator::Multiply,
            "/" => Operator::Divide,
            _ => panic!("Invalid operator: {:?}", pair.as_str()),
        }
    }
}

impl StatementParser for Literal {
    fn parse(pair: Pair<Rule>) -> Self {
        match pair.as_rule() {
            Rule::string => Literal::String(pair.as_str().to_string()),
            Rule::integer => Literal::Integer(pair.as_str().parse().expect("Invalid integer")),
            Rule::decimal => Literal::Float(pair.as_str().parse().expect("Invalid float")),
            Rule::boolean => Literal::Boolean(pair.as_str().eq_ignore_ascii_case("true")),
            _ => panic!("Invalid literal: {:?}", pair.as_str()),
        }
    }
}

impl Expression {
    pub fn entity(&self) -> Option<String> {
        match self {
            Expression::Lookup { entity, .. } => Some(entity.clone()),
            _ => None,
        }
    }

    pub fn key(&self) -> Option<String> {
        match self {
            Expression::Lookup { key, .. } => Some(key.clone()),
            _ => None,
        }
    }
}
