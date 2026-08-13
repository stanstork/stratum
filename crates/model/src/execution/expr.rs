use crate::core::value::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Compiled expression ready for runtime evaluation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompiledExpression {
    Literal(Value),
    Identifier(String),
    DotPath(Vec<String>),
    Binary {
        left: Box<CompiledExpression>,
        op: BinaryOp,
        right: Box<CompiledExpression>,
    },
    Unary {
        op: UnaryOp,
        operand: Box<CompiledExpression>,
    },
    FunctionCall {
        name: String,
        args: Vec<CompiledExpression>,
    },
    Array(Vec<CompiledExpression>),
    When {
        branches: Vec<WhenBranch>,
        else_expr: Option<Box<CompiledExpression>>,
    },
    IsNull(Box<CompiledExpression>),
    IsNotNull(Box<CompiledExpression>),
    Grouped(Box<CompiledExpression>),
}

/// Binary operators matching AST BinaryOperator
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    // Comparison
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterOrEqual,
    LessOrEqual,
    // Logical
    And,
    Or,
}

/// Unary operators matching AST UnaryOperator
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Negate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhenBranch {
    pub condition: CompiledExpression,
    pub value: CompiledExpression,
}

impl CompiledExpression {
    /// Collect every column reference in this expression tree into `out`
    /// (lowercased). Used by projection pushdown to learn which source columns a
    /// `select`/`when`/`validate` expression actually needs.
    pub fn collect_column_refs(&self, out: &mut HashSet<String>) {
        match self {
            CompiledExpression::Identifier(name) => {
                out.insert(name.to_ascii_lowercase());
            }
            CompiledExpression::DotPath(segments) => {
                if let Some(col) = segments.last() {
                    out.insert(col.to_ascii_lowercase());
                }
            }
            CompiledExpression::Binary { left, right, .. } => {
                left.collect_column_refs(out);
                right.collect_column_refs(out);
            }
            CompiledExpression::Unary { operand, .. } => operand.collect_column_refs(out),
            CompiledExpression::FunctionCall { args, .. } => {
                for a in args {
                    a.collect_column_refs(out);
                }
            }
            CompiledExpression::Array(elems) => {
                for e in elems {
                    e.collect_column_refs(out);
                }
            }
            CompiledExpression::When {
                branches,
                else_expr,
            } => {
                for b in branches {
                    b.condition.collect_column_refs(out);
                    b.value.collect_column_refs(out);
                }
                if let Some(e) = else_expr {
                    e.collect_column_refs(out);
                }
            }
            CompiledExpression::IsNull(e)
            | CompiledExpression::IsNotNull(e)
            | CompiledExpression::Grouped(e) => e.collect_column_refs(out),
            CompiledExpression::Literal(_) => {}
        }
    }
}

#[cfg(test)]
mod collect_column_refs_tests {
    use super::*;
    use std::collections::HashSet;

    fn refs(e: &CompiledExpression) -> HashSet<String> {
        let mut s = HashSet::new();
        e.collect_column_refs(&mut s);
        s
    }

    #[test]
    fn identifier_and_dotpath_are_columns() {
        assert!(refs(&CompiledExpression::Identifier("Amount".into())).contains("amount"));
        // table.column -> the column (lowercased); table part ignored.
        let dp = CompiledExpression::DotPath(vec!["Orders".into(), "Id".into()]);
        assert_eq!(refs(&dp), HashSet::from(["id".to_string()]));
    }

    #[test]
    fn walks_nested_function_binary_and_when() {
        // upper(concat(first_name, when x > 0 then note else tags))
        let cond = CompiledExpression::Binary {
            left: Box::new(CompiledExpression::Identifier("x".into())),
            op: BinaryOp::GreaterThan,
            right: Box::new(CompiledExpression::Literal(Value::Int(0))),
        };
        let when = CompiledExpression::When {
            branches: vec![WhenBranch {
                condition: cond,
                value: CompiledExpression::Identifier("note".into()),
            }],
            else_expr: Some(Box::new(CompiledExpression::Identifier("tags".into()))),
        };
        let concat = CompiledExpression::FunctionCall {
            name: "concat".into(),
            args: vec![CompiledExpression::Identifier("first_name".into()), when],
        };
        let expr = CompiledExpression::FunctionCall {
            name: "upper".into(),
            args: vec![concat],
        };
        let got = refs(&expr);
        assert_eq!(
            got,
            HashSet::from([
                "x".to_string(),
                "note".to_string(),
                "tags".to_string(),
                "first_name".to_string(),
            ]),
            "must collect every column across nested nodes"
        );
    }

    #[test]
    fn literals_and_function_names_are_not_columns() {
        // env("HOME") -> the function name and the literal arg are not columns.
        let e = CompiledExpression::FunctionCall {
            name: "env".into(),
            args: vec![CompiledExpression::Literal(Value::String("HOME".into()))],
        };
        assert!(refs(&e).is_empty());
    }
}
