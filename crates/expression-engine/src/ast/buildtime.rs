use crate::{context::EvalContext, error::ExpressionError, functions::FunctionRegistry};
use model::core::value::Value;
use smql_syntax::ast::{
    expr::{Expression, ExpressionKind},
    literal::Literal,
};
use std::collections::HashMap;

/// Evaluate AST expressions to values at build-time
/// This is used during plan building for simple expressions (literals + function calls)
pub fn eval_ast_expression(
    expr: &Expression,
    definitions: &HashMap<String, Value>,
    env_getter: fn(&str) -> Option<String>,
) -> Result<Value, ExpressionError> {
    match &expr.kind {
        ExpressionKind::Literal(lit) => Ok(match lit {
            Literal::String(s) => Value::String(s.clone()),
            Literal::Number(n) => Value::Float(*n),
            Literal::Boolean(b) => Value::Boolean(*b),
            Literal::Null => Value::Null,
        }),
        ExpressionKind::FunctionCall { name, arguments } => {
            // Evaluate arguments to values
            let args: Vec<Value> = arguments
                .iter()
                .map(|arg| eval_ast_expression(arg, definitions, env_getter))
                .collect::<Result<Vec<_>, _>>()?;

            let registry = FunctionRegistry::new();
            let ctx = EvalContext::BuildTime {
                definitions,
                env_getter,
            };

            registry.call(name, &args, &ctx)
        }
        _ => Err(ExpressionError::InvalidFunctionArgs {
            function: "eval_ast_expression".to_string(),
            message: format!("cannot evaluate complex expression: {:?}", expr),
        }),
    }
}
