use smql::statements::expr::{Expression, Literal, Operator};

pub mod eval;
pub mod types;

pub fn format_expr(e: &Expression) -> Result<String, Expression> {
    match e {
        Expression::Literal(lit) => Ok(match lit {
            Literal::String(s) => s.trim_start_matches('"').trim_end_matches('"').to_string(),
            Literal::Integer(i) => i.to_string(),
            Literal::Float(f) => f.to_string(),
            Literal::Boolean(b) => b.to_string(),
        }),
        Expression::Identifier(ident) => Ok(ident.clone()),

        Expression::Lookup { entity, key, .. } => Ok(format!("{entity}.{key}")),

        Expression::Arithmetic {
            left,
            operator,
            right,
        } => {
            let l = format_expr(left)?;
            let r = format_expr(right)?;
            let op = match operator {
                Operator::Add => "+",
                Operator::Subtract => "-",
                Operator::Multiply => "*",
                Operator::Divide => "/",
            };
            Ok(format!("{l} {op} {r}"))
        }
        Expression::FunctionCall { name, arguments } => {
            let args: Result<Vec<String>, Expression> = arguments.iter().map(format_expr).collect();
            Ok(format!("{}({})", name, args?.join(", ")))
        }
    }
}
