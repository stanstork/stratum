use smql::statements::expr::{Expression, Literal, Operator};

pub mod eval;
pub mod types;

pub fn expr_to_string(e: &Expression) -> Result<String, Expression> {
    match e {
        Expression::Literal(lit) => Ok(match lit {
            Literal::String(s) => format!("{}", s.trim_start_matches('"').trim_end_matches('"')),
            Literal::Integer(i) => i.to_string(),
            Literal::Float(f) => f.to_string(),
            Literal::Boolean(b) => b.to_string(),
        }),
        Expression::Identifier(ident) => Ok(ident.clone()),

        Expression::Lookup { entity, key, .. } => Ok(format!("{}.{}", entity, key)),

        Expression::Arithmetic {
            left,
            operator,
            right,
        } => {
            let l = expr_to_string(left)?;
            let r = expr_to_string(right)?;
            let op = match operator {
                Operator::Add => "+",
                Operator::Subtract => "-",
                Operator::Multiply => "*",
                Operator::Divide => "/",
            };
            Ok(format!("{} {} {}", l, op, r))
        }
        Expression::FunctionCall { name, arguments } => {
            let args: Result<Vec<String>, Expression> =
                arguments.iter().map(expr_to_string).collect();
            Ok(format!("{}({})", name, args?.join(", ")))
        }
    }
}
