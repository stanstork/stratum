use super::compiler::FilterCompiler;
use connectors::sql::base::filter::{SqlFilter, condition::Condition, expr::SqlFilterExpr};
use model::execution::expr::{BinaryOp, CompiledExpression};

pub struct SqlFilterCompiler;

impl FilterCompiler for SqlFilterCompiler {
    type Filter = SqlFilter;

    fn compile(expr: &CompiledExpression) -> Self::Filter {
        let sql_expr = compile_sql_expr(expr);
        SqlFilter::with_expr(sql_expr)
    }
}

fn compile_sql_expr(expr: &CompiledExpression) -> SqlFilterExpr {
    match expr {
        // Binary expression represents a condition (e.g., table.column = value)
        CompiledExpression::Binary { left, op, right } => {
            // Check if this is a logical operator (AND/OR) or a comparison
            if matches!(op, BinaryOp::And | BinaryOp::Or) {
                // Logical operator - recursively compile both sides
                let left_expr = compile_sql_expr(left);
                let right_expr = compile_sql_expr(right);
                let children = vec![left_expr, right_expr];

                match op {
                    BinaryOp::And => SqlFilterExpr::and(children),
                    BinaryOp::Or => SqlFilterExpr::or(children),
                    _ => unreachable!(),
                }
            } else {
                // Comparison operator - create a leaf condition
                let condition = from_compiled_condition(left, op, right).unwrap();
                SqlFilterExpr::leaf(condition)
            }
        }

        // Function call with logical operators
        CompiledExpression::FunctionCall { name, args } => {
            let children = args.iter().map(compile_sql_expr).collect::<Vec<_>>();

            match name.to_ascii_uppercase().as_str() {
                "AND" => SqlFilterExpr::and(children),
                "OR" => SqlFilterExpr::or(children),
                _ => panic!("Unsupported function call: {name}"),
            }
        }

        _ => panic!("Unsupported expression type for filter: {:?}", expr),
    }
}

fn from_compiled_condition(
    left: &CompiledExpression,
    op: &BinaryOp,
    right: &CompiledExpression,
) -> Result<Condition, Box<dyn std::error::Error>> {
    // Extract table & column from left side (expected to be DotPath like table.column)
    let (table, column) = match left {
        CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
            (segments[0].clone(), segments[1].clone())
        }
        CompiledExpression::Identifier(name) => {
            // Single identifier without table prefix
            (String::new(), name.clone())
        }
        other => {
            return Err(format!("Unsupported expression type for filter field: {other:?}").into());
        }
    };

    // Convert right side to string value
    let value = format_expr_value(right)?;

    // Map BinaryOp to SQL comparator
    let comparator = match op {
        BinaryOp::Equal => "=",
        BinaryOp::NotEqual => "!=",
        BinaryOp::GreaterThan => ">",
        BinaryOp::GreaterOrEqual => ">=",
        BinaryOp::LessThan => "<",
        BinaryOp::LessOrEqual => "<=",
        _ => return Err(format!("Unsupported operator for filter: {:?}", op).into()),
    }
    .to_string();

    Ok(Condition {
        table,
        column,
        comparator,
        value,
    })
}

fn format_expr_value(expr: &CompiledExpression) -> Result<String, Box<dyn std::error::Error>> {
    match expr {
        CompiledExpression::Literal(value) => Ok(format!("{:?}", value)),
        CompiledExpression::Identifier(name) => Ok(name.clone()),
        CompiledExpression::DotPath(segments) => Ok(segments.join(".")),
        _ => Err(format!("Unsupported expression type for filter value: {:?}", expr).into()),
    }
}
