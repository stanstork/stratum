use super::compiler::FilterCompiler;
use connectors::file::csv::filter::{CsvComparator, CsvCondition, CsvFilter, CsvFilterExpr};
use model::execution::expr::{BinaryOp, CompiledExpression};
use std::str::FromStr;

pub struct CsvFilterCompiler;

impl FilterCompiler for CsvFilterCompiler {
    type Filter = CsvFilter;

    fn compile(expr: &CompiledExpression) -> Self::Filter {
        let csv_expr = compile_csv_expr(expr);
        CsvFilter::with_expr(csv_expr)
    }
}

fn from_compiled_condition(
    left: &CompiledExpression,
    op: &BinaryOp,
    right: &CompiledExpression,
) -> Result<CsvCondition, Box<dyn std::error::Error>> {
    // Extract the field name from left side
    let field = match left {
        CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
            // For CSV, we just need the column name (second segment)
            segments[1].clone()
        }
        CompiledExpression::Identifier(name) => name.clone(),
        other => {
            return Err(format!("Unsupported expression type for filter field: {other:?}").into());
        }
    };

    // Convert right side to string value
    let value = format_expr_value(right)?
        .trim_start_matches('\'')
        .trim_end_matches('\'')
        .trim_start_matches('"')
        .trim_end_matches('"')
        .to_string();

    // Map BinaryOp to CsvComparator
    let op_str = match op {
        BinaryOp::Equal => "=",
        BinaryOp::NotEqual => "!=",
        BinaryOp::GreaterThan => ">",
        BinaryOp::GreaterOrEqual => ">=",
        BinaryOp::LessThan => "<",
        BinaryOp::LessOrEqual => "<=",
        _ => return Err(format!("Unsupported operator for CSV filter: {:?}", op).into()),
    };

    let csv_op = CsvComparator::from_str(op_str)
        .map_err(|_| format!("Unsupported comparator: {}", op_str))?;

    Ok(CsvCondition {
        left: field,
        op: csv_op,
        right: value,
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

/// Recursively compiles a filter expression into a CSV filter AST.
fn compile_csv_expr(expr: &CompiledExpression) -> CsvFilterExpr {
    match expr {
        // Binary expression represents a condition
        CompiledExpression::Binary { left, op, right } => {
            // Check if this is a logical operator (AND/OR) or a comparison
            if matches!(op, BinaryOp::And | BinaryOp::Or) {
                // Logical operator - recursively compile both sides
                let left_expr = compile_csv_expr(left);
                let right_expr = compile_csv_expr(right);
                let children = vec![left_expr, right_expr];

                match op {
                    BinaryOp::And => CsvFilterExpr::and(children),
                    BinaryOp::Or => CsvFilterExpr::or(children),
                    _ => unreachable!(),
                }
            } else {
                // Comparison operator - create a leaf condition
                let csv_cond = from_compiled_condition(left, op, right).unwrap();
                CsvFilterExpr::leaf(csv_cond)
            }
        }

        // Function call with logical operators
        CompiledExpression::FunctionCall { name, args } => {
            let mut children = Vec::with_capacity(args.len());
            for arg in args {
                children.push(compile_csv_expr(arg));
            }
            match name.to_ascii_uppercase().as_str() {
                "AND" => CsvFilterExpr::and(children),
                "OR" => CsvFilterExpr::or(children),
                _ => panic!("Unsupported function call: {name}"),
            }
        }

        _ => panic!("Unsupported expression type for CSV filter: {:?}", expr),
    }
}
