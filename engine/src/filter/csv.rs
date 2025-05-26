use std::str::FromStr;

use super::compiler::FilterCompiler;
use crate::filter::expr_to_string;
use csv::filter::{CsvComparator, CsvCondition, CsvFilter, CsvFilterExpr};
use smql::statements::{self, expr::Expression, filter::FilterExpression};

pub struct CsvFilterCompiler;

impl FilterCompiler for CsvFilterCompiler {
    type Filter = CsvFilter;

    fn compile(expr: &FilterExpression) -> Self::Filter {
        let csv_expr = compile_csv_expr(expr);
        CsvFilter::with_expr(csv_expr)
    }
}

fn from_stmt_condition(
    cond: &statements::filter::Condition,
) -> Result<CsvCondition, Box<dyn std::error::Error>> {
    // Extract the lookup key for the left-hand side
    let field = match &cond.left {
        Expression::Lookup { key, .. } => key.clone(),
        other => {
            return Err(format!("Unsupported expression type filter field: {:?}", other).into())
        }
    };

    // Render the right-hand side to string
    let value = expr_to_string(&cond.right)
        .map_err(|e| format!("Unsupported expression type filter value: {:?}", e))?
        .trim_start_matches('\'')
        .trim_end_matches('\'')
        .to_string();

    // Parse comparator
    let op = CsvComparator::from_str(&cond.op.to_string())
        .map_or(Err(format!("Unsupported comparator: {:?}", cond.op)), Ok)?;

    Ok(CsvCondition {
        left: field,
        op,
        right: value,
    })
}

/// Recursively compiles a filter expression into a CSV filter AST.
fn compile_csv_expr(expr: &FilterExpression) -> CsvFilterExpr {
    match expr {
        FilterExpression::Condition(cond) => {
            let csv_cond = from_stmt_condition(cond).unwrap();
            CsvFilterExpr::leaf(csv_cond)
        }
        FilterExpression::FunctionCall(name, args) => {
            let mut children = Vec::with_capacity(args.len());
            for arg in args {
                children.push(compile_csv_expr(arg));
            }
            match name.to_ascii_uppercase().as_str() {
                "AND" => CsvFilterExpr::and(children),
                "OR" => CsvFilterExpr::or(children),
                _ => panic!("Unsupported function call: {}", name),
            }
        }
    }
}
