use super::{compiler::FilterCompiler, filter::expr_to_string};
use smql::statements::{
    self,
    expr::Expression,
    filter::{Comparator, FilterExpression},
};
use sql_adapter::filter::{condition::Condition, expr::SqlFilterExpr, filter::SqlFilter};

pub struct SqlFilterCompiler;

impl FilterCompiler for SqlFilterCompiler {
    type Filter = SqlFilter;

    fn compile(expr: &FilterExpression) -> Self::Filter {
        let sql_expr = compile_sql_expr(expr);
        SqlFilter::with_expr(sql_expr)
    }
}

fn compile_sql_expr(expr: &FilterExpression) -> SqlFilterExpr {
    match expr {
        FilterExpression::Condition(condition) => {
            let condition = from_stmt_condition(condition).unwrap();
            SqlFilterExpr::leaf(condition)
        }
        FilterExpression::FunctionCall(name, args) => {
            let children = args.iter().map(compile_sql_expr).collect::<Vec<_>>();

            match name.to_ascii_uppercase().as_str() {
                "AND" => SqlFilterExpr::and(children),
                "OR" => SqlFilterExpr::or(children),
                "NOT" if children.len() == 1 => {
                    SqlFilterExpr::not(children.into_iter().next().unwrap())
                }
                _ => panic!("Unsupported function call: {}", name),
            }
        }
    }
}

fn from_stmt_condition(
    c: &statements::filter::Condition,
) -> Result<Condition, Box<dyn std::error::Error>> {
    // extract table & column
    let (table, column) = match &c.field {
        Expression::Lookup { table, key, .. } => (table.clone(), key.clone()),
        other => {
            return Err(format!("Unsupported expression type filter field: {:?}", other).into())
        }
    };

    // stringify the RHS (literal, identifier, lookup or arithmetic)
    let value = expr_to_string(&c.value)
        .map_err(|e| format!("Unsupported expression type filter value: {:?}", e))?;

    // map comparator to its SQL symbol
    let comparator = match c.comparator {
        Comparator::Equal => "=",
        Comparator::NotEqual => "!=",
        Comparator::GreaterThan => ">",
        Comparator::GreaterThanOrEqual => ">=",
        Comparator::LessThan => "<",
        Comparator::LessThanOrEqual => "<=",
    }
    .to_string();

    Ok(Condition {
        table,
        column,
        comparator,
        value,
    })
}
