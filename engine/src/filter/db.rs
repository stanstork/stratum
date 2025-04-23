use super::expr_to_string;
use smql::{
    plan::MigrationPlan,
    statements::{self, expr::Expression, filter::Comparator},
};
use sql_adapter::filter::{Condition, SqlFilter};

pub fn sql_filter(plan: &MigrationPlan) -> Result<SqlFilter, Box<dyn std::error::Error>> {
    let stmt_filter = plan.filter.as_ref().ok_or("No filter found in the plan")?;
    let conditions = stmt_filter
        .conditions
        .iter()
        .map(from_stmt_condtion)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SqlFilter { conditions })
}

fn from_stmt_condtion(
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
