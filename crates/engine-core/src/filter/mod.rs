use model::execution::{
    expr::{BinaryOp, CompiledExpression},
    pipeline::Filter,
};

pub mod compiler;
pub mod csv;
pub mod sql;

pub fn combine_filters(filters: &[Filter]) -> Option<CompiledExpression> {
    if filters.is_empty() {
        return None;
    }

    // Combine all filter conditions with AND logic.
    // Multiple where blocks or conditions in SMQL are semantically joined with AND,
    // meaning ALL conditions must be satisfied (standard SQL WHERE clause behavior).
    //
    // Example: where { age > 18 } where { status == "active" }
    // Results in: (age > 18) AND (status == "active")
    let combined_condition = if filters.len() == 1 {
        filters[0].condition.clone()
    } else {
        // Start with the first filter condition
        let mut combined = filters[0].condition.clone();
        // AND all subsequent filter conditions together
        for filter in &filters[1..] {
            combined = CompiledExpression::Binary {
                left: Box::new(combined),
                op: BinaryOp::And,
                right: Box::new(filter.condition.clone()),
            };
        }

        combined
    };

    Some(combined_condition)
}
