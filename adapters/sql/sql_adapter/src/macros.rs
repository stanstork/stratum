#[macro_export]
macro_rules! ident {
    ($field:expr) => {
        query_builder::ast::expr::Expr::Alias {
            expr: Box::new(query_builder::ast::expr::Expr::Identifier(
                query_builder::ast::expr::Ident {
                    qualifier: Some($field.table.clone()),
                    name: $field.column.clone(),
                },
            )),
            alias: $field.alias.clone().unwrap(),
        }
    };
}

/// Converts a `JoinClause` into a `stratum_sql::ast::expr::Expr`.
///
/// This macro iterates through the conditions in a `JoinClause` and
/// builds a nested `BinaryOp` expression, chaining them with `AND`.
#[macro_export]
macro_rules! join_on_expr {
    ($join_clause:expr) => {{
        let conditions = &$join_clause.conditions;
        if conditions.is_empty() {
            // A JOIN ON clause cannot be empty.
            panic!("JoinClause must have at least one condition to build an expression.");
        }

        // Helper closure to create a single equality expression.
        let condition_to_expr = |cond: &$crate::join::clause::JoinCondition| {
            query_builder::ast::expr::Expr::BinaryOp(Box::new(query_builder::ast::expr::BinaryOp {
                left: query_builder::ast::expr::Expr::Identifier(query_builder::ast::expr::Ident {
                    qualifier: Some(cond.left.alias.clone()),
                    name: cond.left.column.clone(),
                }),
                op: query_builder::ast::expr::BinaryOperator::Eq,
                right: query_builder::ast::expr::Expr::Identifier(
                    query_builder::ast::expr::Ident {
                        qualifier: Some(cond.right.alias.clone()),
                        name: cond.right.column.clone(),
                    },
                ),
            }))
        };

        // Start with the first condition as the base expression.
        let mut expr_tree = condition_to_expr(&conditions[0]);

        // Fold the rest of the conditions into the tree, chaining with AND.
        for cond in conditions.iter().skip(1) {
            let new_op = condition_to_expr(cond);

            expr_tree = query_builder::ast::expr::Expr::BinaryOp(Box::new(
                query_builder::ast::expr::BinaryOp {
                    left: expr_tree, // The previous expression tree
                    op: query_builder::ast::expr::BinaryOperator::And,
                    right: new_op, // The new condition
                },
            ));
        }

        expr_tree
    }};
}
