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

#[macro_export]
macro_rules! sql_filter_expr {
    ($filter_expr:expr) => {{
        fn convert(expr: &$crate::filter::expr::SqlFilterExpr) -> query_builder::ast::expr::Expr {
            match expr {
                $crate::filter::expr::SqlFilterExpr::Leaf(cond) => {
                    let op = match cond.comparator.as_str() {
                        "=" => query_builder::ast::expr::BinaryOperator::Eq,
                        "!=" => query_builder::ast::expr::BinaryOperator::NotEq,
                        ">" => query_builder::ast::expr::BinaryOperator::Gt,
                        ">=" => query_builder::ast::expr::BinaryOperator::GtEq,
                        "<" => query_builder::ast::expr::BinaryOperator::Lt,
                        "<=" => query_builder::ast::expr::BinaryOperator::LtEq,
                        other => panic!("Unsupported comparator: {}", other),
                    };

                    query_builder::ast::expr::Expr::BinaryOp(Box::new(
                        query_builder::ast::expr::BinaryOp {
                            left: query_builder::ast::expr::Expr::Identifier(
                                query_builder::ast::expr::Ident {
                                    qualifier: Some(cond.table.clone()),
                                    name: cond.column.clone(),
                                },
                            ),
                            op,
                            right: query_builder::ast::expr::Expr::Value(
                                common::value::Value::String(cond.value.clone()),
                            ),
                        },
                    ))
                }
                $crate::filter::expr::SqlFilterExpr::And(children)
                | $crate::filter::expr::SqlFilterExpr::Or(children) => {
                    if children.is_empty() {
                        panic!("AND/OR expressions cannot be empty.");
                    }

                    let op = if matches!(expr, $crate::filter::expr::SqlFilterExpr::And(_)) {
                        query_builder::ast::expr::BinaryOperator::And
                    } else {
                        query_builder::ast::expr::BinaryOperator::Or
                    };

                    // Start with the first child as the base.
                    let mut expr_tree = convert(&children[0]);

                    // Fold the rest of the children into the tree.
                    for child in children.iter().skip(1) {
                        expr_tree = query_builder::ast::expr::Expr::BinaryOp(Box::new(
                            query_builder::ast::expr::BinaryOp {
                                left: expr_tree,
                                op: op.clone(),
                                right: convert(child),
                            },
                        ));
                    }
                    expr_tree
                }
            }
        }
        convert($filter_expr)
    }};
}

#[macro_export]
macro_rules! add_joins {
    ($builder:expr, $joins:expr) => {{
        $joins.iter().fold($builder, |b, join| {
            let join_kind = match join.join_type {
                $crate::join::clause::JoinType::Inner => {
                    query_builder::ast::common::JoinKind::Inner
                }
                $crate::join::clause::JoinType::Left => query_builder::ast::common::JoinKind::Left,
                $crate::join::clause::JoinType::Right => {
                    query_builder::ast::common::JoinKind::Right
                }
                $crate::join::clause::JoinType::Full => query_builder::ast::common::JoinKind::Full,
            };

            b.join(
                join_kind,
                table_ref!(&join.left.table),
                Some(&join.left.alias),
                join_on_expr!(join),
            )
        })
    }};
}

#[macro_export]
macro_rules! add_where {
    ($builder:expr, $filter:expr) => {{
        let mut temp_builder = $builder;
        if let Some(filter) = $filter {
            if let Some(expr) = &filter.expr {
                temp_builder = temp_builder.where_clause(sql_filter_expr!(expr));
            }
        }
        temp_builder
    }};
}
