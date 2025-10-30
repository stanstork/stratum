#[macro_export]
macro_rules! ident {
    ($field:expr) => {{
        // Create the base expression (either a simple identifier or a function call for geometry)
        let base_expr = if $field.is_geometry() {
            planner::query::ast::expr::Expr::FunctionCall(planner::query::ast::expr::FunctionCall {
                name: "ST_AsBinary".to_string(),
                args: vec![planner::query::ast::expr::Expr::Identifier(
                    planner::query::ast::expr::Ident {
                        qualifier: Some($field.table.clone()),
                        name: $field.column.clone(),
                    },
                )],
                wildcard: false,
            })
        } else {
            planner::query::ast::expr::Expr::Identifier(planner::query::ast::expr::Ident {
                qualifier: Some($field.table.clone()),
                name: $field.column.clone(),
            })
        };

        if let Some(alias) = &$field.alias {
            planner::query::ast::expr::Expr::Alias {
                expr: Box::new(base_expr),
                alias: alias.clone(),
            }
        } else {
            base_expr
        }
    }};
}

#[macro_export]
macro_rules! join_on_expr {
    ($join_clause:expr) => {
        (|| -> Result<planner::query::ast::expr::Expr, $crate::sql::base::error::DbError> {
            let conditions = &$join_clause.conditions;
            if conditions.is_empty() {
                return Err($crate::sql::base::error::DbError::QueryBuildError(
                    "JoinClause must have at least one condition.".to_string(),
                ));
            }

            let condition_to_expr = |cond: &$crate::sql::base::join::clause::JoinCondition| {
                planner::query::ast::expr::Expr::BinaryOp(Box::new(
                    planner::query::ast::expr::BinaryOp {
                        left: planner::query::ast::expr::Expr::Identifier(
                            planner::query::ast::expr::Ident {
                                qualifier: Some(cond.left.alias.clone()),
                                name: cond.left.column.clone(),
                            },
                        ),
                        op: planner::query::ast::expr::BinaryOperator::Eq,
                        right: planner::query::ast::expr::Expr::Identifier(
                            planner::query::ast::expr::Ident {
                                qualifier: Some(cond.right.alias.clone()),
                                name: cond.right.column.clone(),
                            },
                        ),
                    },
                ))
            };

            conditions[1..]
                .iter()
                .try_fold(condition_to_expr(&conditions[0]), |left_expr, cond| {
                    let right_expr = condition_to_expr(cond);
                    Ok(planner::query::ast::expr::Expr::BinaryOp(Box::new(
                        planner::query::ast::expr::BinaryOp {
                            left: left_expr,
                            op: planner::query::ast::expr::BinaryOperator::And,
                            right: right_expr,
                        },
                    )))
                })
        })()
    };
}

#[macro_export]
macro_rules! sql_filter_expr {
    ($filter_expr:expr) => {{
        fn convert(
            expr: &$crate::sql::base::filter::expr::SqlFilterExpr,
        ) -> Result<planner::query::ast::expr::Expr, $crate::sql::base::error::DbError> {
            match expr {
                $crate::sql::base::filter::expr::SqlFilterExpr::Leaf(cond) => {
                    let op = match cond.comparator.as_str() {
                        "=" => Ok(planner::query::ast::expr::BinaryOperator::Eq),
                        "!=" => Ok(planner::query::ast::expr::BinaryOperator::NotEq),
                        ">" => Ok(planner::query::ast::expr::BinaryOperator::Gt),
                        ">=" => Ok(planner::query::ast::expr::BinaryOperator::GtEq),
                        "<" => Ok(planner::query::ast::expr::BinaryOperator::Lt),
                        "<=" => Ok(planner::query::ast::expr::BinaryOperator::LtEq),
                        other => Err($crate::sql::base::error::DbError::QueryBuildError(format!(
                            "Unsupported comparator: {}",
                            other
                        ))),
                    }?;

                    Ok(planner::query::ast::expr::Expr::BinaryOp(Box::new(
                        planner::query::ast::expr::BinaryOp {
                            left: planner::query::ast::expr::Expr::Identifier(
                                planner::query::ast::expr::Ident {
                                    qualifier: Some(cond.table.clone()),
                                    name: cond.column.clone(),
                                },
                            ),
                            op,
                            right: planner::query::ast::expr::Expr::Value(
                                model::core::value::Value::String(cond.value.clone()),
                            ),
                        },
                    )))
                }
                $crate::sql::base::filter::expr::SqlFilterExpr::And(children)
                | $crate::sql::base::filter::expr::SqlFilterExpr::Or(children) => {
                    if children.is_empty() {
                        return Err($crate::sql::base::error::DbError::QueryBuildError(
                            "AND/OR expressions cannot be empty.".to_string(),
                        ));
                    }

                    let op =
                        if matches!(expr, $crate::sql::base::filter::expr::SqlFilterExpr::And(_)) {
                            planner::query::ast::expr::BinaryOperator::And
                        } else {
                            planner::query::ast::expr::BinaryOperator::Or
                        };

                    children[1..]
                        .iter()
                        .try_fold(convert(&children[0])?, |left_expr, child| {
                            let right_expr = convert(child)?;
                            Ok(planner::query::ast::expr::Expr::BinaryOp(Box::new(
                                planner::query::ast::expr::BinaryOp {
                                    left: left_expr,
                                    op: op.clone(),
                                    right: right_expr,
                                },
                            )))
                        })
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
                $crate::sql::base::join::clause::JoinType::Inner => {
                    planner::query::ast::common::JoinKind::Inner
                }
                $crate::sql::base::join::clause::JoinType::Left => {
                    planner::query::ast::common::JoinKind::Left
                }
                $crate::sql::base::join::clause::JoinType::Right => {
                    planner::query::ast::common::JoinKind::Right
                }
                $crate::sql::base::join::clause::JoinType::Full => {
                    planner::query::ast::common::JoinKind::Full
                }
            };

            b.join(
                join_kind,
                table_ref!(&join.left.table),
                Some(&join.left.alias),
                join_on_expr!(join).unwrap(),
            )
        })
    }};
}

#[macro_export]
macro_rules! add_where {
    ($builder:expr, $filter:expr) => {{
        let mut builder = $builder;
        if let Some(filter) = $filter {
            if let Some(expr) = &filter.expr {
                builder = builder.where_clause(sql_filter_expr!(expr).unwrap());
            }
        }
        builder
    }};
}
