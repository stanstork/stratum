#[macro_export]
macro_rules! ident {
    ($field:expr) => {{
        // Create the base expression (either a simple identifier or a function call for geometry)
        let base_expr = if $field.is_geometry() {
            query_builder::ast::expr::Expr::FunctionCall(query_builder::ast::expr::FunctionCall {
                name: "ST_AsBinary".to_string(),
                args: vec![query_builder::ast::expr::Expr::Identifier(
                    query_builder::ast::expr::Ident {
                        qualifier: Some($field.table.clone()),
                        name: $field.column.clone(),
                    },
                )],
                wildcard: false,
            })
        } else {
            query_builder::ast::expr::Expr::Identifier(query_builder::ast::expr::Ident {
                qualifier: Some($field.table.clone()),
                name: $field.column.clone(),
            })
        };

        if let Some(alias) = &$field.alias {
            query_builder::ast::expr::Expr::Alias {
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
        (|| -> Result<query_builder::ast::expr::Expr, $crate::error::db::DbError> {
            let conditions = &$join_clause.conditions;
            if conditions.is_empty() {
                return Err($crate::error::db::DbError::QueryBuildError(
                    "JoinClause must have at least one condition.".to_string(),
                ));
            }

            let condition_to_expr = |cond: &$crate::join::clause::JoinCondition| {
                query_builder::ast::expr::Expr::BinaryOp(Box::new(
                    query_builder::ast::expr::BinaryOp {
                        left: query_builder::ast::expr::Expr::Identifier(
                            query_builder::ast::expr::Ident {
                                qualifier: Some(cond.left.alias.clone()),
                                name: cond.left.column.clone(),
                            },
                        ),
                        op: query_builder::ast::expr::BinaryOperator::Eq,
                        right: query_builder::ast::expr::Expr::Identifier(
                            query_builder::ast::expr::Ident {
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
                    Ok(query_builder::ast::expr::Expr::BinaryOp(Box::new(
                        query_builder::ast::expr::BinaryOp {
                            left: left_expr,
                            op: query_builder::ast::expr::BinaryOperator::And,
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
            expr: &$crate::filter::expr::SqlFilterExpr,
        ) -> Result<query_builder::ast::expr::Expr, $crate::error::db::DbError> {
            match expr {
                $crate::filter::expr::SqlFilterExpr::Leaf(cond) => {
                    let op = match cond.comparator.as_str() {
                        "=" => Ok(query_builder::ast::expr::BinaryOperator::Eq),
                        "!=" => Ok(query_builder::ast::expr::BinaryOperator::NotEq),
                        ">" => Ok(query_builder::ast::expr::BinaryOperator::Gt),
                        ">=" => Ok(query_builder::ast::expr::BinaryOperator::GtEq),
                        "<" => Ok(query_builder::ast::expr::BinaryOperator::Lt),
                        "<=" => Ok(query_builder::ast::expr::BinaryOperator::LtEq),
                        other => Err($crate::error::db::DbError::QueryBuildError(format!(
                            "Unsupported comparator: {}",
                            other
                        ))),
                    }?;

                    Ok(query_builder::ast::expr::Expr::BinaryOp(Box::new(
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
                    )))
                }
                $crate::filter::expr::SqlFilterExpr::And(children)
                | $crate::filter::expr::SqlFilterExpr::Or(children) => {
                    if children.is_empty() {
                        return Err($crate::error::db::DbError::QueryBuildError(
                            "AND/OR expressions cannot be empty.".to_string(),
                        ));
                    }

                    let op = if matches!(expr, $crate::filter::expr::SqlFilterExpr::And(_)) {
                        query_builder::ast::expr::BinaryOperator::And
                    } else {
                        query_builder::ast::expr::BinaryOperator::Or
                    };

                    children[1..]
                        .iter()
                        .try_fold(convert(&children[0])?, |left_expr, child| {
                            let right_expr = convert(child)?;
                            Ok(query_builder::ast::expr::Expr::BinaryOp(Box::new(
                                query_builder::ast::expr::BinaryOp {
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
