use crate::{
    ast::{
        common::OrderDir,
        expr::{BinaryOp, BinaryOperator, Expr},
    },
    build::select::{FromState, SelectBuilder},
    ident, value,
};
use async_trait::async_trait;
use common::value::Value;
use serde::{Deserialize, Serialize};

/// Represents the pagination cursor.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Cursor {
    /// No cursor, fetch the first page.
    None,
    /// Cursor for simple Primary Key offset.
    Pk { id: u64 },
    /// Cursor for composite (timestamp/numeric + PK) offset.
    CompositeTsPk {
        ts_col: String,
        pk_col: String,
        /// The timestamp (micros) or numeric value.
        ts: i64,
        /// The tie-breaker ID.
        id: u64,
    },
}

#[async_trait]
pub trait OffsetStrategy: Send + Sync {
    /// Applies the pagination logic (WHERE and ORDER BY) to a SelectBuilder.
    fn apply_to_builder(
        &self,
        builder: SelectBuilder<FromState>,
        cursor: &Cursor,
        limit: usize,
    ) -> SelectBuilder<FromState>;

    /// Clones the boxed trait object.
    fn clone_box(&self) -> Box<dyn OffsetStrategy>;
}

pub struct PkOffset {
    pub pk: String,
}

pub struct NumericOffset {
    pub col: String,
    pub pk: String,
}

pub struct TimestampOffset {
    pub ts_col: String,
    pub pk: String,
    pub tz: chrono_tz::Tz,
}

impl OffsetStrategy for PkOffset {
    fn apply_to_builder(
        &self,
        mut builder: SelectBuilder<FromState>,
        cursor: &Cursor,
        limit: usize,
    ) -> SelectBuilder<FromState> {
        // Add WHERE clause based on cursor
        if let Cursor::Pk { id } = cursor {
            // WHERE pk > ?
            let where_cond = Expr::BinaryOp(Box::new(BinaryOp {
                left: ident(&self.pk),
                op: BinaryOperator::Gt,
                right: value(Value::Uint(*id)),
            }));

            // Check if a WHERE clause already exists
            if let Some(existing_where) = builder.ast.where_clause {
                // If so, combine with AND
                builder.ast.where_clause = Some(Expr::BinaryOp(Box::new(BinaryOp {
                    left: existing_where,
                    op: BinaryOperator::And,
                    right: where_cond,
                })));
            } else {
                // Otherwise, just set it
                builder.ast.where_clause = Some(where_cond);
            }
        }

        // If Cursor::None, we are on the first page, so no WHERE clause is added.

        // Add ORDER BY
        // ORDER BY pk ASC
        builder = builder.order_by(ident(&self.pk), Some(OrderDir::Asc));

        // Add LIMIT
        // LIMIT ?
        builder = builder.limit(value(Value::Uint(limit as u64)));

        builder
    }

    fn clone_box(&self) -> Box<dyn OffsetStrategy> {
        Box::new(PkOffset {
            pk: self.pk.clone(),
        })
    }
}

impl OffsetStrategy for NumericOffset {
    fn apply_to_builder(
        &self,
        mut builder: SelectBuilder<FromState>,
        cursor: &Cursor,
        limit: usize,
    ) -> SelectBuilder<FromState> {
        // Add WHERE clause based on cursor
        if let Cursor::CompositeTsPk { ts, id, .. } = cursor {
            // WHERE (ts_col > ?) OR (ts_col = ? AND pk_col > ?)

            // (col > ?)
            let cond1 = Expr::BinaryOp(Box::new(BinaryOp {
                left: ident(&self.col),
                op: BinaryOperator::Gt,
                right: value(Value::Int(*ts)),
            }));

            // (col = ?)
            let cond2_left = Expr::BinaryOp(Box::new(BinaryOp {
                left: ident(&self.col),
                op: BinaryOperator::Eq,
                right: value(Value::Int(*ts)),
            }));

            // (pk > ?)
            let cond2_right = Expr::BinaryOp(Box::new(BinaryOp {
                left: ident(&self.pk),
                op: BinaryOperator::Gt,
                right: value(Value::Uint(*id)),
            }));

            // (col = ? AND pk > ?)
            let cond2 = Expr::BinaryOp(Box::new(BinaryOp {
                left: cond2_left,
                op: BinaryOperator::And,
                right: cond2_right,
            }));

            // (cond1) OR (cond2)
            let where_cond = Expr::BinaryOp(Box::new(BinaryOp {
                left: cond1,
                op: BinaryOperator::Or,
                right: cond2,
            }));

            // Combine with existing WHERE clause
            if let Some(existing_where) = builder.ast.where_clause {
                builder.ast.where_clause = Some(Expr::BinaryOp(Box::new(BinaryOp {
                    left: existing_where,
                    op: BinaryOperator::And,
                    right: where_cond,
                })));
            } else {
                builder.ast.where_clause = Some(where_cond);
            }
        }

        // Add ORDER BY
        builder = builder.order_by(ident(&self.col), Some(OrderDir::Asc));
        builder = builder.order_by(ident(&self.pk), Some(OrderDir::Asc));

        // Add LIMIT
        builder = builder.limit(value(Value::Uint(limit as u64)));

        builder
    }

    fn clone_box(&self) -> Box<dyn OffsetStrategy> {
        Box::new(NumericOffset {
            col: self.col.clone(),
            pk: self.pk.clone(),
        })
    }
}

impl OffsetStrategy for TimestampOffset {
    fn apply_to_builder(
        &self,
        mut builder: SelectBuilder<FromState>,
        cursor: &Cursor,
        limit: usize,
    ) -> SelectBuilder<FromState> {
        // Add WHERE clause based on cursor
        if let Cursor::CompositeTsPk { ts, id, .. } = cursor {
            // WHERE (ts > ?) OR (ts = ? AND pk > ?)
            let cond1 = Expr::BinaryOp(Box::new(BinaryOp {
                left: ident(&self.ts_col),
                op: BinaryOperator::Gt,
                right: value(Value::Int(*ts)),
            }));
            let cond2_left = Expr::BinaryOp(Box::new(BinaryOp {
                left: ident(&self.ts_col),
                op: BinaryOperator::Eq,
                right: value(Value::Int(*ts)),
            }));
            let cond2_right = Expr::BinaryOp(Box::new(BinaryOp {
                left: ident(&self.pk),
                op: BinaryOperator::Gt,
                right: value(Value::Uint(*id)),
            }));
            let cond2 = Expr::BinaryOp(Box::new(BinaryOp {
                left: cond2_left,
                op: BinaryOperator::And,
                right: cond2_right,
            }));
            let where_cond = Expr::BinaryOp(Box::new(BinaryOp {
                left: cond1,
                op: BinaryOperator::Or,
                right: cond2,
            }));

            if let Some(existing_where) = builder.ast.where_clause {
                builder.ast.where_clause = Some(Expr::BinaryOp(Box::new(BinaryOp {
                    left: existing_where,
                    op: BinaryOperator::And,
                    right: where_cond,
                })));
            } else {
                builder.ast.where_clause = Some(where_cond);
            }
        }

        // Add ORDER BY
        builder = builder.order_by(ident(&self.ts_col), Some(OrderDir::Asc));
        builder = builder.order_by(ident(&self.pk), Some(OrderDir::Asc));

        // Add LIMIT
        builder = builder.limit(value(Value::Uint(limit as u64)));

        builder
    }

    fn clone_box(&self) -> Box<dyn OffsetStrategy> {
        Box::new(TimestampOffset {
            ts_col: self.ts_col.clone(),
            pk: self.pk.clone(),
            tz: self.tz,
        })
    }
}
