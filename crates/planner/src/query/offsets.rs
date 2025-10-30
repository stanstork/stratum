use crate::query::{
    ast::{
        common::OrderDir,
        expr::{BinaryOp, BinaryOperator, Expr},
    },
    builder::select::{FromState, SelectBuilder},
    ident, value,
};
use async_trait::async_trait;
use model::{
    core::value::Value,
    pagination::{cursor::Cursor, offset_config::OffsetConfig},
    records::row::RowData,
};

#[async_trait]
pub trait OffsetStrategy: Send + Sync {
    /// Applies the pagination logic (WHERE and ORDER BY) to a SelectBuilder.
    fn apply_to_builder(
        &self,
        builder: SelectBuilder<FromState>,
        cursor: &Cursor,
        limit: usize,
    ) -> SelectBuilder<FromState>;

    /// Generates the next cursor based on the last fetched row.
    fn next_cursor(&self, row: &RowData) -> Cursor;

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
        if let Cursor::Pk { pk_col, id } = cursor {
            // WHERE pk > ?
            let where_cond = Expr::BinaryOp(Box::new(BinaryOp {
                left: ident(pk_col),
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

    fn next_cursor(&self, row: &RowData) -> Cursor {
        let pk_value = row.get_value(&self.pk);
        match pk_value {
            Value::Uint(id) => Cursor::Pk {
                pk_col: self.pk.clone(),
                id,
            },
            _ => Cursor::None, // Fallback for unexpected types
        }
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

    fn next_cursor(&self, row: &RowData) -> Cursor {
        let num_v = row.get_value(&self.col);
        let pk_v = row.get_value(&self.pk);

        let to_i128 = |v: &Value| -> Option<i128> {
            match v {
                Value::Int(i) => Some(*i as i128),
                Value::Uint(u) => Some(*u as i128),
                Value::Float(d) => Some(d.to_string().parse::<i128>().ok()?),
                _ => None,
            }
        };

        match (to_i128(&num_v), pk_v) {
            (Some(val), Value::Uint(id)) => Cursor::CompositeNumPk {
                num_col: self.col.clone(),
                pk_col: self.pk.clone(),
                val,
                id,
            },
            _ => Cursor::None,
        }
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

    fn next_cursor(&self, row: &RowData) -> Cursor {
        let ts_v = row.get_value(&self.ts_col);
        let pk_v = row.get_value(&self.pk);

        // Expect ts is stored canonically as micros (i64) in RowData (after normalization).
        match (ts_v, pk_v) {
            (Value::Int(ts), Value::Uint(id)) => Cursor::CompositeTsPk {
                ts_col: self.ts_col.clone(),
                pk_col: self.pk.clone(),
                ts,
                id,
            },
            _ => Cursor::None,
        }
    }

    fn clone_box(&self) -> Box<dyn OffsetStrategy> {
        Box::new(TimestampOffset {
            ts_col: self.ts_col.clone(),
            pk: self.pk.clone(),
            tz: self.tz,
        })
    }
}

pub fn strategy_from_config(config: &OffsetConfig) -> Box<dyn OffsetStrategy> {
    // If user didn't specify a cursor column -> default PK "id".
    let default_pk = "id".to_string();

    // If the config includes a 'strategy' hint, use it; otherwise infer:
    // - no cursor  -> PK
    // - cursor + no tiebreaker -> Numeric (single) by default
    // - cursor + tiebreaker    -> use Timestamp if name suggests time, else Numeric
    let strategy =
        config
            .strategy
            .as_deref()
            .unwrap_or_else(|| match (&config.cursor, &config.tiebreaker) {
                (None, _) => "pk",
                (Some(_), None) => "numeric",
                (Some(c), Some(_))
                    if c.to_lowercase().contains("time") || c.to_lowercase().contains("date") =>
                {
                    "timestamp"
                }
                _ => "numeric",
            });

    match strategy {
        "pk" => Box::new(PkOffset {
            pk: config.cursor.clone().unwrap_or(default_pk),
        }),

        "numeric" => {
            let col = config
                .cursor
                .clone()
                .unwrap_or_else(|| panic!("Numeric offset requires 'cursor' column"));
            let pk = config.tiebreaker.clone().unwrap_or(default_pk);
            Box::new(NumericOffset { col, pk })
        }

        "timestamp" => {
            let ts_col = config
                .cursor
                .clone()
                .unwrap_or_else(|| panic!("Timestamp offset requires 'cursor' column"));
            let pk = config.tiebreaker.clone().unwrap_or(default_pk);
            let tz = config
                .timezone
                .as_deref()
                .unwrap_or("UTC")
                .parse::<chrono_tz::Tz>()
                .unwrap_or(chrono_tz::UTC);
            Box::new(TimestampOffset { ts_col, pk, tz })
        }

        other => panic!("Unsupported offset strategy: {other}"),
    }
}

/// Build a strategy from a concrete cursor (e.g., when resuming).
pub fn offset_strategy_from_cursor(cursor: &Cursor) -> Box<dyn OffsetStrategy> {
    match cursor {
        Cursor::Pk { pk_col, .. } => Box::new(PkOffset { pk: pk_col.clone() }),

        Cursor::Numeric { col, .. } => {
            // Without a pk in the cursor, default tiebreaker to "id".
            Box::new(NumericOffset {
                col: col.clone(),
                pk: "id".to_string(),
            })
        }

        Cursor::CompositeNumPk {
            num_col, pk_col, ..
        } => Box::new(NumericOffset {
            col: num_col.clone(),
            pk: pk_col.clone(),
        }),

        Cursor::Timestamp { col, .. } => Box::new(TimestampOffset {
            ts_col: col.clone(),
            pk: "id".to_string(),
            tz: chrono_tz::UTC,
        }),

        Cursor::CompositeTsPk { ts_col, pk_col, .. } => Box::new(TimestampOffset {
            ts_col: ts_col.clone(),
            pk: pk_col.clone(),
            tz: chrono_tz::UTC,
        }),

        Cursor::None => panic!("Cannot derive offset strategy from Cursor::None"),
    }
}
