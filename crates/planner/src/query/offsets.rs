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
use std::sync::Arc;

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

pub struct DefaultOffset {
    pub offset: usize,
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

        // Cursor::None => no WHERE (start from beginning)

        // ORDER BY pk ASC, LIMIT ?
        builder = builder.order_by(ident(&self.pk), Some(OrderDir::Asc));
        builder = builder.limit(value(Value::Uint(limit as u64)));

        builder
    }

    fn next_cursor(&self, row: &RowData) -> Cursor {
        match row.get_value(&self.pk) {
            Value::Uint(id) => Cursor::Pk {
                pk_col: self.pk.clone(),
                id,
            },
            Value::Int(i) if i >= 0 => Cursor::Pk {
                pk_col: self.pk.clone(),
                id: i as u64,
            },
            Value::String(s) => {
                if let Ok(id) = s.parse::<u64>() {
                    Cursor::Pk {
                        pk_col: self.pk.clone(),
                        id,
                    }
                } else {
                    Cursor::None // couldn't advance; treat as end/error path
                }
            }
            _ => Cursor::None, // unexpected type; upstream should normalize PKs to Uint
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
            _ => Cursor::Default { offset: 0 }, // TODO: better fallback
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
            _ => Cursor::Default { offset: 0 }, // TODO: better fallback
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

impl OffsetStrategy for DefaultOffset {
    fn apply_to_builder(
        &self,
        mut builder: SelectBuilder<FromState>,
        cursor: &Cursor,
        limit: usize,
    ) -> SelectBuilder<FromState> {
        // Add offset based on cursor
        if let Cursor::Default { offset } = cursor {
            // OFFSET ?
            builder = builder.offset(value(Value::Uint(*offset as u64)));
        }
        // Add LIMIT
        builder = builder.limit(value(Value::Uint(limit as u64)));

        builder
    }

    fn next_cursor(&self, _row: &RowData) -> Cursor {
        Cursor::Default {
            offset: self.offset + 1,
        }
    }

    fn clone_box(&self) -> Box<dyn OffsetStrategy> {
        Box::new(DefaultOffset {
            offset: self.offset,
        })
    }
}
pub struct OffsetStrategyFactory;

impl OffsetStrategyFactory {
    /// Build a strategy from configuration.
    pub fn from_config(config: &OffsetConfig) -> Arc<dyn OffsetStrategy> {
        // If user didn't specify a cursor column -> default PK "id".
        let default_pk = "id".to_string();

        // If the config includes a 'strategy' hint, use it; otherwise infer:
        // - no cursor  -> PK
        // - cursor + no tiebreaker -> Numeric (single) by default
        // - cursor + tiebreaker    -> use Timestamp if name suggests time, else Numeric
        let strategy = config.strategy.as_deref().unwrap_or_else(|| {
            match (&config.cursor, &config.tiebreaker) {
                (None, _) => "pk",
                (Some(_), None) => "numeric",
                (Some(c), Some(_))
                    if c.to_lowercase().contains("time") || c.to_lowercase().contains("date") =>
                {
                    "timestamp"
                }
                _ => "numeric",
            }
        });

        match strategy {
            "pk" => Arc::new(PkOffset {
                pk: config.cursor.clone().unwrap_or(default_pk),
            }),

            "numeric" => {
                let col = config
                    .cursor
                    .clone()
                    .unwrap_or_else(|| panic!("Numeric offset requires 'cursor' column"));
                let pk = config.tiebreaker.clone().unwrap_or(default_pk);
                Arc::new(NumericOffset { col, pk })
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
                Arc::new(TimestampOffset { ts_col, pk, tz })
            }

            other => panic!("Unsupported offset strategy: {other}"),
        }
    }

    /// Build a strategy from a concrete cursor (e.g., when resuming).
    pub fn from_cursor(cursor: &Cursor) -> Arc<dyn OffsetStrategy> {
        match cursor {
            Cursor::Pk { pk_col, .. } => Arc::new(PkOffset { pk: pk_col.clone() }),

            Cursor::Numeric { col, .. } => {
                // Without a pk in the cursor, default tiebreaker to "id".
                Arc::new(NumericOffset {
                    col: col.clone(),
                    pk: "id".to_string(),
                })
            }

            Cursor::CompositeNumPk {
                num_col, pk_col, ..
            } => Arc::new(NumericOffset {
                col: num_col.clone(),
                pk: pk_col.clone(),
            }),

            Cursor::Timestamp { col, .. } => Arc::new(TimestampOffset {
                ts_col: col.clone(),
                pk: "id".to_string(),
                tz: chrono_tz::UTC,
            }),

            Cursor::CompositeTsPk { ts_col, pk_col, .. } => Arc::new(TimestampOffset {
                ts_col: ts_col.clone(),
                pk: pk_col.clone(),
                tz: chrono_tz::UTC,
            }),

            Cursor::Default { offset } => Arc::new(DefaultOffset { offset: *offset }),

            Cursor::None => Arc::new(DefaultOffset { offset: 0 }), // start from beginning
        }
    }

    /// Build a strategy from SMQL offset syntax.
    pub fn from_smql(smql_offset: &smql_syntax::ast::offset::Offset) -> Arc<dyn OffsetStrategy> {
        let mut strategy: Option<String> = None;
        let mut cursor: Option<String> = None;
        let mut tiebreaker: Option<String> = None;
        let mut timezone: Option<String> = None;

        for pair in &smql_offset.pairs {
            match pair.key {
                smql_syntax::ast::offset::OffsetKey::Strategy => {
                    strategy = Some(pair.value.clone())
                }
                smql_syntax::ast::offset::OffsetKey::Cursor => cursor = Some(pair.value.clone()),
                smql_syntax::ast::offset::OffsetKey::TieBreaker => {
                    tiebreaker = Some(pair.value.clone())
                }
                smql_syntax::ast::offset::OffsetKey::TimeZone => {
                    timezone = Some(pair.value.clone())
                }
            }
        }

        let config = OffsetConfig {
            strategy,
            cursor,
            tiebreaker,
            timezone,
        };

        println!("OffsetConfig from SMQL: {:?}", config);

        Self::from_config(&config)
    }
}
