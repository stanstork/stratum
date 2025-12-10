use crate::query::{
    ast::{
        common::OrderDir,
        expr::{BinaryOp, BinaryOperator, Expr},
    },
    builder::select::{FromState, SelectBuilder},
    ident_q, value,
};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use chrono_tz::Tz;
use model::{
    core::value::Value,
    execution::pipeline::Pagination,
    pagination::{
        cursor::{Cursor, QualCol},
        offset_config::OffsetConfig,
    },
    records::row::RowData,
};
use std::{convert::TryFrom, str::FromStr, sync::Arc};

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

    /// Returns the name of the offset strategy.
    fn name(&self) -> String;
}

pub struct PkOffset {
    pub pk: QualCol,
}

pub struct NumericOffset {
    pub col: QualCol,
    pub pk: QualCol,
}

pub struct TimestampOffset {
    pub ts_col: QualCol,
    pub pk: QualCol,
    pub tz: chrono_tz::Tz,
}

pub struct DefaultOffset {
    pub offset: usize,
}

/// Helper for constructing a binary expression.
fn binary_expr(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
    Expr::BinaryOp(Box::new(BinaryOp { left, op, right }))
}

/// Helper for chaining a new predicate onto the existing WHERE clause.
fn append_where(
    mut builder: SelectBuilder<FromState>,
    predicate: Expr,
) -> SelectBuilder<FromState> {
    let combined = match builder.ast.where_clause.take() {
        Some(existing) => binary_expr(existing, BinaryOperator::And, predicate),
        None => predicate,
    };
    builder.ast.where_clause = Some(combined);
    builder
}

fn limit_expr(limit: usize) -> Expr {
    value(Value::Uint(limit as u64))
}

fn offset_expr(offset: usize) -> Expr {
    value(Value::Uint(offset as u64))
}

fn uint_literal(val: u64) -> Expr {
    value(Value::Uint(val))
}

fn int_literal(val: i64) -> Expr {
    value(Value::Int(val))
}

fn numeric_literal(val: i128) -> Expr {
    if let Ok(casted) = i64::try_from(val) {
        int_literal(casted)
    } else if val >= 0 {
        let casted =
            u64::try_from(val).expect("numeric cursor value exceeds supported unsigned range");
        uint_literal(casted)
    } else {
        panic!("numeric cursor value below supported signed range: {val}");
    }
}

fn default_pk() -> QualCol {
    QualCol {
        table: "".to_string(),
        column: "id".to_string(),
    }
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
            let where_cond = binary_expr(ident_q(pk_col), BinaryOperator::Gt, uint_literal(*id));

            builder = append_where(builder, where_cond);
        }

        // Cursor::None => no WHERE (start from beginning)

        // ORDER BY pk ASC, LIMIT ?
        builder = builder.order_by(ident_q(&self.pk), Some(OrderDir::Asc));
        builder = builder.limit(limit_expr(limit));

        builder
    }

    fn next_cursor(&self, row: &RowData) -> Cursor {
        let id_opt = match row.get_value(&self.pk.column) {
            Value::Uint(id) => Some(id),
            Value::Usize(u) => Some(u as u64),
            Value::Int(i) if i >= 0 => Some(i as u64),
            Value::Int32(i) if i >= 0 => Some(i as u64),
            Value::String(s) => s.parse::<u64>().ok(),
            _ => None,
        };

        match id_opt {
            Some(id) => Cursor::Pk {
                pk_col: self.pk.clone(),
                id,
            },
            None => Cursor::None,
        }
    }

    fn clone_box(&self) -> Box<dyn OffsetStrategy> {
        Box::new(PkOffset {
            pk: self.pk.clone(),
        })
    }

    fn name(&self) -> String {
        "pk".to_string()
    }
}

impl OffsetStrategy for NumericOffset {
    fn apply_to_builder(
        &self,
        mut builder: SelectBuilder<FromState>,
        cursor: &Cursor,
        limit: usize,
    ) -> SelectBuilder<FromState> {
        if let Some(predicate) = self.where_clause(cursor) {
            builder = append_where(builder, predicate);
        }

        builder = builder.order_by(ident_q(&self.col), Some(OrderDir::Asc));
        builder = builder.order_by(ident_q(&self.pk), Some(OrderDir::Asc));

        builder = builder.limit(limit_expr(limit));

        builder
    }

    fn next_cursor(&self, row: &RowData) -> Cursor {
        let num_v = row.get_value(&self.col.column);
        let pk_v = row.get_value(&self.pk.column);

        match (extract_numeric_value(&num_v), pk_v) {
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

    fn name(&self) -> String {
        "numeric".to_string()
    }
}

impl NumericOffset {
    fn where_clause(&self, cursor: &Cursor) -> Option<Expr> {
        match cursor {
            Cursor::CompositeNumPk {
                num_col: _,
                pk_col: _,
                val,
                id,
            } => {
                let gt_value = binary_expr(
                    ident_q(&self.col),
                    BinaryOperator::Gt,
                    numeric_literal(*val),
                );
                let eq_value = binary_expr(
                    ident_q(&self.col),
                    BinaryOperator::Eq,
                    numeric_literal(*val),
                );
                let pk_gt = binary_expr(ident_q(&self.pk), BinaryOperator::Gt, uint_literal(*id));
                let tie_breaker = binary_expr(eq_value, BinaryOperator::And, pk_gt);

                Some(binary_expr(gt_value, BinaryOperator::Or, tie_breaker))
            }
            Cursor::Numeric { val, .. } => Some(binary_expr(
                ident_q(&self.col),
                BinaryOperator::Gt,
                numeric_literal(*val),
            )),
            _ => None,
        }
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
            let ts_sql = utc_to_local_sql(*ts, &self.tz);
            // WHERE (ts > ?) OR (ts = ? AND pk > ?)
            let cond1 = binary_expr(
                ident_q(&self.ts_col),
                BinaryOperator::Gt,
                value(Value::String(ts_sql.clone())),
            );
            let cond2_left = binary_expr(
                ident_q(&self.ts_col),
                BinaryOperator::Eq,
                value(Value::String(ts_sql)),
            );
            let cond2_right = binary_expr(ident_q(&self.pk), BinaryOperator::Gt, uint_literal(*id));
            let cond2 = binary_expr(cond2_left, BinaryOperator::And, cond2_right);
            let where_cond = binary_expr(cond1, BinaryOperator::Or, cond2);

            builder = append_where(builder, where_cond);
        }

        // Add ORDER BY
        builder = builder.order_by(ident_q(&self.ts_col), Some(OrderDir::Asc));
        builder = builder.order_by(ident_q(&self.pk), Some(OrderDir::Asc));

        // Add LIMIT
        builder = builder.limit(limit_expr(limit));

        builder
    }

    fn next_cursor(&self, row: &RowData) -> Cursor {
        let ts_v = row.get_value(&self.ts_col.column);
        let pk_v = row.get_value(&self.pk.column);

        match ts_v {
            Value::Timestamp(dt_local) => {
                // Convert local timestamp to UTC micros
                if let Some(dt_utc) = self
                    .tz
                    .from_local_datetime(&dt_local.naive_local())
                    .single()
                {
                    let utc_ts = dt_utc.timestamp_micros();
                    let id = match pk_v {
                        Value::Uint(id) => id,
                        Value::Int(i) if i >= 0 => i as u64,
                        Value::String(ref s) => s.parse::<u64>().unwrap_or(0),
                        _ => 0,
                    };
                    Cursor::CompositeTsPk {
                        ts_col: self.ts_col.clone(),
                        pk_col: self.pk.clone(),
                        ts: utc_ts,
                        id,
                    }
                } else {
                    Cursor::None
                }
            }
            Value::TimestampNaive(dt_local) => {
                if let Some(dt_utc) = self.tz.from_local_datetime(&dt_local).single() {
                    let utc_ts = dt_utc.timestamp_micros();
                    let id = match pk_v {
                        Value::Uint(id) => id,
                        Value::Int(i) if i >= 0 => i as u64,
                        Value::String(ref s) => s.parse::<u64>().unwrap_or(0),
                        _ => 0,
                    };
                    Cursor::CompositeTsPk {
                        ts_col: self.ts_col.clone(),
                        pk_col: self.pk.clone(),
                        ts: utc_ts,
                        id,
                    }
                } else {
                    Cursor::None
                }
            }
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

    fn name(&self) -> String {
        "timestamp".to_string()
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
            builder = builder.offset(offset_expr(*offset));
        }
        // Add LIMIT
        builder = builder.limit(limit_expr(limit));

        builder
    }

    fn next_cursor(&self, _row: &RowData) -> Cursor {
        Cursor::Default {
            offset: self.offset,
        }
    }

    fn clone_box(&self) -> Box<dyn OffsetStrategy> {
        Box::new(DefaultOffset {
            offset: self.offset,
        })
    }

    fn name(&self) -> String {
        "default".to_string()
    }
}
pub struct OffsetStrategyFactory;

impl OffsetStrategyFactory {
    /// Build a strategy from configuration.
    pub fn from_config(config: &OffsetConfig) -> Arc<dyn OffsetStrategy> {
        // If user didn't specify a cursor column -> default PK "id".
        let strategy = config
            .strategy
            .as_deref()
            .unwrap_or("default")
            .to_lowercase();

        match strategy.as_str() {
            "pk" => Arc::new(PkOffset {
                pk: config
                    .cursor
                    .clone()
                    .expect("PK offset requires 'cursor' column"),
            }),

            "numeric" => {
                let col = config
                    .cursor
                    .clone()
                    .expect("Numeric offset requires 'cursor' column");
                let pk = config
                    .tiebreaker
                    .clone()
                    .expect("Numeric offset requires 'tiebreaker' column");
                Arc::new(NumericOffset { col, pk })
            }

            "timestamp" => {
                let ts_col = config
                    .cursor
                    .clone()
                    .unwrap_or_else(|| panic!("Timestamp offset requires 'cursor' column"));
                let pk = config
                    .tiebreaker
                    .clone()
                    .expect("Timestamp offset requires 'tiebreaker' column");
                let tz = config
                    .timezone
                    .as_deref()
                    .unwrap_or("UTC")
                    .parse::<chrono_tz::Tz>()
                    .unwrap_or(chrono_tz::UTC);
                Arc::new(TimestampOffset { ts_col, pk, tz })
            }

            "default" => Arc::new(DefaultOffset { offset: 0 }),

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
                    pk: default_pk(),
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
                pk: default_pk(),
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

    pub fn from_pagination(pagination: &Option<Pagination>) -> Arc<dyn OffsetStrategy> {
        if let Some(pagination) = pagination {
            let mut cursor: Option<QualCol> = None;
            let mut tiebreaker: Option<QualCol> = None;
            let mut timezone: Option<String> = None;

            if !pagination.cursor.is_empty() {
                cursor = Some(QualCol::from_str(&pagination.cursor).unwrap());
            }

            if let Some(tb) = &pagination.tiebreaker {
                tiebreaker = Some(QualCol::from_str(tb).unwrap());
            }

            if let Some(tz) = &pagination.timezone {
                timezone = Some(tz.clone());
            }

            let config = OffsetConfig {
                strategy: Some(pagination.strategy.clone()),
                cursor,
                tiebreaker,
                timezone,
            };

            OffsetStrategyFactory::from_config(&config)
        } else {
            OffsetStrategyFactory::default_strategy()
        }
    }

    pub fn default_strategy() -> Arc<dyn OffsetStrategy> {
        Arc::new(DefaultOffset { offset: 0 })
    }
}

fn extract_numeric_value(val: &Value) -> Option<i128> {
    match val {
        Value::Int(i) => Some(*i as i128),
        Value::Int32(i) => Some(*i as i128),
        Value::Uint(u) => Some(*u as i128),
        Value::Usize(u) => Some(*u as i128),
        Value::Float(f) => {
            if f.is_finite() {
                Some(*f as i128)
            } else {
                None
            }
        }
        Value::String(s) => s.parse::<i128>().ok(),
        _ => None,
    }
}

fn utc_to_local_sql(ts_utc: i64, user_tz: &Tz) -> String {
    let dt_utc = Utc.timestamp_micros(ts_utc).unwrap();
    let dt_local = dt_utc.with_timezone(user_tz);
    format!("{}", dt_local.format("%Y-%m-%d %H:%M:%S"))
}
