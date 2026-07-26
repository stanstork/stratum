use crate::{
    ast::{
        common::OrderDir,
        expr::{BinaryOp, BinaryOperator, Expr},
    },
    builder::select::{FromState, SelectBuilder},
    ident_q, value,
};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use model::{
    core::value::Value,
    execution::pipeline::Pagination,
    pagination::{
        cursor::{Cursor, QualCol},
        offset_config::OffsetConfig,
    },
    records::Record,
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
    fn next_cursor(&self, row: &Record) -> Cursor;

    /// Clones the boxed trait object.
    fn clone_box(&self) -> Box<dyn OffsetStrategy>;

    /// Returns the name of the offset strategy.
    fn name(&self) -> String;

    /// Whether this is the fallback OFFSET strategy with no deterministic order.
    fn is_default(&self) -> bool {
        false
    }
}

pub struct PkOffset {
    pub pk: QualCol,
    pub lane: Option<(u64, u64)>,
}

impl PkOffset {
    pub fn new(pk: QualCol) -> Self {
        Self { pk, lane: None }
    }

    /// Confine this strategy to the half-open key range `[lo, hi)`.
    pub fn with_lane(mut self, lo: u64, hi: u64) -> Self {
        self.lane = Some((lo, hi));
        self
    }
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

/// Keyset pagination over an ordered list of key columns (typically the primary
/// key). Deterministic and correct for composite and non-integer keys.
pub struct KeysetOffset {
    pub keys: Vec<QualCol>,
    pub lane: Option<(u64, u64)>,
}

impl KeysetOffset {
    pub fn new(keys: Vec<QualCol>) -> Self {
        Self { keys, lane: None }
    }

    /// Confine this scan to the half-open range `[lo, hi)` on the first key.
    pub fn with_lane(mut self, lo: u64, hi: u64) -> Self {
        self.lane = Some((lo, hi));
        self
    }
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
    value(Value::Int(i64::try_from(limit).unwrap_or(i64::MAX)))
}

fn offset_expr(offset: usize) -> Expr {
    value(Value::UInt(offset as u64))
}

fn uint_literal(val: u64) -> Expr {
    value(Value::UInt(val))
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

        // Confine to this lane's key range: pk >= lo AND pk < hi.
        if let Some((lo, hi)) = self.lane {
            let lo_cond = binary_expr(ident_q(&self.pk), BinaryOperator::GtEq, uint_literal(lo));
            builder = append_where(builder, lo_cond);
            let hi_cond = binary_expr(ident_q(&self.pk), BinaryOperator::Lt, uint_literal(hi));
            builder = append_where(builder, hi_cond);
        }

        // ORDER BY pk ASC, LIMIT ?
        builder = builder.order_by(ident_q(&self.pk), Some(OrderDir::Asc));
        builder = builder.limit(limit_expr(limit));

        builder
    }

    fn next_cursor(&self, row: &Record) -> Cursor {
        let id_opt = match row.get_value(&self.pk.column) {
            Value::UInt(id) => Some(id),
            Value::Int(i) if i >= 0 => Some(i as u64),
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
            lane: self.lane,
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

    fn next_cursor(&self, row: &Record) -> Cursor {
        let num_v = row.get_value(&self.col.column);
        let pk_v = row.get_value(&self.pk.column);

        let pk_id: Option<u64> = match &pk_v {
            Value::UInt(id) => Some(*id),
            Value::Int(i) if *i >= 0 => Some(*i as u64),
            Value::String(s) => s.parse::<u64>().ok(),
            _ => None,
        };

        match (extract_numeric_value(&num_v), pk_id) {
            (Some(val), Some(id)) => Cursor::CompositeNumPk {
                num_col: self.col.clone(),
                pk_col: self.pk.clone(),
                val,
                id,
            },
            _ => Cursor::Default { offset: 0 },
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
            let dt_local = Utc
                .timestamp_micros(*ts)
                .unwrap()
                .with_timezone(&self.tz)
                .naive_local();
            let ts_value = Value::Timestamp {
                value: dt_local,
                offset_secs: None,
            };
            // WHERE (ts > ?) OR (ts = ? AND pk > ?)
            let cond1 = binary_expr(
                ident_q(&self.ts_col),
                BinaryOperator::Gt,
                value(ts_value.clone()),
            );
            let cond2_left =
                binary_expr(ident_q(&self.ts_col), BinaryOperator::Eq, value(ts_value));
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

    fn next_cursor(&self, row: &Record) -> Cursor {
        let ts_v = row.get_value(&self.ts_col.column);
        let pk_v = row.get_value(&self.pk.column);

        let extract_pk_id = |pk: &Value| -> u64 {
            match pk {
                Value::UInt(id) => *id,
                Value::Int(i) if *i >= 0 => *i as u64,
                Value::String(s) => s.parse::<u64>().unwrap_or(0),
                _ => 0,
            }
        };

        match ts_v {
            // Timestamp with timezone offset
            Value::Timestamp {
                value: dt_local,
                offset_secs: Some(_),
            } => {
                // Convert local timestamp to UTC micros
                if let Some(dt_utc) = self.tz.from_local_datetime(&dt_local).single() {
                    let utc_ts = dt_utc.timestamp_micros();
                    Cursor::CompositeTsPk {
                        ts_col: self.ts_col.clone(),
                        pk_col: self.pk.clone(),
                        ts: utc_ts,
                        id: extract_pk_id(&pk_v),
                    }
                } else {
                    Cursor::None
                }
            }
            // Timestamp without timezone
            Value::Timestamp {
                value: dt_local,
                offset_secs: None,
            } => {
                if let Some(dt_utc) = self.tz.from_local_datetime(&dt_local).single() {
                    let utc_ts = dt_utc.timestamp_micros();
                    Cursor::CompositeTsPk {
                        ts_col: self.ts_col.clone(),
                        pk_col: self.pk.clone(),
                        ts: utc_ts,
                        id: extract_pk_id(&pk_v),
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

    fn next_cursor(&self, _row: &Record) -> Cursor {
        Cursor::Default {
            offset: self.offset,
        }
    }

    fn clone_box(&self) -> Box<dyn OffsetStrategy> {
        Box::new(DefaultOffset {
            offset: self.offset,
        })
    }

    fn is_default(&self) -> bool {
        true
    }

    fn name(&self) -> String {
        "default".to_string()
    }
}

impl KeysetOffset {
    /// `(k1 > v1) OR (k1 = v1 AND k2 > v2) OR ...` - a strict lexicographic
    /// "greater than the cursor" over the key columns.
    fn after(&self, values: &[Value]) -> Option<Expr> {
        let n = self.keys.len().min(values.len());
        if n == 0 {
            return None;
        }
        let mut terms: Vec<Expr> = Vec::with_capacity(n);
        for i in 0..n {
            // ki > vi, with equality on every earlier key prepended.
            let mut term = binary_expr(
                ident_q(&self.keys[i]),
                BinaryOperator::Gt,
                value(values[i].clone()),
            );
            for (key, val) in self.keys[..i].iter().zip(&values[..i]) {
                let eq = binary_expr(ident_q(key), BinaryOperator::Eq, value(val.clone()));
                term = binary_expr(eq, BinaryOperator::And, term);
            }
            terms.push(term);
        }
        terms
            .into_iter()
            .reduce(|a, b| binary_expr(a, BinaryOperator::Or, b))
    }
}

impl OffsetStrategy for KeysetOffset {
    fn apply_to_builder(
        &self,
        mut builder: SelectBuilder<FromState>,
        cursor: &Cursor,
        limit: usize,
    ) -> SelectBuilder<FromState> {
        if let Cursor::Keyset { values, .. } = cursor
            && let Some(predicate) = self.after(values)
        {
            builder = append_where(builder, predicate);
        }

        // Confine to this lane's range on the first (primary) key column.
        if let (Some((lo, hi)), Some(first)) = (self.lane, self.keys.first()) {
            let lo_cond = binary_expr(ident_q(first), BinaryOperator::GtEq, uint_literal(lo));
            builder = append_where(builder, lo_cond);
            let hi_cond = binary_expr(ident_q(first), BinaryOperator::Lt, uint_literal(hi));
            builder = append_where(builder, hi_cond);
        }
        for key in &self.keys {
            builder = builder.order_by(ident_q(key), Some(OrderDir::Asc));
        }
        builder = builder.limit(limit_expr(limit));
        builder
    }

    fn next_cursor(&self, row: &Record) -> Cursor {
        let values: Vec<Value> = self.keys.iter().map(|k| row.get_value(&k.column)).collect();
        // A NULL key value cannot anchor a `>` boundary; fall back rather than
        // silently skip or repeat rows. (Primary keys are non-null, so this only
        // guards misuse.)
        if values.iter().any(|v| matches!(v, Value::Null)) {
            return Cursor::None;
        }
        Cursor::Keyset {
            keys: self.keys.clone(),
            values,
        }
    }

    fn clone_box(&self) -> Box<dyn OffsetStrategy> {
        Box::new(KeysetOffset {
            keys: self.keys.clone(),
            lane: self.lane,
        })
    }

    fn name(&self) -> String {
        "keyset".to_string()
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
            "pk" => {
                let cursor = config
                    .cursor
                    .clone()
                    .expect("PK offset requires 'cursor' column");
                match config.tiebreaker.clone() {
                    // A tiebreaker makes the order row-unique: the boundary becomes
                    // `(pk, tb) > (last_pk, last_tb)`, so a `with` join that fans out
                    // (1:N) can't drop the tail of a group at a batch boundary the way
                    // a bare `pk > last` does.
                    Some(tb) => Arc::new(KeysetOffset::new(vec![cursor, tb])),
                    None => Arc::new(PkOffset {
                        pk: cursor,
                        lane: None,
                    }),
                }
            }

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
            Cursor::Pk { pk_col, .. } => Arc::new(PkOffset {
                pk: pk_col.clone(),
                lane: None,
            }),

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

            Cursor::Keyset { keys, .. } => Arc::new(KeysetOffset {
                keys: keys.clone(),
                lane: None,
            }),

            Cursor::Default { offset } => Arc::new(DefaultOffset { offset: *offset }),

            Cursor::None => Arc::new(DefaultOffset { offset: 0 }), // start from beginning

            Cursor::Opaque(_) => {
                unreachable!("Cursor::Opaque is consumed by WASM source readers, not SQL offsets")
            }
        }
    }

    pub fn from_pagination(pagination: &Option<Pagination>) -> Arc<dyn OffsetStrategy> {
        if let Some(pagination) = pagination {
            let mut cursor: Option<QualCol> = None;
            let mut tiebreaker: Option<QualCol> = None;
            let mut timezone: Option<String> = None;

            if !pagination.column.is_empty() {
                cursor = Some(QualCol::from_str(&pagination.column).unwrap());
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

    /// Upgrade the fallback OFFSET strategy to keyset pagination over `table`'s
    /// primary key, which is deterministic. A non-default (user-configured)
    /// strategy is returned unchanged.
    pub fn keyset_over_pk(
        strategy: Arc<dyn OffsetStrategy>,
        table: &str,
        primary_keys: &[String],
    ) -> Arc<dyn OffsetStrategy> {
        if strategy.is_default() && !primary_keys.is_empty() {
            let keys = primary_keys
                .iter()
                .map(|col| QualCol {
                    table: table.to_string(),
                    column: col.clone(),
                })
                .collect();
            Arc::new(KeysetOffset { keys, lane: None })
        } else {
            strategy
        }
    }
}

fn extract_numeric_value(val: &Value) -> Option<i128> {
    match val {
        Value::Int(i) => Some(*i as i128),
        Value::UInt(u) => Some(*u as i128),
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

#[cfg(test)]
mod keyset_tests {
    use super::*;
    use crate::builder::select::SelectBuilder;
    use crate::dialect::Postgres;
    use crate::renderer::{Render, Renderer};
    use model::core::types::Type;
    use model::core::value::{FieldValue, Value};
    use model::records::Record;

    fn qc(col: &str) -> QualCol {
        QualCol {
            table: "t".to_string(),
            column: col.to_string(),
        }
    }

    fn render(cursor: &Cursor, keys: &[&str]) -> String {
        render_strat(
            &KeysetOffset::new(keys.iter().map(|c| qc(c)).collect()),
            cursor,
        )
    }

    fn render_strat(strat: &KeysetOffset, cursor: &Cursor) -> String {
        let builder = SelectBuilder::new().select(vec![ident_q(&qc("a"))]).from(
            crate::ast::common::TableRef {
                schema: None,
                name: "t".into(),
            },
            Some("t"),
        );
        let ast = strat.apply_to_builder(builder, cursor, 100).build();
        let dialect = Postgres;
        let mut r = Renderer::new(&dialect);
        ast.render(&mut r);
        r.finish().0
    }

    #[test]
    fn lane_bounds_confine_first_key_range() {
        // Literals render as bind params ($N), so assert on the operator shape.
        // A lane over [100, 200) on the first page: both range predicates appear,
        // no cursor predicate, ordering preserved.
        let strat = KeysetOffset::new(vec![qc("id")]).with_lane(100, 200);
        let sql = render_strat(&strat, &Cursor::None);
        assert!(sql.contains(r#""t"."id" >="#), "lower bound missing: {sql}");
        assert!(sql.contains(r#""t"."id" <"#), "upper bound missing: {sql}");
        assert!(
            !sql.contains(r#""t"."id" > $"#),
            "no cursor on first page: {sql}"
        );
        assert!(
            sql.contains(r#"ORDER BY "t"."id" ASC"#),
            "ordering missing: {sql}"
        );

        // Mid-lane: the strict cursor predicate composes with the range bounds.
        let cursor = Cursor::Keyset {
            keys: vec![qc("id")],
            values: vec![Value::Int(150)],
        };
        let sql = render_strat(&strat, &cursor);
        assert!(sql.contains(r#""t"."id" > $"#), "cursor missing: {sql}");
        assert!(sql.contains(r#""t"."id" >="#), "lower bound missing: {sql}");
        assert!(sql.contains(r#""t"."id" <"#), "upper bound missing: {sql}");
    }

    #[test]
    fn first_page_orders_by_all_keys_no_where() {
        let sql = render(&Cursor::None, &["actor_id", "film_id"]);
        assert!(
            sql.contains(r#"ORDER BY "t"."actor_id" ASC, "t"."film_id" ASC"#),
            "got: {sql}"
        );
        assert!(!sql.contains("WHERE"), "first page has no cursor: {sql}");
    }

    #[test]
    fn composite_cursor_is_lexicographic() {
        let cursor = Cursor::Keyset {
            keys: vec![qc("actor_id"), qc("film_id")],
            values: vec![Value::Int(5), Value::Int(9)],
        };
        let sql = render(&cursor, &["actor_id", "film_id"]);
        // (actor_id > 5) OR (actor_id = 5 AND film_id > 9)
        assert!(sql.contains("WHERE"), "{sql}");
        assert!(sql.contains(r#""t"."actor_id" >"#), "{sql}");
        assert!(sql.contains(r#""t"."actor_id" ="#), "{sql}");
        assert!(sql.contains(r#""t"."film_id" >"#), "{sql}");
    }

    #[test]
    fn next_cursor_reads_all_key_values() {
        let strat = KeysetOffset::new(vec![qc("actor_id"), qc("film_id")]);
        let row = Record {
            schema: "t".into(),
            fields: vec![
                FieldValue {
                    name: "actor_id".into(),
                    value: Some(Value::Int(3)),
                    data_type: Type::Boolean,
                },
                FieldValue {
                    name: "film_id".into(),
                    value: Some(Value::Int(7)),
                    data_type: Type::Boolean,
                },
            ],
            op_type: Default::default(),
        };
        match strat.next_cursor(&row) {
            Cursor::Keyset { values, .. } => assert_eq!(values.len(), 2),
            other => panic!("expected keyset cursor, got {other:?}"),
        }
    }

    #[test]
    fn default_upgrades_to_keyset_only_with_pk() {
        let d = OffsetStrategyFactory::default_strategy();
        assert_eq!(
            OffsetStrategyFactory::keyset_over_pk(d.clone(), "t", &["id".into()]).name(),
            "keyset"
        );
        // No PK -> stays default.
        assert_eq!(
            OffsetStrategyFactory::keyset_over_pk(d.clone(), "t", &[]).name(),
            "default"
        );
        // A configured (non-default) strategy is untouched even with a PK.
        let pk = OffsetStrategyFactory::from_cursor(&Cursor::Pk {
            pk_col: qc("id"),
            id: 0,
        });
        assert_eq!(
            OffsetStrategyFactory::keyset_over_pk(pk, "t", &["id".into()]).name(),
            "pk"
        );
    }
}
