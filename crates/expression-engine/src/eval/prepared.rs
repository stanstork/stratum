use crate::context::EvalContext;
use crate::eval::binary::BinaryOpEvaluator;
use crate::eval::runtime::{lookup_function, Evaluator};
use crate::functions::FunctionImpl;
use model::{
    core::value::Value,
    execution::expr::{BinaryOp, CompiledExpression},
    records::{Record, RecordSchema},
    transform::mapping::TransformationMetadata,
};
use tracing::warn;

/// An expression bound to a specific batch schema + mapping.
pub enum PreparedExpr {
    /// Column resolved to a positional index into the row's values.
    Column(usize),
    Literal(Value),
    Binary {
        left: Box<PreparedExpr>,
        op: BinaryOp,
        right: Box<PreparedExpr>,
    },
    Function {
        /// Kept only for the diagnostic on evaluation failure.
        name: String,
        f: FunctionImpl,
        args: Vec<PreparedExpr>,
    },
    /// Not confidently preparable (joins, `when`, `is null`, unknown function,
    /// column absent from this schema): evaluate the original expression so
    /// behaviour is identical to the un-prepared path.
    Dynamic(CompiledExpression),
}

impl PreparedExpr {
    /// Bind `expr` to `schema`/`mapping`/`table`, resolving names to indices and
    /// function pointers. Done once per batch.
    pub fn compile(
        expr: &CompiledExpression,
        schema: &RecordSchema,
        mapping: &TransformationMetadata,
        table: &str,
    ) -> PreparedExpr {
        match expr {
            CompiledExpression::Literal(v) => PreparedExpr::Literal(v.clone()),

            CompiledExpression::Identifier(name) => Self::column_or_dynamic(schema, name, expr),

            CompiledExpression::DotPath(segments) if segments.len() == 1 => {
                Self::column_or_dynamic(schema, &segments[0], expr)
            }

            CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
                let entity = &segments[0];
                let key = &segments[1];

                // A reference resolvable through a joined/foreign table takes the
                // dynamic `mapped.or(raw)` path - keep exact semantics.
                let has_foreign = mapping
                    .foreign_fields
                    .get(entity)
                    .map(|fields| {
                        fields
                            .iter()
                            .any(|lk| lk.target.is_some() && lk.field.eq_ignore_ascii_case(key))
                    })
                    .unwrap_or(false);

                if has_foreign {
                    return PreparedExpr::Dynamic(expr.clone());
                }

                // Otherwise it's a same-table reference, possibly renamed: resolve
                // the source name to the name present in the row, then to an index.
                let resolved = mapping.field_mappings.resolve_cow(table, key);
                Self::column_or_dynamic(schema, &resolved, expr)
            }

            CompiledExpression::Binary { left, op, right } => PreparedExpr::Binary {
                left: Box::new(Self::compile(left, schema, mapping, table)),
                op: *op,
                right: Box::new(Self::compile(right, schema, mapping, table)),
            },

            CompiledExpression::Grouped(inner) => Self::compile(inner, schema, mapping, table),

            CompiledExpression::FunctionCall { name, args } => match lookup_function(name) {
                Some(f) => PreparedExpr::Function {
                    name: name.clone(),
                    f,
                    args: args
                        .iter()
                        .map(|a| Self::compile(a, schema, mapping, table))
                        .collect(),
                },
                None => PreparedExpr::Dynamic(expr.clone()),
            },

            // Unary, When, IsNull, IsNotNull, Array, empty DotPath: rarer and/or
            // branch-y - defer to the dynamic path so behaviour is unchanged.
            _ => PreparedExpr::Dynamic(expr.clone()),
        }
    }

    /// Evaluate against one row.
    pub fn eval(
        &self,
        row: &Record,
        mapping: &TransformationMetadata,
        env_getter: &dyn Fn(&str) -> Option<String>,
    ) -> Option<Value> {
        match self {
            PreparedExpr::Column(i) => row.value_at(*i).cloned(),

            PreparedExpr::Literal(v) => Some(v.clone()),

            PreparedExpr::Binary { left, op, right } => {
                let l = left.eval(row, mapping, env_getter)?;
                let r = right.eval(row, mapping, env_getter)?;
                BinaryOpEvaluator::new(&l, &r, op).evaluate()
            }

            PreparedExpr::Function { name, f, args } => {
                let mut argv = Vec::with_capacity(args.len());
                for a in args {
                    argv.push(a.eval(row, mapping, env_getter)?);
                }
                let ctx = EvalContext::Runtime {
                    row_data: row,
                    mapping,
                    env_getter,
                };
                match f(&argv, &ctx) {
                    Ok(value) => Some(value),
                    Err(e) => {
                        warn!(function = %name, error = %e, "function evaluation failed");
                        None
                    }
                }
            }

            PreparedExpr::Dynamic(expr) => expr.evaluate(row, mapping, env_getter),
        }
    }

    fn column_or_dynamic(
        schema: &RecordSchema,
        name: &str,
        original: &CompiledExpression,
    ) -> PreparedExpr {
        match schema.index_of(name) {
            Some(i) => PreparedExpr::Column(i),
            None => PreparedExpr::Dynamic(original.clone()),
        }
    }
}
