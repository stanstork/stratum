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
use std::borrow::Cow;
use tracing::warn;

/// An expression tree bound to a specific batch schema + mapping.
pub enum TreeExpr {
    /// Column resolved to a positional index into the row's values.
    Column(usize),
    Literal(Value),
    Binary {
        left: Box<TreeExpr>,
        op: BinaryOp,
        right: Box<TreeExpr>,
    },
    Function {
        /// Kept only for the diagnostic on evaluation failure.
        name: String,
        f: FunctionImpl,
        args: Vec<TreeExpr>,
    },
    /// Can't be resolved ahead of time (joins, `when`, `is null`, unknown
    /// function, column absent from this schema): evaluate the original
    /// expression so behaviour is identical to the fully-dynamic path.
    Dynamic(CompiledExpression),
}

impl TreeExpr {
    /// Bind `expr` to `schema`/`mapping`/`table`, resolving names to indices and
    /// function pointers. Done once per batch.
    pub fn compile(
        expr: &CompiledExpression,
        schema: &RecordSchema,
        mapping: &TransformationMetadata,
        table: &str,
    ) -> TreeExpr {
        match expr {
            CompiledExpression::Literal(v) => TreeExpr::Literal(v.clone()),

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
                    return TreeExpr::Dynamic(expr.clone());
                }

                // Otherwise it's a same-table reference, possibly renamed: resolve
                // the source name to the name present in the row, then to an index.
                let resolved = mapping.field_mappings.resolve_cow(table, key);
                Self::column_or_dynamic(schema, &resolved, expr)
            }

            CompiledExpression::Binary { left, op, right } => TreeExpr::Binary {
                left: Box::new(Self::compile(left, schema, mapping, table)),
                op: *op,
                right: Box::new(Self::compile(right, schema, mapping, table)),
            },

            CompiledExpression::Grouped(inner) => Self::compile(inner, schema, mapping, table),

            CompiledExpression::FunctionCall { name, args } => match lookup_function(name) {
                Some(f) => TreeExpr::Function {
                    name: name.clone(),
                    f,
                    args: args
                        .iter()
                        .map(|a| Self::compile(a, schema, mapping, table))
                        .collect(),
                },
                None => TreeExpr::Dynamic(expr.clone()),
            },

            // Unary, When, IsNull, IsNotNull, Array, empty DotPath: rarer and/or
            // branch-y - defer to the dynamic path so behaviour is unchanged.
            _ => TreeExpr::Dynamic(expr.clone()),
        }
    }

    /// Evaluate against one row.
    pub fn eval<'a>(
        &'a self,
        row: &'a Record,
        mapping: &TransformationMetadata,
        env_getter: &dyn Fn(&str) -> Option<String>,
    ) -> Option<Cow<'a, Value>> {
        match self {
            TreeExpr::Column(i) => row.value_at(*i).map(Cow::Borrowed),

            TreeExpr::Literal(v) => Some(Cow::Borrowed(v)),

            TreeExpr::Binary { left, op, right } => {
                let l = left.eval(row, mapping, env_getter)?;
                let r = right.eval(row, mapping, env_getter)?;
                BinaryOpEvaluator::new(&l, &r, op)
                    .evaluate()
                    .map(Cow::Owned)
            }

            TreeExpr::Function { name, f, args } => {
                let mut argv: Vec<Cow<'_, Value>> = Vec::with_capacity(args.len());
                for a in args {
                    argv.push(a.eval(row, mapping, env_getter)?);
                }

                let ctx = EvalContext::Runtime {
                    row_data: row,
                    mapping,
                    env_getter,
                };

                match f(&argv, &ctx) {
                    Ok(value) => Some(Cow::Owned(value)),
                    Err(e) => {
                        warn!(function = %name, error = %e, "function evaluation failed");
                        None
                    }
                }
            }

            TreeExpr::Dynamic(expr) => expr.evaluate(row, mapping, env_getter).map(Cow::Owned),
        }
    }

    fn column_or_dynamic(
        schema: &RecordSchema,
        name: &str,
        original: &CompiledExpression,
    ) -> TreeExpr {
        match schema.index_of(name) {
            Some(i) => TreeExpr::Column(i),
            None => TreeExpr::Dynamic(original.clone()),
        }
    }
}
