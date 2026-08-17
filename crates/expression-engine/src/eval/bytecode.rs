use crate::context::EvalContext;
use crate::eval::binary::BinaryOpEvaluator;
use crate::eval::runtime::lookup_function;
use crate::functions::FunctionImpl;
use model::{
    core::value::Value,
    execution::expr::{BinaryOp, CompiledExpression},
    records::{Record, RecordSchema},
    transform::mapping::TransformationMetadata,
};
use smallvec::SmallVec;
use std::borrow::Cow;
use tracing::warn;

/// One stack-machine instruction.
#[derive(Clone, Copy)]
enum Instr {
    /// Push the row value at this column index (borrowed).
    Col(u32),
    /// Push the constant-pool literal at this index (borrowed).
    Const(u32),
    /// Pop rhs, pop lhs, push `lhs op rhs`.
    Bin(BinaryOp),
    /// Call `f` over the top `argc` stack slots, replacing them with its result.
    Call {
        f: FunctionImpl,
        argc: u16,
        /// Index into `Program::names`, for the failure diagnostic only.
        name: u32,
    },

    /// Push `col[a] op col[b]`.
    ColColBin(u32, u32, BinaryOp),
    /// Push `col[a] op const[k]`.
    ColConstBin(u32, u32, BinaryOp),
    /// Push `const[k] op col[a]` (operand order preserved for non-commutative ops).
    ConstColBin(u32, u32, BinaryOp),
    /// Push `f([col[a]])` - a one-argument function applied straight to a column.
    Col1Call {
        col: u32,
        f: FunctionImpl,
        name: u32,
    },
}

/// Accumulates instructions/constants while lowering an expression tree.
#[derive(Default)]
struct Builder {
    code: Vec<Instr>,
    consts: Vec<Value>,
    names: Vec<Box<str>>,
}

/// A compiled expression: a flat instruction stream plus its constant pool.
pub struct Program {
    code: Vec<Instr>,
    consts: Vec<Value>,
    names: Vec<Box<str>>,
}

impl Program {
    /// Compile `expr` for `schema`/`mapping`/`table`, resolving names to column
    /// indices and functions to pointers - done once per batch.
    pub fn compile(
        expr: &CompiledExpression,
        schema: &RecordSchema,
        mapping: &TransformationMetadata,
        table: &str,
    ) -> Option<Program> {
        let mut b = Builder::default();

        b.emit(expr, schema, mapping, table)?;

        Some(Program {
            code: peephole(b.code),
            consts: b.consts,
            names: b.names,
        })
    }

    /// Evaluate against one row.
    pub fn eval<'a>(&'a self, row: &'a Record, ctx: &EvalContext) -> Option<Cow<'a, Value>> {
        let mut stack: SmallVec<[Cow<'a, Value>; 16]> = SmallVec::new();

        for instr in &self.code {
            match *instr {
                Instr::Col(i) => stack.push(row.eval_value_at(i as usize)?),
                Instr::Const(k) => stack.push(Cow::Borrowed(&self.consts[k as usize])),
                Instr::Bin(op) => {
                    // rhs is on top; pop rhs then lhs.
                    let r = stack.pop()?;
                    let l = stack.pop()?;
                    let out = BinaryOpEvaluator::new(&l, &r, &op).evaluate()?;
                    stack.push(Cow::Owned(out));
                }
                Instr::Call { f, argc, name } => {
                    let base = stack.len().checked_sub(argc as usize)?;
                    // The top `argc` slots ARE the argument slice - no argv alloc.
                    match f(&stack[base..], ctx) {
                        Ok(v) => {
                            stack.truncate(base);
                            stack.push(Cow::Owned(v));
                        }
                        Err(e) => {
                            self.warn_fail(name, &e);
                            return None;
                        }
                    }
                }
                Instr::ColColBin(a, b, op) => {
                    let l = row.eval_value_at(a as usize)?;
                    let r = row.eval_value_at(b as usize)?;
                    stack.push(Cow::Owned(
                        BinaryOpEvaluator::new(l.as_ref(), r.as_ref(), &op).evaluate()?,
                    ));
                }
                Instr::ColConstBin(a, k, op) => {
                    let l = row.eval_value_at(a as usize)?;
                    let r = &self.consts[k as usize];
                    stack.push(Cow::Owned(
                        BinaryOpEvaluator::new(l.as_ref(), r, &op).evaluate()?,
                    ));
                }
                Instr::ConstColBin(k, a, op) => {
                    let l = &self.consts[k as usize];
                    let r = row.eval_value_at(a as usize)?;
                    stack.push(Cow::Owned(
                        BinaryOpEvaluator::new(l, r.as_ref(), &op).evaluate()?,
                    ));
                }
                Instr::Col1Call { col, f, name } => {
                    let arg = [row.eval_value_at(col as usize)?];
                    match f(&arg, ctx) {
                        Ok(v) => stack.push(Cow::Owned(v)),
                        Err(e) => {
                            self.warn_fail(name, &e);
                            return None;
                        }
                    }
                }
            }
        }

        stack.pop()
    }

    /// Log a function failure.
    fn warn_fail(&self, name: u32, e: &crate::error::ExpressionError) {
        let name = self.names.get(name as usize).map(|s| &**s).unwrap_or("?");
        warn!(function = %name, error = %e, "function evaluation failed");
    }
}

/// Fuse common patterns into superinstructions, skipping intermediate operand-
/// stack traffic. A single forward pass; each fused window has the same net stack
/// effect (+1) as the sequence it replaces, so balance is preserved.
fn peephole(code: Vec<Instr>) -> Vec<Instr> {
    let mut out = Vec::with_capacity(code.len());
    let mut i = 0;

    while i < code.len() {
        match &code[i..] {
            // 3-instruction binary fusions
            [Instr::Col(a), Instr::Col(b), Instr::Bin(op), ..] => {
                out.push(Instr::ColColBin(*a, *b, *op));
                i += 3;
            }
            [Instr::Col(a), Instr::Const(k), Instr::Bin(op), ..] => {
                out.push(Instr::ColConstBin(*a, *k, *op));
                i += 3;
            }
            [Instr::Const(k), Instr::Col(a), Instr::Bin(op), ..] => {
                out.push(Instr::ConstColBin(*k, *a, *op));
                i += 3;
            }
            // 2-instruction unary-column-call fast path
            [Instr::Col(a), Instr::Call { f, argc: 1, name }, ..] => {
                out.push(Instr::Col1Call {
                    col: *a,
                    f: *f,
                    name: *name,
                });
                i += 2;
            }
            // Fallback: no optimization matched, push the single instruction
            [instr, ..] => {
                out.push(*instr);
                i += 1;
            }
            [] => break,
        }
    }
    out
}

impl Builder {
    fn const_idx(&mut self, v: Value) -> u32 {
        let i = self.consts.len() as u32;
        self.consts.push(v);
        i
    }

    fn name_idx(&mut self, name: &str) -> u32 {
        let i = self.names.len() as u32;
        self.names.push(name.into());
        i
    }

    /// Emit code for `expr` in postfix order. Returns `None` (aborting the whole
    /// compile) for any node the VM does not lower - mirrors `TreeExpr::compile`
    /// so the compilable set is exactly the same.
    fn emit(
        &mut self,
        expr: &CompiledExpression,
        schema: &RecordSchema,
        mapping: &TransformationMetadata,
        table: &str,
    ) -> Option<()> {
        match expr {
            CompiledExpression::Literal(v) => {
                let k = self.const_idx(v.clone());
                self.code.push(Instr::Const(k));
            }

            CompiledExpression::Identifier(name) => self.emit_column(schema, name)?,

            CompiledExpression::DotPath(segments) if segments.len() == 1 => {
                self.emit_column(schema, &segments[0])?
            }

            CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
                let entity = &segments[0];
                let key = &segments[1];

                // A reference resolvable through a joined/foreign table needs the
                // dynamic `mapped.or(raw)` path - decline to compile.
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
                    return None;
                }

                let resolved = mapping.field_mappings.resolve_cow(table, key);
                self.emit_column(schema, &resolved)?
            }

            CompiledExpression::Binary { left, op, right } => {
                self.emit(left, schema, mapping, table)?;
                self.emit(right, schema, mapping, table)?;
                self.code.push(Instr::Bin(*op));
            }

            CompiledExpression::Grouped(inner) => self.emit(inner, schema, mapping, table)?,

            CompiledExpression::FunctionCall { name, args } => {
                let f = lookup_function(name)?;
                let argc = u16::try_from(args.len()).ok()?;
                for a in args {
                    self.emit(a, schema, mapping, table)?;
                }

                let name = self.name_idx(name);
                self.code.push(Instr::Call { f, argc, name });
            }

            // Unary, When, IsNull, IsNotNull, Array, empty/other DotPath: defer to
            // the tree-walk so behaviour is unchanged.
            _ => return None,
        }
        Some(())
    }

    /// Resolve a name to a column index and emit `Col`, or decline (`None`) if the
    /// name is absent from the batch schema (the tree-walk's `Dynamic` handles it).
    fn emit_column(&mut self, schema: &RecordSchema, name: &str) -> Option<()> {
        let i = schema.index_of(name)?;
        self.code.push(Instr::Col(u32::try_from(i).ok()?));
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::TreeExpr;
    use model::{
        core::types::Type,
        execution::expr::BinaryOp,
        records::{OpType, SchemaColumn},
        transform::mapping::TransformationMetadata,
    };

    fn meta() -> TransformationMetadata {
        // All fields default: no renames, computed, foreign refs, or projection.
        TransformationMetadata {
            entities: Default::default(),
            field_mappings: Default::default(),
            foreign_fields: Default::default(),
            plugin_columns: Default::default(),
            migrated_tables: Default::default(),
            has_projection: false,
        }
    }

    fn fixture() -> (std::sync::Arc<RecordSchema>, Record) {
        let schema = RecordSchema::new(
            "orders",
            vec![
                SchemaColumn::new("amount", Type::Boolean),
                SchemaColumn::new("qty", Type::Boolean),
                SchemaColumn::new("name", Type::Boolean),
            ],
        );
        let rec = Record::new(
            std::sync::Arc::clone(&schema),
            vec![
                Some(Value::Float(2.5)),
                Some(Value::Int(3)),
                Some(Value::String("Abc".into())),
            ],
            OpType::Insert,
        );
        (schema, rec)
    }

    fn col(n: &str) -> CompiledExpression {
        CompiledExpression::Identifier(n.into())
    }
    fn lit(v: Value) -> CompiledExpression {
        CompiledExpression::Literal(v)
    }
    fn bin(l: CompiledExpression, op: BinaryOp, r: CompiledExpression) -> CompiledExpression {
        CompiledExpression::Binary {
            left: Box::new(l),
            op,
            right: Box::new(r),
        }
    }
    fn call(name: &str, args: Vec<CompiledExpression>) -> CompiledExpression {
        CompiledExpression::FunctionCall {
            name: name.into(),
            args,
        }
    }

    /// Every VM-compiled expression must evaluate to exactly the tree-walk result,
    /// exercising each base instruction and superinstruction path.
    #[test]
    fn vm_matches_tree_walk() {
        let (schema, rec) = fixture();
        let mapping = meta();
        let env = |_: &str| -> Option<String> { None };

        let cases = [
            col("amount"),                                              // Col
            lit(Value::Int(7)),                                         // Const
            bin(col("amount"), BinaryOp::Multiply, col("qty")),         // ColColBin
            bin(col("amount"), BinaryOp::Multiply, lit(Value::Int(2))), // ColConstBin
            bin(lit(Value::Int(10)), BinaryOp::Add, col("qty")),        // ConstColBin
            call("upper", vec![col("name")]),                           // Col1Call
            call(
                "concat",
                vec![col("name"), lit(Value::String("_".into())), col("name")],
            ), // base Call (multi-arg)
            call(
                "upper",
                vec![call("concat", vec![col("name"), col("name")])],
            ), // nested
            bin(
                bin(col("amount"), BinaryOp::Multiply, col("qty")),
                BinaryOp::Add,
                col("qty"),
            ), // partial fusion (inner ColColBin, outer Bin)
        ];

        for expr in &cases {
            let prog = Program::compile(expr, &schema, &mapping, "orders")
                .expect("expression should compile to the VM");
            let tree = TreeExpr::compile(expr, &schema, &mapping, "orders");

            let ctx = EvalContext::Runtime {
                row_data: &rec,
                mapping: &mapping,
                env_getter: &env,
            };
            let vm_out = prog.eval(&rec, &ctx).map(|c| c.into_owned());
            let tree_out = tree.eval(&rec, &mapping, &env).map(|c| c.into_owned());

            assert_eq!(vm_out, tree_out, "VM/tree mismatch for {expr:?}");
        }
    }

    /// A reference to a SQL NULL column must evaluate to `Value::Null`, not fail.
    #[test]
    fn null_column_evaluates_to_null_not_error() {
        let schema = RecordSchema::new(
            "orders",
            vec![
                SchemaColumn::new("amount", Type::Boolean),
                SchemaColumn::new("name", Type::Boolean),
            ],
        );
        // `name` is SQL NULL, spelled as PostgreSQL's bare `None`.
        let rec = Record::new(
            std::sync::Arc::clone(&schema),
            vec![Some(Value::Float(2.5)), None],
            OpType::Insert,
        );
        let mapping = meta();
        let env = |_: &str| -> Option<String> { None };
        let ctx = EvalContext::Runtime {
            row_data: &rec,
            mapping: &mapping,
            env_getter: &env,
        };

        // A bare column read yields the null value; superinstructions that read
        // a NULL column must read it as NULL and let the op decide, rather than
        // failing at the read. `amount * name` (arithmetic with NULL) now
        // propagates NULL instead of routing the row to the DLQ.
        for expr in [
            col("name"),
            bin(col("name"), BinaryOp::Equal, lit(Value::Int(0))),
            bin(col("amount"), BinaryOp::Multiply, col("name")),
        ] {
            let prog =
                Program::compile(&expr, &schema, &mapping, "orders").expect("compiles to VM");
            let tree = TreeExpr::compile(&expr, &schema, &mapping, "orders");

            let vm_out = prog.eval(&rec, &ctx).map(|c| c.into_owned());
            let tree_out = tree.eval(&rec, &mapping, &env).map(|c| c.into_owned());

            assert!(
                vm_out.is_some(),
                "a NULL column must not fail eval (would route the row to the DLQ): {expr:?}"
            );
            assert_eq!(
                vm_out, tree_out,
                "VM and tree walk must agree on NULL: {expr:?}"
            );
        }

        // The bare reference is the null value itself.
        let prog = Program::compile(&col("name"), &schema, &mapping, "orders").unwrap();
        assert_eq!(
            prog.eval(&rec, &ctx).map(|c| c.into_owned()),
            Some(Value::Null)
        );
    }
}
