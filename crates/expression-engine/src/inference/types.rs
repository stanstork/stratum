use model::{
    core::{
        types::{FloatSize, IntSize, Type},
        value::Value,
    },
    execution::expr::CompiledExpression,
};
use tracing::warn;

/// Infer the data type of a compiled expression
/// For cross-entity references (DotPath with 2+ segments), returns None and
/// the caller should handle fetching metadata asynchronously
pub fn infer_expression_type<F>(expr: &CompiledExpression, column_lookup: &F) -> Option<Type>
where
    F: Fn(&str) -> Option<Type>,
{
    match expr {
        CompiledExpression::Identifier(identifier) => column_lookup(identifier),

        CompiledExpression::Literal(value) => Some(match value {
            Value::String(_) => Type::Text { charset: None },
            Value::Int(_) => Type::Int {
                bits: IntSize::I64,
                unsigned: false,
                auto_increment: false,
            },
            Value::UInt(_) => Type::Int {
                bits: IntSize::I64,
                unsigned: true,
                auto_increment: false,
            },
            Value::Float(_) => Type::Float {
                bits: FloatSize::F64,
            },
            Value::Decimal(_) => Type::Decimal {
                precision: None,
                scale: None,
            },
            Value::Boolean(_) => Type::Boolean,
            Value::Date(_) => Type::Date,
            Value::Time { offset_secs, .. } => Type::Time {
                precision: None,
                with_tz: offset_secs.is_some(),
            },
            Value::Timestamp { offset_secs, .. } => Type::Timestamp {
                precision: None,
                with_tz: offset_secs.is_some(),
            },
            Value::Uuid(_) => Type::Uuid,
            Value::Json(_) => Type::Json { binary: false },
            Value::Binary(_) => Type::Varbinary { length: None },
            Value::Null => Type::Varchar {
                length: None,
                charset: None,
            }, // Default for null
            _ => Type::Text { charset: None }, // Fallback for other types
        }),

        CompiledExpression::Binary { left, right, .. } => {
            let lt = infer_expression_type(left, column_lookup)?;
            let rt = infer_expression_type(right, column_lookup)?;
            Some(get_numeric_type(&lt, &rt))
        }

        CompiledExpression::FunctionCall { name, .. } => match name.to_ascii_lowercase().as_str() {
            "lower" | "upper" | "concat" | "env" => Some(Type::Varchar {
                length: None,
                charset: None,
            }),
            _ => None,
        },

        // DotPath with 2+ segments = cross-entity reference (table.column)
        // Caller should handle fetching metadata asynchronously
        CompiledExpression::DotPath(segments) if segments.len() >= 2 => None,

        // Single-segment DotPath is just a field reference
        CompiledExpression::DotPath(segments) if segments.len() == 1 => column_lookup(&segments[0]),

        // Handle other expression types
        CompiledExpression::Unary { operand, .. } => infer_expression_type(operand, column_lookup),

        CompiledExpression::Grouped(expr) => infer_expression_type(expr, column_lookup),

        CompiledExpression::When {
            branches,
            else_expr,
        } => {
            // Try to infer from first branch value, fallback to else
            if let Some(branch) = branches.first() {
                infer_expression_type(&branch.value, column_lookup)
            } else if let Some(else_val) = else_expr {
                infer_expression_type(else_val, column_lookup)
            } else {
                None
            }
        }

        CompiledExpression::IsNull(_) | CompiledExpression::IsNotNull(_) => Some(Type::Boolean),

        CompiledExpression::Array(_) => None, // Arrays not yet supported
        CompiledExpression::DotPath(_) => None, // Empty DotPath
    }
}

fn get_numeric_type(left: &Type, right: &Type) -> Type {
    match (left, right) {
        (Type::Int { .. }, Type::Int { .. }) => {
            // For int + int, use 64-bit signed as safe default
            Type::Int {
                bits: IntSize::I64,
                unsigned: false,
                auto_increment: false,
            }
        }
        (Type::Float { .. }, Type::Float { .. }) => {
            // For float + float, use 64-bit as safe default
            Type::Float {
                bits: FloatSize::F64,
            }
        }
        (Type::Int { .. }, Type::Float { .. }) | (Type::Float { .. }, Type::Int { .. }) => {
            // Mixed int/float promotes to float
            Type::Float {
                bits: FloatSize::F64,
            }
        }
        (Type::Decimal { .. }, _) | (_, Type::Decimal { .. }) => Type::Decimal {
            precision: None,
            scale: None,
        },
        _ => {
            warn!(left = ?left, right = ?right, "incompatible types for arithmetic operation");
            Type::Text { charset: None } // Fallback to Text for unsupported types
        }
    }
}

#[cfg(test)]
mod tests {
    use core::f64;

    use super::*;

    #[test]
    fn test_infer_literal_types() {
        let no_lookup = |_: &str| None;
        assert_eq!(
            infer_expression_type(
                &CompiledExpression::Literal(Value::String("hello".to_string())),
                &no_lookup
            ),
            Some(Type::Text { charset: None })
        );
        assert_eq!(
            infer_expression_type(&CompiledExpression::Literal(Value::Int(42)), &no_lookup),
            Some(Type::Int {
                bits: IntSize::I64,
                unsigned: false,
                auto_increment: false
            })
        );
        assert_eq!(
            infer_expression_type(
                &CompiledExpression::Literal(Value::Float(f64::consts::PI)),
                &no_lookup
            ),
            Some(Type::Float {
                bits: FloatSize::F64
            })
        );
        assert_eq!(
            infer_expression_type(
                &CompiledExpression::Literal(Value::Boolean(true)),
                &no_lookup
            ),
            Some(Type::Boolean)
        );
    }

    #[test]
    fn test_infer_identifier() {
        let column_lookup = |name: &str| {
            if name == "age" {
                Some(Type::Int {
                    bits: IntSize::I64,
                    unsigned: false,
                    auto_increment: false,
                })
            } else if name == "name" {
                Some(Type::Text { charset: None })
            } else {
                None
            }
        };

        assert_eq!(
            infer_expression_type(
                &CompiledExpression::Identifier("age".to_string()),
                &column_lookup
            ),
            Some(Type::Int {
                bits: IntSize::I64,
                unsigned: false,
                auto_increment: false,
            })
        );
        assert_eq!(
            infer_expression_type(
                &CompiledExpression::Identifier("name".to_string()),
                &column_lookup
            ),
            Some(Type::Text { charset: None })
        );
    }

    #[test]
    fn test_infer_function_call() {
        let no_lookup = |_: &str| None;
        let expr = CompiledExpression::FunctionCall {
            name: "upper".to_string(),
            args: vec![],
        };
        assert_eq!(
            infer_expression_type(&expr, &no_lookup),
            Some(Type::Varchar {
                length: None,
                charset: None
            })
        );
    }

    #[test]
    fn test_infer_is_null() {
        let no_lookup = |_: &str| None;
        let expr = CompiledExpression::IsNull(Box::new(CompiledExpression::Literal(Value::Null)));
        assert_eq!(
            infer_expression_type(&expr, &no_lookup),
            Some(Type::Boolean)
        );
    }
}
