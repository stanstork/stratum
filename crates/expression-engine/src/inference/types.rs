use model::{
    core::{data_type::DataType, value::Value},
    execution::expr::CompiledExpression,
};
use tracing::warn;

/// Infer the data type of a compiled expression
/// For cross-entity references (DotPath with 2+ segments), returns None and
/// the caller should handle fetching metadata asynchronously
pub fn infer_expression_type<F>(expr: &CompiledExpression, column_lookup: &F) -> Option<DataType>
where
    F: Fn(&str) -> Option<DataType>,
{
    match expr {
        CompiledExpression::Identifier(identifier) => column_lookup(identifier),

        CompiledExpression::Literal(value) => Some(match value {
            Value::String(_) => DataType::String,
            Value::Int(_) => DataType::Int,
            Value::Float(_) => DataType::Float,
            Value::Boolean(_) => DataType::Boolean,
            Value::Null => DataType::String, // Default for null
            _ => DataType::String,           // Fallback for other types
        }),

        CompiledExpression::Binary { left, right, .. } => {
            let lt = infer_expression_type(left, column_lookup)?;
            let rt = infer_expression_type(right, column_lookup)?;
            Some(get_numeric_type(&lt, &rt))
        }

        CompiledExpression::FunctionCall { name, .. } => match name.to_ascii_lowercase().as_str() {
            "lower" | "upper" | "concat" | "env" => Some(DataType::VarChar),
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

        CompiledExpression::IsNull(_) | CompiledExpression::IsNotNull(_) => Some(DataType::Boolean),

        CompiledExpression::Array(_) => None, // Arrays not yet supported
        CompiledExpression::DotPath(_) => None, // Empty DotPath
    }
}

fn get_numeric_type(left: &DataType, right: &DataType) -> DataType {
    match (left, right) {
        (DataType::Int, DataType::Int) => DataType::Int,
        (DataType::Float, DataType::Float) => DataType::Float,
        (DataType::Int, DataType::Float) => DataType::Float,
        (DataType::Float, DataType::Int) => DataType::Float,
        (DataType::Decimal, DataType::Decimal) => DataType::Decimal,
        (DataType::Int, DataType::Decimal) => DataType::Decimal,
        (DataType::Decimal, DataType::Int) => DataType::Decimal,
        (DataType::Float, DataType::Decimal) => DataType::Decimal,
        (DataType::Decimal, DataType::Float) => DataType::Decimal,
        _ => {
            warn!(
                "Incompatible types for arithmetic operation: {:?} and {:?}",
                left, right
            );
            DataType::String // Fallback to String for unsupported types
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_literal_types() {
        let no_lookup = |_: &str| None;
        assert_eq!(
            infer_expression_type(
                &CompiledExpression::Literal(Value::String("hello".to_string())),
                &no_lookup
            ),
            Some(DataType::String)
        );
        assert_eq!(
            infer_expression_type(&CompiledExpression::Literal(Value::Int(42)), &no_lookup),
            Some(DataType::Int)
        );
        assert_eq!(
            infer_expression_type(&CompiledExpression::Literal(Value::Float(3.14)), &no_lookup),
            Some(DataType::Float)
        );
        assert_eq!(
            infer_expression_type(
                &CompiledExpression::Literal(Value::Boolean(true)),
                &no_lookup
            ),
            Some(DataType::Boolean)
        );
    }

    #[test]
    fn test_infer_identifier() {
        let column_lookup = |name: &str| {
            if name == "age" {
                Some(DataType::Int)
            } else if name == "name" {
                Some(DataType::String)
            } else {
                None
            }
        };

        assert_eq!(
            infer_expression_type(
                &CompiledExpression::Identifier("age".to_string()),
                &column_lookup
            ),
            Some(DataType::Int)
        );
        assert_eq!(
            infer_expression_type(
                &CompiledExpression::Identifier("name".to_string()),
                &column_lookup
            ),
            Some(DataType::String)
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
            Some(DataType::VarChar)
        );
    }

    #[test]
    fn test_infer_is_null() {
        let no_lookup = |_: &str| None;
        let expr = CompiledExpression::IsNull(Box::new(CompiledExpression::Literal(Value::Null)));
        assert_eq!(
            infer_expression_type(&expr, &no_lookup),
            Some(DataType::Boolean)
        );
    }
}
