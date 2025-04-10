use crate::metadata::column::{data_type::ColumnDataType, metadata::ColumnMetadata};
use smql::statements::expr::{Expression, Literal};

pub trait TypeInferencer {
    fn infer_type(&self, input_columns: &[ColumnMetadata]) -> Option<ColumnDataType>;
}

impl TypeInferencer for Expression {
    fn infer_type(&self, input_columns: &[ColumnMetadata]) -> Option<ColumnDataType> {
        match self {
            Expression::Identifier(identifier) => input_columns
                .iter()
                .find(|col| col.name.eq_ignore_ascii_case(identifier))
                .map(|col| col.data_type.clone()),

            Expression::Literal(literal) => Some(match literal {
                Literal::String(_) => ColumnDataType::String,
                Literal::Integer(_) => ColumnDataType::Int,
                Literal::Float(_) => ColumnDataType::Float,
                Literal::Boolean(_) => ColumnDataType::Boolean,
            }),

            Expression::Arithmetic { left, right, .. } => {
                let lt = left.infer_type(input_columns)?;
                let rt = right.infer_type(input_columns)?;
                Some(get_numeric_type(lt, rt))
            }

            Expression::FunctionCall { name, arguments: _ } => {
                match name.to_ascii_lowercase().as_str() {
                    "lower" | "upper" => Some(ColumnDataType::VarChar),
                    "concat" => Some(ColumnDataType::VarChar),
                    _ => None,
                }
            }

            _ => {
                eprintln!("Unsupported expression type for type inference: {:?}", self);
                None
            }
        }
    }
}

fn get_numeric_type(left: ColumnDataType, right: ColumnDataType) -> ColumnDataType {
    match (left, right) {
        (ColumnDataType::Int, ColumnDataType::Int) => ColumnDataType::Int,
        (ColumnDataType::Float, ColumnDataType::Float) => ColumnDataType::Float,
        (ColumnDataType::Int, ColumnDataType::Float) => ColumnDataType::Float,
        (ColumnDataType::Float, ColumnDataType::Int) => ColumnDataType::Float,
        (ColumnDataType::Decimal, ColumnDataType::Decimal) => ColumnDataType::Decimal,
        (ColumnDataType::Int, ColumnDataType::Decimal) => ColumnDataType::Decimal,
        (ColumnDataType::Decimal, ColumnDataType::Int) => ColumnDataType::Decimal,
        (ColumnDataType::Float, ColumnDataType::Decimal) => ColumnDataType::Decimal,
        (ColumnDataType::Decimal, ColumnDataType::Float) => ColumnDataType::Decimal,
        _ => {
            eprintln!(
                "Incompatible types for arithmetic operation: {:?} and {:?}",
                left, right
            );
            ColumnDataType::String // Fallback to String for unsupported types
        }
    }
}
