use super::column::{ColumnType, ColumnValue};
use bigdecimal::{BigDecimal, ToPrimitive};
use serde_json::{Number, Value};
use sqlx::{mysql::MySqlRow, Column, Row, TypeInfo};

#[derive(Debug)]
pub struct RowData {
    pub columns: Vec<ColumnValue>,
}

pub trait RowDataExt {
    type Row: sqlx::Row;

    fn from_row(row: &Self::Row) -> RowData;
}

pub struct MySqlRowDataExt;
pub struct PostgresRowDataExt;

impl RowDataExt for MySqlRowDataExt {
    type Row = MySqlRow;

    fn from_row(row: &Self::Row) -> RowData {
        let mut columns = Vec::new();

        for column in row.columns().iter() {
            let name = column.name().to_string();
            let column_type = ColumnType::from(column.type_info().name());

            let value = match column_type {
                ColumnType::Int24 | ColumnType::Long => row
                    .try_get::<i32, _>(column.ordinal())
                    .map(|v| Value::Number(Number::from(v)))
                    .unwrap_or(Value::Null),
                ColumnType::Float => row
                    .try_get::<f64, _>(column.ordinal())
                    .map(|v| Value::Number(Number::from_f64(v).unwrap()))
                    .unwrap_or(Value::Null),
                ColumnType::Decimal => row
                    .try_get::<BigDecimal, _>(column.ordinal())
                    .map(|v| Value::Number(Number::from_f64(v.to_f64().unwrap()).unwrap()))
                    .unwrap_or(Value::Null),
                ColumnType::String | ColumnType::VarChar => row
                    .try_get::<String, _>(column.ordinal())
                    .map(Value::String)
                    .unwrap_or(Value::Null),
                ColumnType::Json => row
                    .try_get::<Value, _>(column.ordinal())
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            };

            columns.push(ColumnValue {
                name,
                value,
                type_info: column_type,
            });
        }

        RowData { columns }
    }
}
