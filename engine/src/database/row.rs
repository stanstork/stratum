use crate::metadata::column::{ColumnData, ColumnType, ColumnValue};
use bigdecimal::{BigDecimal, ToPrimitive};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{mysql::MySqlRow, Column, Row, TypeInfo};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowData {
    pub columns: Vec<ColumnData>,
}

pub trait RowDataExt {
    type Row: Row;

    fn from_row(row: &Self::Row) -> RowData;
}

pub struct MySqlRowDataExt;
pub struct PostgresRowDataExt;

impl RowDataExt for MySqlRowDataExt {
    type Row = MySqlRow;

    fn from_row(row: &Self::Row) -> RowData {
        let mut columns = Vec::new();

        for column in row.columns() {
            let name = column.name().to_string();
            let column_type =
                ColumnType::try_from(column.type_info().name()).unwrap_or_else(|_| {
                    eprintln!("Unknown column type: {}", column.type_info().name());
                    ColumnType::String
                });

            let value = match column_type {
                ColumnType::Int24 | ColumnType::Long => row
                    .try_get::<i32, _>(column.ordinal())
                    .ok()
                    .map(|v| ColumnValue::Int(v as i64)),
                ColumnType::Float => row
                    .try_get::<f64, _>(column.ordinal())
                    .ok()
                    .map(ColumnValue::Float),
                ColumnType::Decimal => row
                    .try_get::<BigDecimal, _>(column.ordinal())
                    .ok()
                    .and_then(|v| v.to_f64().map(ColumnValue::Float)),
                ColumnType::String | ColumnType::VarChar => row
                    .try_get::<String, _>(column.ordinal())
                    .ok()
                    .map(ColumnValue::String),
                ColumnType::Json => row
                    .try_get::<Value, _>(column.ordinal())
                    .ok()
                    .map(ColumnValue::Json),
                _ => None,
            };

            columns.push(ColumnData {
                name,
                value,
                type_info: column_type,
            });
        }

        RowData { columns }
    }
}
