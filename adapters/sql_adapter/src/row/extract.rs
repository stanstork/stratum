use super::row::RowData;
use crate::metadata::column::{
    data_type::ColumnDataType,
    value::{ColumnData, ColumnValue},
};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{mysql::MySqlRow, Column, Row, TypeInfo};

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
                ColumnDataType::try_from(column.type_info().name()).unwrap_or_else(|_| {
                    eprintln!("Unknown column type: {}", column.type_info().name());
                    ColumnDataType::String
                });

            let value = match column_type {
                ColumnDataType::Int24 | ColumnDataType::Long => row
                    .try_get::<i32, _>(column.ordinal())
                    .ok()
                    .map(|v| ColumnValue::Int(v as i64)),
                ColumnDataType::Float => row
                    .try_get::<f64, _>(column.ordinal())
                    .ok()
                    .map(ColumnValue::Float),
                ColumnDataType::Decimal => row
                    .try_get::<BigDecimal, _>(column.ordinal())
                    .ok()
                    .and_then(|v| v.to_f64().map(ColumnValue::Float)),
                ColumnDataType::String | ColumnDataType::VarChar => row
                    .try_get::<String, _>(column.ordinal())
                    .ok()
                    .map(ColumnValue::String),
                ColumnDataType::Json => row
                    .try_get::<Value, _>(column.ordinal())
                    .ok()
                    .map(ColumnValue::Json),
                ColumnDataType::Timestamp => row
                    .try_get::<DateTime<Utc>, _>(column.ordinal())
                    .ok()
                    .map(ColumnValue::Timestamp),
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
