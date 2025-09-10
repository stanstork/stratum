use ::csv::filter::CsvFilter;
use smql::statements::expr::{Expression, Literal, Operator};
use sql_adapter::filter::SqlFilter;

pub mod compiler;
pub mod csv;
pub mod sql;

#[derive(Debug, Clone)]
pub enum Filter {
    Sql(SqlFilter),
    Csv(CsvFilter),
}
