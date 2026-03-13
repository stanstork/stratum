use connectors::{drivers::csv::filter::CsvFilter, sql::filter::SqlFilter};

pub mod compiler;
pub mod utils;

#[derive(Debug, Clone)]
pub enum Filter {
    Sql(SqlFilter),
    Csv(CsvFilter),
}
