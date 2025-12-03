use connectors::{file::csv::filter::CsvFilter, sql::base::filter::SqlFilter};

#[derive(Debug, Clone)]
pub enum Filter {
    Sql(SqlFilter),
    Csv(CsvFilter),
}
