use sql_adapter::filter::SqlFilter;

#[derive(Debug, Clone)]
pub enum Filter {
    Sql(SqlFilter),
}
