#[macro_export]
macro_rules! value {
    ($val:expr) => {
        $crate::query::ast::expr::Expr::Value($val)
    };
}

#[macro_export]
macro_rules! table_ref {
    ($name:expr) => {
        $crate::query::ast::common::TableRef {
            schema: None,
            name: $name.to_string(),
        }
    };
    ($schema:expr, $name:expr) => {
        $crate::query::ast::data_model::TableRef {
            schema: Some($schema.to_string()),
            name: $name.to_string(),
        }
    };
}
