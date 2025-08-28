#[macro_export]
macro_rules! value {
    ($val:expr) => {
        $crate::ast::expr::Expr::Value($val)
    };
}

#[macro_export]
macro_rules! table_ref {
    ($name:expr) => {
        $crate::ast::common::TableRef {
            schema: None,
            name: $name.to_string(),
        }
    };
    ($schema:expr, $name:expr) => {
        $crate::ast::common::TableRef {
            schema: Some($schema.to_string()),
            name: $name.to_string(),
        }
    };
}

#[macro_export]
macro_rules! ident {
    ($name:expr) => {
        $crate::ast::expr::Expr::Identifier($crate::ast::expr::Ident {
            qualifier: None,
            name: $name.to_string(),
        })
    };
    ($qualifier:expr, $name:expr) => {
        $crate::ast::expr::Expr::Identifier($crate::ast::expr::Ident {
            qualifier: $qualifier,
            name: $name.to_string(),
        })
    };
}

/// Creates an aliased identifier expression.
#[macro_export]
macro_rules! ident_as {
    // Arm for simple identifier: ident_as!("id", "user_id")
    ($name:expr, $alias:expr) => {
        $crate::ast::expr::Expr::Alias {
            expr: Box::new($crate::ast::expr::Expr::Identifier(
                $crate::ast::expr::Ident {
                    qualifier: None,
                    name: $name.to_string(),
                },
            )),
            alias: $alias.to_string(),
        }
    };
    // Arm for qualified identifier: ident_as!("u", "id", "user_id")
    ($qualifier:expr, $name:expr, $alias:expr) => {
        $crate::ast::expr::Expr::Alias {
            expr: Box::new($crate::ast::expr::Expr::Identifier(
                $crate::ast::expr::Ident {
                    qualifier: Some($qualifier.to_string()),
                    name: $name.to_string(),
                },
            )),
            alias: $alias.to_string(),
        }
    };
}
