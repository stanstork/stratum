//! Provides a fluent builder for constructing `CreateIndex` ASTs.

use crate::ast::{
    common::TableRef,
    create_index::{CreateIndex, IndexColumnExpr},
};

pub struct CreateIndexBuilder {
    ast: CreateIndex,
}

impl CreateIndexBuilder {
    pub fn new(name: impl Into<String>, table: TableRef) -> Self {
        Self {
            ast: CreateIndex {
                name: name.into(),
                table,
                columns: Vec::new(),
                unique: false,
                if_not_exists: false,
                concurrent: false,
                index_type: None,
                condition: None,
            },
        }
    }

    pub fn column(mut self, name: impl Into<String>) -> Self {
        self.ast.columns.push(IndexColumnExpr {
            expr: name.into(),
            sort_order: None,
            nulls: None,
        });
        self
    }

    pub fn column_with_order(
        mut self,
        name: impl Into<String>,
        sort_order: Option<String>,
        nulls: Option<String>,
    ) -> Self {
        self.ast.columns.push(IndexColumnExpr {
            expr: name.into(),
            sort_order,
            nulls,
        });
        self
    }

    pub fn columns(mut self, columns: Vec<IndexColumnExpr>) -> Self {
        self.ast.columns = columns;
        self
    }

    pub fn unique(mut self) -> Self {
        self.ast.unique = true;
        self
    }

    pub fn if_not_exists(mut self) -> Self {
        self.ast.if_not_exists = true;
        self
    }

    pub fn concurrent(mut self) -> Self {
        self.ast.concurrent = true;
        self
    }

    pub fn using(mut self, method: impl Into<String>) -> Self {
        self.ast.index_type = Some(method.into());
        self
    }

    pub fn condition(mut self, where_clause: impl Into<String>) -> Self {
        self.ast.condition = Some(where_clause.into());
        self
    }

    pub fn build(self) -> CreateIndex {
        self.ast
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_ref;

    #[test]
    fn test_build_simple_index() {
        let idx = CreateIndexBuilder::new("idx_users_email", table_ref!("users"))
            .column("email")
            .build();

        assert_eq!(idx.name, "idx_users_email");
        assert_eq!(idx.table.name, "users");
        assert_eq!(idx.columns.len(), 1);
        assert!(!idx.unique);
    }

    #[test]
    fn test_build_unique_index_with_condition() {
        let idx = CreateIndexBuilder::new("idx_users_active_email", table_ref!("users"))
            .column("email")
            .unique()
            .if_not_exists()
            .condition("active = true")
            .build();

        assert!(idx.unique);
        assert!(idx.if_not_exists);
        assert_eq!(idx.condition, Some("active = true".to_string()));
    }
}
