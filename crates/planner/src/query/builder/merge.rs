use crate::query::ast::{
    common::TableRef,
    expr::Expr,
    merge::{Merge, MergeAssignment, MergeMatched, MergeNotMatched},
};

#[derive(Debug, Clone)]
pub struct MergeBuilder {
    ast: Merge,
}

impl MergeBuilder {
    pub fn new(target: TableRef, source: TableRef) -> Self {
        Self {
            ast: Merge {
                target,
                target_alias: None,
                source,
                source_alias: None,
                on: Expr::Literal("TRUE".to_string()),
                when_matched: None,
                when_not_matched: None,
            },
        }
    }

    pub fn target_alias(mut self, alias: &str) -> Self {
        self.ast.target_alias = Some(alias.to_string());
        self
    }

    pub fn source_alias(mut self, alias: &str) -> Self {
        self.ast.source_alias = Some(alias.to_string());
        self
    }

    pub fn on(mut self, condition: Expr) -> Self {
        self.ast.on = condition;
        self
    }

    pub fn when_matched_update(mut self, assignments: Vec<MergeAssignment>) -> Self {
        self.ast.when_matched = Some(MergeMatched::Update { assignments });
        self
    }

    pub fn when_matched_do_nothing(mut self) -> Self {
        self.ast.when_matched = Some(MergeMatched::DoNothing);
        self
    }

    pub fn when_not_matched_insert(mut self, columns: Vec<String>, values: Vec<Expr>) -> Self {
        self.ast.when_not_matched = Some(MergeNotMatched { columns, values });
        self
    }

    pub fn build(self) -> Merge {
        self.ast
    }
}
