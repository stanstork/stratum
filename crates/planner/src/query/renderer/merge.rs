use crate::query::{
    ast::merge::{Merge, MergeAssignment, MergeMatched, MergeNotMatched},
    renderer::Render,
};

impl Render for Merge {
    fn render(&self, r: &mut super::Renderer) {
        r.sql.push_str("MERGE INTO ");
        r.render_table_ref(&self.target);
        if let Some(alias) = &self.target_alias {
            r.sql.push_str(" AS ");
            r.sql.push_str(&r.dialect.quote_identifier(alias));
        }

        r.sql.push_str(" USING ");
        r.render_table_ref(&self.source);
        if let Some(alias) = &self.source_alias {
            r.sql.push_str(" AS ");
            r.sql.push_str(&r.dialect.quote_identifier(alias));
        }

        r.sql.push_str(" ON ");
        self.on.render(r);

        if let Some(matched) = &self.when_matched {
            r.sql.push(' ');
            matched.render(r);
        }

        if let Some(not_matched) = &self.when_not_matched {
            r.sql.push(' ');
            not_matched.render(r);
        }

        r.sql.push(';');
    }
}

impl Render for MergeMatched {
    fn render(&self, r: &mut super::Renderer) {
        match self {
            MergeMatched::Update { assignments } => {
                r.sql.push_str("WHEN MATCHED THEN UPDATE SET ");
                for (i, assignment) in assignments.iter().enumerate() {
                    if i > 0 {
                        r.sql.push_str(", ");
                    }
                    assignment.render(r);
                }
            }
            MergeMatched::DoNothing => {
                r.sql.push_str("WHEN MATCHED THEN DO NOTHING");
            }
        }
    }
}

impl Render for MergeNotMatched {
    fn render(&self, r: &mut super::Renderer) {
        r.sql.push_str("WHEN NOT MATCHED THEN INSERT (");
        let quoted_cols: Vec<String> = self
            .columns
            .iter()
            .map(|c| r.dialect.quote_identifier(c))
            .collect();
        r.sql.push_str(&quoted_cols.join(", "));
        r.sql.push_str(") VALUES (");

        for (i, value) in self.values.iter().enumerate() {
            if i > 0 {
                r.sql.push_str(", ");
            }
            value.render(r);
        }
        r.sql.push(')');
    }
}

impl Render for MergeAssignment {
    fn render(&self, r: &mut super::Renderer) {
        r.sql.push_str(&r.dialect.quote_identifier(&self.column));
        r.sql.push_str(" = ");
        self.value.render(r);
    }
}

#[cfg(test)]
mod tests {
    use crate::query::{
        ast::{
            common::TableRef,
            expr::{BinaryOp, BinaryOperator, Expr, Ident},
            merge::{Merge, MergeAssignment, MergeMatched, MergeNotMatched},
        },
        dialect::Postgres,
        renderer::{Render, Renderer},
    };

    fn ident(alias: &str, column: &str) -> Expr {
        Expr::Identifier(Ident {
            qualifier: Some(alias.to_string()),
            name: column.to_string(),
        })
    }

    #[test]
    fn test_render_merge_with_update() {
        let ast = Merge {
            target: TableRef {
                schema: None,
                name: "users".to_string(),
            },
            target_alias: Some("t".to_string()),
            source: TableRef {
                schema: None,
                name: "users_stage".to_string(),
            },
            source_alias: Some("s".to_string()),
            on: Expr::BinaryOp(Box::new(BinaryOp {
                left: ident("t", "id"),
                op: BinaryOperator::Eq,
                right: ident("s", "id"),
            })),
            when_matched: Some(MergeMatched::Update {
                assignments: vec![MergeAssignment {
                    column: "name".to_string(),
                    value: ident("s", "name"),
                }],
            }),
            when_not_matched: Some(MergeNotMatched {
                columns: vec!["id".to_string(), "name".to_string()],
                values: vec![ident("s", "id"), ident("s", "name")],
            }),
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(
            sql,
            concat!(
                "MERGE INTO \"users\" AS \"t\" USING \"users_stage\" AS \"s\" ",
                "ON (\"t\".\"id\" = \"s\".\"id\") ",
                "WHEN MATCHED THEN UPDATE SET \"name\" = \"s\".\"name\" ",
                "WHEN NOT MATCHED THEN INSERT (\"id\", \"name\") VALUES (\"s\".\"id\", \"s\".\"name\");"
            )
        );
        assert!(params.is_empty());
    }
}
