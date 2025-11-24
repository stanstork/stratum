use crate::query::ast::copy::{Copy, CopyDirection, CopyEndpoint};
use crate::query::renderer::Render;

impl Render for Copy {
    fn render(&self, r: &mut super::Renderer) {
        r.sql.push_str("COPY ");
        r.render_table_ref(&self.table);

        if !self.columns.is_empty() {
            r.sql.push_str(" (");
            let cols: Vec<String> = self
                .columns
                .iter()
                .map(|col| r.dialect.quote_identifier(col))
                .collect();
            r.sql.push_str(&cols.join(", "));
            r.sql.push(')');
        }

        match self.direction {
            CopyDirection::From => r.sql.push_str(" FROM "),
            CopyDirection::To => r.sql.push_str(" TO "),
        }

        match &self.endpoint {
            CopyEndpoint::Stdin => r.sql.push_str("STDIN"),
            CopyEndpoint::Stdout => r.sql.push_str("STDOUT"),
            CopyEndpoint::File(path) => {
                r.sql.push('\'');
                r.sql.push_str(path);
                r.sql.push('\'');
            }
            CopyEndpoint::Program(cmd) => {
                r.sql.push_str("PROGRAM '");
                r.sql.push_str(cmd);
                r.sql.push('\'');
            }
        }

        if !self.options.is_empty() {
            r.sql.push_str(" WITH (");
            for (i, option) in self.options.iter().enumerate() {
                if i > 0 {
                    r.sql.push_str(", ");
                }
                r.sql.push_str(&option.key);
                if let Some(value) = &option.value {
                    r.sql.push(' ');
                    r.sql.push_str(value);
                }
            }
            r.sql.push(')');
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::query::{
        ast::{
            common::TableRef,
            copy::{CopyDirection, CopyEndpoint},
        },
        builder::copy::CopyBuilder,
        dialect::Postgres,
        renderer::{Render, Renderer},
    };

    #[test]
    fn test_render_copy_from_stdin() {
        let copy = CopyBuilder::new(TableRef {
            schema: Some("public".to_string()),
            name: "users".to_string(),
        })
        .columns(&["id", "name"])
        .direction(CopyDirection::From)
        .endpoint(CopyEndpoint::Stdin)
        .option("FORMAT", Some("TEXT"))
        .build();

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        copy.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert_eq!(
            sql,
            r#"COPY "public"."users" ("id", "name") FROM STDIN WITH (FORMAT TEXT)"#
        );
        assert!(params.is_empty());
    }
}
