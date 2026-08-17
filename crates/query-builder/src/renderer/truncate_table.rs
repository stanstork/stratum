use crate::{
    ast::truncate_table::TruncateTable,
    renderer::{Render, Renderer},
};

impl Render for TruncateTable {
    fn render(&self, r: &mut Renderer) {
        r.sql.push_str("TRUNCATE TABLE ");
        r.render_table_ref(&self.table);
        r.sql.push(';');
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{common::TableRef, truncate_table::TruncateTable},
        dialect::{MySql, Postgres, QueryDialect},
        renderer::{Render, Renderer},
    };

    fn render(dialect: &dyn QueryDialect, name: &str) -> String {
        let mut r = Renderer::new(dialect);
        TruncateTable {
            table: TableRef {
                schema: None,
                name: name.to_string(),
            },
        }
        .render(&mut r);
        r.finish().0
    }

    #[test]
    fn renders_truncate_per_dialect() {
        assert_eq!(render(&Postgres, "orders"), "TRUNCATE TABLE \"orders\";");
        assert_eq!(render(&MySql, "orders"), "TRUNCATE TABLE `orders`;");
    }
}
