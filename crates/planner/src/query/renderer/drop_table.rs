use crate::query::{
    ast::drop_table::DropTable,
    renderer::{Render, Renderer},
};

impl Render for DropTable {
    fn render(&self, r: &mut Renderer) {
        r.sql.push_str("DROP TABLE ");
        if self.if_exists {
            r.sql.push_str("IF EXISTS ");
        }
        r.render_table_ref(&self.table);
        r.sql.push(';');
    }
}

#[cfg(test)]
mod tests {
    use crate::query::{
        ast::{common::TableRef, drop_table::DropTable},
        dialect::Postgres,
        renderer::{Render, Renderer},
    };

    #[test]
    fn test_render_drop_table() {
        let ast = DropTable {
            table: TableRef {
                schema: None,
                name: "users".to_string(),
            },
            if_exists: true,
        };

        let mut renderer = Renderer::new(&Postgres);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        assert!(params.is_empty());
        assert_eq!(sql, r#"DROP TABLE IF EXISTS "users";"#);
    }
}
