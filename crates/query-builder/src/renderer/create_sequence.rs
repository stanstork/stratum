use crate::{
    ast::create_sequence::CreateSequence,
    renderer::{Render, Renderer},
};

impl Render for CreateSequence {
    fn render(&self, r: &mut Renderer) {
        r.sql.push_str("CREATE SEQUENCE ");
        if self.if_not_exists {
            r.sql.push_str("IF NOT EXISTS ");
        }
        r.sql.push_str(&r.dialect.quote_identifier(&self.name));

        if let Some(inc) = self.increment {
            r.sql.push_str(&format!(" INCREMENT BY {}", inc));
        }
        if let Some(min) = self.min_value {
            r.sql.push_str(&format!(" MINVALUE {}", min));
        }
        if let Some(max) = self.max_value {
            r.sql.push_str(&format!(" MAXVALUE {}", max));
        }
        if let Some(start) = self.start {
            r.sql.push_str(&format!(" START WITH {}", start));
        }
        if let Some((table, column)) = &self.owned_by {
            r.sql.push_str(" OWNED BY ");
            r.sql.push_str(&r.dialect.quote_identifier(table));
            r.sql.push('.');
            r.sql.push_str(&r.dialect.quote_identifier(column));
        }

        r.sql.push(';');
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::create_sequence::CreateSequence,
        dialect::Postgres,
        renderer::{Render, Renderer},
    };

    #[test]
    fn test_render_simple_sequence() {
        let ast = CreateSequence {
            name: "users_id_seq".to_string(),
            if_not_exists: true,
            start: Some(1),
            increment: Some(1),
            min_value: None,
            max_value: None,
            owned_by: None,
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, _) = renderer.finish();

        assert_eq!(
            sql,
            r#"CREATE SEQUENCE IF NOT EXISTS "users_id_seq" INCREMENT BY 1 START WITH 1;"#
        );
    }

    #[test]
    fn test_render_full_sequence() {
        let ast = CreateSequence {
            name: "orders_id_seq".to_string(),
            if_not_exists: false,
            start: Some(1000),
            increment: Some(1),
            min_value: Some(1),
            max_value: Some(999999999),
            owned_by: Some(("orders".to_string(), "id".to_string())),
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, _) = renderer.finish();

        assert_eq!(
            sql,
            r#"CREATE SEQUENCE "orders_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 999999999 START WITH 1000 OWNED BY "orders"."id";"#
        );
    }
}
