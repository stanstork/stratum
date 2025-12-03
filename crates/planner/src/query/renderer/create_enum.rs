use crate::query::{
    ast::create_enum::CreateEnum,
    renderer::{Render, Renderer},
};

impl Render for CreateEnum {
    fn render(&self, r: &mut Renderer) {
        r.sql.push_str("CREATE TYPE ");
        r.sql.push_str(&r.dialect.quote_identifier(&self.name.name));
        r.sql.push_str(" AS ENUM (");

        let quoted_values: Vec<String> = self
            .values
            .iter()
            .map(|v| format!("'{}'", v.replace("'", "''"))) // Basic escaping
            .collect();

        r.sql.push_str(&quoted_values.join(", "));
        r.sql.push_str(");");
    }
}

#[cfg(test)]
mod tests {
    use crate::query::{
        ast::{common::TypeName, create_enum::CreateEnum},
        dialect::Postgres,
        renderer::{Render, Renderer},
    };

    #[test]
    fn test_render_create_enum() {
        let ast = CreateEnum {
            name: TypeName {
                schema: Some("public".to_string()),
                name: "order_status".to_string(),
            },
            values: vec![
                "pending".to_string(),
                "shipped".to_string(),
                "delivered".to_string(),
            ],
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, params) = renderer.finish();

        let expected_sql =
            r#"CREATE TYPE "order_status" AS ENUM ('pending', 'shipped', 'delivered');"#;
        assert_eq!(sql, expected_sql);
        assert!(params.is_empty());
    }
}
