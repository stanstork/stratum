use crate::{
    ast::create_table::{ColumnDef, CreateTable, TableConstraint},
    render::{Render, Renderer},
};

impl Render for CreateTable {
    fn render(&self, r: &mut Renderer) {
        r.sql.push_str("CREATE TABLE ");
        if self.if_not_exists {
            r.sql.push_str("IF NOT EXISTS ");
        }
        r.sql
            .push_str(&r.dialect.quote_identifier(&self.table.name));
        r.sql.push_str(" (");

        let num_cols = self.columns.len();
        for (i, col) in self.columns.iter().enumerate() {
            r.sql.push_str("\n\t");
            col.render(r);
            if i < num_cols - 1 || !self.constraints.is_empty() {
                r.sql.push(',');
            }
        }

        for (i, constraint) in self.constraints.iter().enumerate() {
            r.sql.push_str("\n\t");
            constraint.render(r);
            if i < self.constraints.len() - 1 {
                r.sql.push(',');
            }
        }

        r.sql.push_str("\n);");
    }
}

impl Render for ColumnDef {
    fn render(&self, r: &mut Renderer) {
        // Name and Type
        r.sql.push_str(&r.dialect.quote_identifier(&self.name));
        r.sql.push(' ');
        r.sql.push_str(&r.dialect.render_data_type(&self.data_type));

        // Constraints
        if self.is_primary_key {
            r.sql.push_str(" PRIMARY KEY");
        }
        if !self.is_nullable {
            r.sql.push_str(" NOT NULL");
        }
        if let Some(default) = &self.default_value {
            r.sql.push_str(" DEFAULT ");
            default.render(r);
        }
    }
}

impl Render for TableConstraint {
    fn render(&self, r: &mut Renderer) {
        match self {
            TableConstraint::PrimaryKey { columns } => {
                r.sql.push_str("PRIMARY KEY (");
                let quoted: Vec<String> = columns
                    .iter()
                    .map(|c| r.dialect.quote_identifier(c))
                    .collect();
                r.sql.push_str(&quoted.join(", "));
                r.sql.push(')');
            }
            TableConstraint::ForeignKey { .. } => {
                // Implementation for foreign keys would go here
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use common::types::DataType;

    use crate::{
        ast::{
            common::TableRef,
            create_table::{ColumnDef, CreateTable, TableConstraint},
        },
        dialect::Postgres,
        render::{Render, Renderer},
    };

    #[test]
    fn test_render_create_table() {
        let ast = CreateTable {
            table: TableRef {
                schema: None,
                name: "users".to_string(),
            },
            if_not_exists: true,
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::IntUnsigned,
                    is_primary_key: true,
                    is_nullable: false,
                    default_value: None,
                },
                ColumnDef {
                    name: "email".to_string(),
                    data_type: DataType::VarChar,
                    is_primary_key: false,
                    is_nullable: false,
                    default_value: None,
                },
            ],
            constraints: vec![TableConstraint::PrimaryKey {
                columns: vec!["id".to_string()],
            }],
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, _) = renderer.finish();

        let expected_sql = r#"CREATE TABLE IF NOT EXISTS "users" (
	"id" SERIAL PRIMARY KEY NOT NULL,
	"email" VARCHAR(255) NOT NULL,
	PRIMARY KEY ("id")
);"#;
        assert_eq!(sql, expected_sql);
    }
}
