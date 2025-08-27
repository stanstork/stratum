use crate::{
    ast::{
        alter_table::{AlterTable, AlterTableOperation},
        create_table::ColumnDef,
    },
    render::{Render, Renderer, create_table::render_data_type},
};

impl Render for AlterTable {
    fn render(&self, r: &mut super::Renderer) {
        // For simplicity and max compatibility, render each operation as a
        // separate ALTER TABLE statement. Some dialects support multiple
        // clauses in one statement, but this approach is more universal.
        for (i, op) in self.operations.iter().enumerate() {
            if i > 0 {
                r.sql.push_str(";\n");
            }
            r.sql.push_str("ALTER TABLE ");
            r.sql
                .push_str(&r.dialect.quote_identifier(&self.table.name));
            r.sql.push(' ');
            op.render(r);
        }
        r.sql.push(';');
    }
}

impl Render for AlterTableOperation {
    fn render(&self, r: &mut super::Renderer) {
        match self {
            AlterTableOperation::AddColumn(col_def) => {
                r.sql.push_str("ADD COLUMN ");
                render_add_column(col_def, r);
            }
            AlterTableOperation::AddConstraint(constraint) => {
                r.sql.push_str("ADD ");
                constraint.render(r);
            }
            AlterTableOperation::ToggleTriggers { enabled } => {
                let action = if *enabled { "ENABLE" } else { "DISABLE" };
                r.sql.push_str(action);
                r.sql.push_str(" TRIGGER ALL");
            }
        }
    }
}

fn render_add_column(col: &ColumnDef, r: &mut Renderer) {
    // Name and Type
    r.sql.push_str(&r.dialect.quote_identifier(&col.name));
    r.sql.push(' ');
    r.sql.push_str(&render_data_type(&col.data_type));

    // Constraints
    if !col.is_nullable {
        r.sql.push_str(" NOT NULL");
    }
    if let Some(default) = &col.default_value {
        r.sql.push_str(" DEFAULT ");
        default.render(r);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{
            alter_table::{AlterTable, AlterTableOperation},
            common::TableRef,
            create_table::{ColumnDef, DataType},
        },
        dialect::Postgres,
        render::{Render, Renderer},
    };

    #[test]
    fn test_render_alter_table_with_toggle_triggers() {
        let ast = AlterTable {
            table: TableRef {
                schema: None,
                name: "posts".to_string(),
            },
            operations: vec![
                AlterTableOperation::AddColumn(ColumnDef {
                    name: "category".to_string(),
                    data_type: DataType::Varchar(100),
                    is_nullable: true,
                    is_primary_key: false,
                    default_value: None,
                }),
                AlterTableOperation::ToggleTriggers { enabled: false },
                AlterTableOperation::ToggleTriggers { enabled: true },
            ],
        };

        let dialect = Postgres;
        let mut renderer = Renderer::new(&dialect);
        ast.render(&mut renderer);
        let (sql, _) = renderer.finish();

        let expected_sql = r#"ALTER TABLE "posts" ADD COLUMN "category" VARCHAR(100);
ALTER TABLE "posts" DISABLE TRIGGER ALL;
ALTER TABLE "posts" ENABLE TRIGGER ALL;"#;
        assert_eq!(sql, expected_sql);
    }
}
