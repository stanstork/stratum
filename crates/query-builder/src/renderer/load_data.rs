use crate::{
    ast::load_data::{LoadData, LoadDataConflict},
    renderer::Render,
};

impl Render for LoadData {
    fn render(&self, r: &mut super::Renderer) {
        r.sql.push_str("LOAD DATA ");
        if self.local {
            r.sql.push_str("LOCAL ");
        }
        r.sql.push_str("INFILE '");
        r.sql.push_str(&self.file_name);
        r.sql.push('\'');

        // Conflict modifier goes between the file name and INTO TABLE.
        match self.on_conflict {
            LoadDataConflict::Default => {}
            LoadDataConflict::Replace => r.sql.push_str(" REPLACE"),
            LoadDataConflict::Ignore => r.sql.push_str(" IGNORE"),
        }

        r.sql.push_str(" INTO TABLE ");
        r.render_table_ref(&self.table);

        r.sql.push_str(" FIELDS TERMINATED BY '");
        r.sql.push_str(&self.fields_terminated_by);
        r.sql.push_str("' ESCAPED BY '");
        r.sql.push_str(&self.fields_escaped_by);
        r.sql.push_str("' LINES TERMINATED BY '");
        r.sql.push_str(&self.lines_terminated_by);
        r.sql.push('\'');

        if !self.columns.is_empty() {
            let cols: Vec<String> = self
                .columns
                .iter()
                .map(|c| r.dialect.quote_identifier(c))
                .collect();
            r.sql.push_str(" (");
            r.sql.push_str(&cols.join(", "));
            r.sql.push(')');
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::common::TableRef,
        builder::load_data::LoadDataBuilder,
        dialect::MySql,
        renderer::{Render, Renderer},
    };

    fn render(load: crate::ast::load_data::LoadData) -> (String, Vec<model::core::value::Value>) {
        let dialect = MySql;
        let mut renderer = Renderer::new(&dialect);
        load.render(&mut renderer);
        renderer.finish()
    }

    #[test]
    fn test_render_local_infile_with_columns() {
        let load = LoadDataBuilder::new(TableRef {
            schema: None,
            name: "users".to_string(),
        })
        .columns(&["id", "name"])
        .build();

        let (sql, params) = render(load);

        assert_eq!(
            sql,
            r"LOAD DATA LOCAL INFILE 'stratum' INTO TABLE `users` FIELDS TERMINATED BY '\t' ESCAPED BY '\\' LINES TERMINATED BY '\n' (`id`, `name`)"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_render_without_columns_omits_column_list() {
        let load = LoadDataBuilder::new(TableRef {
            schema: None,
            name: "users".to_string(),
        })
        .build();

        let (sql, _) = render(load);

        assert_eq!(
            sql,
            r"LOAD DATA LOCAL INFILE 'stratum' INTO TABLE `users` FIELDS TERMINATED BY '\t' ESCAPED BY '\\' LINES TERMINATED BY '\n'"
        );
        assert!(!sql.contains('('));
    }

    #[test]
    fn test_render_qualified_table_quotes_schema() {
        let load = LoadDataBuilder::new(TableRef {
            schema: Some("sakila".to_string()),
            name: "actor".to_string(),
        })
        .columns(&["actor_id"])
        .build();

        let (sql, _) = render(load);

        assert!(sql.contains("INTO TABLE `sakila`.`actor`"));
    }

    #[test]
    fn test_render_replace_modifier() {
        use crate::ast::load_data::LoadDataConflict;

        let load = LoadDataBuilder::new(TableRef {
            schema: None,
            name: "users".to_string(),
        })
        .on_conflict(LoadDataConflict::Replace)
        .columns(&["id"])
        .build();

        let (sql, _) = render(load);

        assert!(
            sql.starts_with("LOAD DATA LOCAL INFILE 'stratum' REPLACE INTO TABLE `users`"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_render_ignore_modifier() {
        use crate::ast::load_data::LoadDataConflict;

        let load = LoadDataBuilder::new(TableRef {
            schema: None,
            name: "users".to_string(),
        })
        .on_conflict(LoadDataConflict::Ignore)
        .build();

        let (sql, _) = render(load);

        assert!(
            sql.starts_with("LOAD DATA LOCAL INFILE 'stratum' IGNORE INTO TABLE `users`"),
            "got: {sql}"
        );
    }

    #[test]
    fn test_render_default_conflict_emits_no_modifier() {
        let load = LoadDataBuilder::new(TableRef {
            schema: None,
            name: "users".to_string(),
        })
        .build();

        let (sql, _) = render(load);

        assert!(!sql.contains("REPLACE"));
        assert!(!sql.contains("IGNORE"));
        assert!(sql.starts_with("LOAD DATA LOCAL INFILE 'stratum' INTO TABLE `users`"));
    }

    #[test]
    fn test_render_non_local_infile() {
        let mut load = LoadDataBuilder::new(TableRef {
            schema: None,
            name: "users".to_string(),
        })
        .build();
        load.local = false;

        let (sql, _) = render(load);

        assert!(sql.starts_with("LOAD DATA INFILE 'stratum'"));
        assert!(!sql.contains("LOCAL"));
    }
}
