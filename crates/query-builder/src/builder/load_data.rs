use crate::ast::{
    common::TableRef,
    load_data::{LoadData, LoadDataConflict},
};

#[derive(Debug, Clone)]
pub struct LoadDataBuilder {
    ast: LoadData,
}

impl LoadDataBuilder {
    pub fn new(table: TableRef) -> Self {
        Self {
            ast: LoadData {
                table,
                columns: Vec::new(),
                local: true,
                file_name: "paganel".to_string(),
                on_conflict: LoadDataConflict::Default,
                fields_terminated_by: r"\t".to_string(),
                fields_escaped_by: r"\\".to_string(),
                lines_terminated_by: r"\n".to_string(),
            },
        }
    }

    pub fn columns(mut self, columns: &[&str]) -> Self {
        self.ast.columns = columns.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn on_conflict(mut self, on_conflict: LoadDataConflict) -> Self {
        self.ast.on_conflict = on_conflict;
        self
    }

    pub fn build(self) -> LoadData {
        self.ast
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{common::TableRef, load_data::LoadDataConflict},
        builder::load_data::LoadDataBuilder,
    };

    fn table(name: &str) -> TableRef {
        TableRef {
            schema: None,
            name: name.to_string(),
        }
    }

    #[test]
    fn test_defaults() {
        let load = LoadDataBuilder::new(table("users")).build();

        assert_eq!(load.table.name, "users");
        assert!(load.table.schema.is_none());
        assert!(load.columns.is_empty());
        assert!(load.local);
        assert_eq!(load.file_name, "paganel");
        assert_eq!(load.on_conflict, LoadDataConflict::Default);
        assert_eq!(load.fields_terminated_by, r"\t");
        assert_eq!(load.fields_escaped_by, r"\\");
        assert_eq!(load.lines_terminated_by, r"\n");
    }

    #[test]
    fn test_on_conflict() {
        let load = LoadDataBuilder::new(table("users"))
            .on_conflict(LoadDataConflict::Replace)
            .build();

        assert_eq!(load.on_conflict, LoadDataConflict::Replace);
    }

    #[test]
    fn test_columns() {
        let load = LoadDataBuilder::new(table("users"))
            .columns(&["id", "name", "email"])
            .build();

        assert_eq!(load.columns, vec!["id", "name", "email"]);
    }

    #[test]
    fn test_columns_overwrites_previous() {
        let load = LoadDataBuilder::new(table("users"))
            .columns(&["id"])
            .columns(&["name"])
            .build();

        assert_eq!(load.columns, vec!["name"]);
    }
}
