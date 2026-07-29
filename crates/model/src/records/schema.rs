use crate::core::types::Type;
use std::collections::HashMap;
use std::sync::Arc;

/// One column's metadata, stored once per schema (not per row).
#[derive(Debug, Clone)]
pub struct SchemaColumn {
    pub name: Arc<str>,
    pub data_type: Type,
}

impl SchemaColumn {
    pub fn new(name: impl Into<Arc<str>>, data_type: Type) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}

/// Column layout shared by every row of a result set via `Arc`.
#[derive(Debug)]
pub struct RecordSchema {
    table: Arc<str>,
    columns: Vec<SchemaColumn>,
    /// Lower-cased column name -> position.
    index: HashMap<String, usize>,
}

impl RecordSchema {
    pub fn new(table: impl Into<Arc<str>>, columns: Vec<SchemaColumn>) -> Arc<Self> {
        let index = build_index(&columns);
        Arc::new(Self {
            table: table.into(),
            columns,
            index,
        })
    }

    /// A schema with no columns for `table` (used before columns are known).
    pub fn empty(table: impl Into<Arc<str>>) -> Arc<Self> {
        Arc::new(Self {
            table: table.into(),
            columns: Vec::new(),
            index: HashMap::new(),
        })
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn table_arc(&self) -> Arc<str> {
        Arc::clone(&self.table)
    }

    pub fn columns(&self) -> &[SchemaColumn] {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn column(&self, i: usize) -> Option<&SchemaColumn> {
        self.columns.get(i)
    }

    /// Position of `name` (case-insensitive), or `None` if absent.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        if name.bytes().any(|b| b.is_ascii_uppercase()) {
            self.index.get(&name.to_ascii_lowercase()).copied()
        } else {
            self.index.get(name).copied()
        }
    }

    /// Produce a schema with each column name remapped by `f`.
    pub fn remapped<F>(self: &Arc<Self>, mut f: F) -> Arc<Self>
    where
        F: FnMut(&str) -> Option<Arc<str>>,
    {
        let mut changed = false;

        let columns: Vec<SchemaColumn> = self
            .columns
            .iter()
            .map(|c| match f(&c.name) {
                Some(new_name) if !new_name.eq_ignore_ascii_case(&c.name) => {
                    changed = true;
                    SchemaColumn {
                        name: new_name,
                        data_type: c.data_type.clone(),
                    }
                }
                _ => c.clone(),
            })
            .collect();

        if !changed {
            return Arc::clone(self);
        }

        RecordSchema::new(Arc::clone(&self.table), columns)
    }

    /// The same columns under a different table name.
    pub fn with_table(&self, table: impl Into<Arc<str>>) -> Arc<Self> {
        Arc::new(Self {
            table: table.into(),
            columns: self.columns.clone(),
            index: self.index.clone(),
        })
    }

    /// A schema with `col` appended (for computed columns).
    pub fn with_appended(&self, col: SchemaColumn) -> Arc<Self> {
        let mut columns = self.columns.clone();
        let mut index = self.index.clone();

        index.insert(col.name.to_ascii_lowercase(), columns.len());
        columns.push(col);

        Arc::new(Self {
            table: Arc::clone(&self.table),
            columns,
            index,
        })
    }
}

fn build_index(columns: &[SchemaColumn]) -> HashMap<String, usize> {
    let mut index = HashMap::with_capacity(columns.len());

    for (i, c) in columns.iter().enumerate() {
        index.insert(c.name.to_ascii_lowercase(), i);
    }

    index
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ty() -> Type {
        Type::Text { charset: None }
    }

    fn schema() -> Arc<RecordSchema> {
        RecordSchema::new(
            "t",
            vec![
                SchemaColumn::new("id", ty()),
                SchemaColumn::new("Name", ty()),
            ],
        )
    }

    #[test]
    fn index_is_case_insensitive() {
        let s = schema();
        assert_eq!(s.index_of("id"), Some(0));
        assert_eq!(s.index_of("ID"), Some(0));
        assert_eq!(s.index_of("name"), Some(1));
        assert_eq!(s.index_of("NAME"), Some(1));
        assert_eq!(s.index_of("missing"), None);
    }

    #[test]
    fn identity_remap_returns_same_arc() {
        let s = schema();
        let same = s.remapped(|_| None);
        assert!(Arc::ptr_eq(&s, &same), "identity remap must not reallocate");
    }

    #[test]
    fn remap_renames_and_reindexes() {
        let s = schema();
        let renamed = s.remapped(|n| (n == "id").then(|| Arc::from("user_id")));
        assert!(!Arc::ptr_eq(&s, &renamed));
        assert_eq!(renamed.index_of("user_id"), Some(0));
        assert_eq!(renamed.index_of("id"), None);
        assert_eq!(renamed.index_of("name"), Some(1));
    }

    #[test]
    fn append_adds_column_at_end() {
        let s = schema();
        let ext = s.with_appended(SchemaColumn::new("label", ty()));
        assert_eq!(ext.len(), 3);
        assert_eq!(ext.index_of("label"), Some(2));
        assert_eq!(ext.index_of("id"), Some(0));
    }
}
