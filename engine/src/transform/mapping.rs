use super::pipeline::Transform;
use crate::record::Record;
use common::mapping::{FieldNameMap, ScopedNameMap};

pub struct ColumnMapper {
    ns_map: ScopedNameMap,
}

pub struct TableMapper {
    name_map: FieldNameMap,
}

impl ColumnMapper {
    pub fn new(ns_map: ScopedNameMap) -> Self {
        Self { ns_map }
    }
}

impl TableMapper {
    pub fn new(name_map: FieldNameMap) -> Self {
        Self { name_map }
    }
}

impl Transform for ColumnMapper {
    fn apply(&self, record: &Record) -> Record {
        match record {
            Record::RowData(row) => {
                let mut new_row = row.clone();
                let table = new_row.table.clone();
                for column in &mut new_row.columns {
                    column.name = self.ns_map.resolve(&table, &column.name);
                }
                Record::RowData(new_row)
            }
        }
    }
}

impl Transform for TableMapper {
    fn apply(&self, record: &Record) -> Record {
        match record {
            Record::RowData(row) => {
                let mut new_row = row.clone();
                new_row.table = self.name_map.resolve(&row.table);
                Record::RowData(new_row)
            }
        }
    }
}
