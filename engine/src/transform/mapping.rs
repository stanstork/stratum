use super::pipeline::Transform;
use crate::record::Record;
use common::mapping::NamespaceMap;

pub struct ColumnMapper {
    ns_map: NamespaceMap,
}

impl ColumnMapper {
    pub fn new(ns_map: NamespaceMap) -> Self {
        Self { ns_map }
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
