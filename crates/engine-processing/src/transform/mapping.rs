use super::pipeline::Transform;
use model::{
    records::record::Record,
    transform::mapping::{FieldMappings, NameMap},
};

pub struct FieldMapper {
    ns_map: FieldMappings,
}

pub struct TableMapper {
    name_map: NameMap,
}

impl FieldMapper {
    pub fn new(ns_map: FieldMappings) -> Self {
        Self { ns_map }
    }
}

impl TableMapper {
    pub fn new(name_map: NameMap) -> Self {
        Self { name_map }
    }
}

impl Transform for FieldMapper {
    fn apply(&self, record: &Record) -> Record {
        match record {
            Record::RowData(row) => {
                let mut new_row = row.clone();
                let table = new_row.entity.clone();
                for column in &mut new_row.field_values {
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
                new_row.entity = self.name_map.resolve(&row.entity);
                Record::RowData(new_row)
            }
        }
    }
}
