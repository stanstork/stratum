use super::pipeline::Transform;
use crate::record::Record;
use common::name_map::NameMap;

pub struct ColumnMapper {
    name_map: NameMap,
}

impl ColumnMapper {
    pub fn new(name_map: NameMap) -> Self {
        Self { name_map }
    }
}

impl Transform for ColumnMapper {
    fn apply(&self, record: &Record) -> Record {
        match record {
            Record::RowData(row) => {
                let mut new_row = row.clone();
                for column in &mut new_row.columns {
                    column.name = self.name_map.resolve(&column.name);
                }
                Record::RowData(new_row)
            }
        }
    }
}
