use super::pipeline::Transform;
use crate::error::TransformError;
use model::{
    records::row::RowData,
    transform::mapping::{FieldTransformations, NameResolver},
};

pub struct FieldMapper {
    ns_map: FieldTransformations,
}

pub struct TableMapper {
    name_map: NameResolver,
}

impl FieldMapper {
    pub fn new(ns_map: FieldTransformations) -> Self {
        Self { ns_map }
    }
}

impl TableMapper {
    pub fn new(name_map: NameResolver) -> Self {
        Self { name_map }
    }
}

impl Transform for FieldMapper {
    fn apply(&self, row: &mut RowData) -> Result<(), TransformError> {
        let table = row.entity.clone();
        for column in &mut row.field_values {
            column.name = self.ns_map.resolve(&table, &column.name);
        }
        Ok(())
    }
}

impl Transform for TableMapper {
    fn apply(&self, row: &mut RowData) -> Result<(), TransformError> {
        let original_entity = row.entity.clone();
        row.entity = self.name_map.resolve(&original_entity);
        Ok(())
    }
}
