use super::pipeline::Transform;
use crate::transform::error::TransformError;
use model::{
    records::Record,
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
    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        let table = row.schema.clone();
        for column in &mut row.fields {
            column.name = self.ns_map.resolve(&table, &column.name);
        }
        Ok(())
    }
}

impl Transform for TableMapper {
    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        let original_schema = row.schema.clone();
        row.schema = self.name_map.resolve(&original_schema);
        Ok(())
    }
}
