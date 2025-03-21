use super::{functions::uppercase::UpperCaseFunction, mapping::TransformMapping};
use sql_adapter::row::row_data::RowData;

pub trait Transform {
    fn apply(&self, row: &RowData) -> RowData;
}

pub struct TransformPipeline {
    transforms: Vec<Box<dyn Transform>>,
}

impl TransformPipeline {
    pub fn new() -> Self {
        Self {
            transforms: Vec::new(),
        }
    }

    pub fn from_mapping(mappings: Vec<TransformMapping>) -> Self {
        let mut pipeline = Self::new();
        for mapping in mappings {
            match mapping {
                TransformMapping::Function { function, args } => match function.as_str() {
                    "uppercase" => pipeline.add_transform(UpperCaseFunction::new(args)),
                    _ => {}
                },
            }
        }
        pipeline
    }

    pub fn apply(&self, row: &RowData) -> RowData {
        let mut row = (*row).clone();
        for transform in &self.transforms {
            row = transform.apply(&row);
        }
        row
    }

    fn add_transform<T: Transform + 'static>(&mut self, transform: T) {
        self.transforms.push(Box::new(transform));
    }
}
