use std::sync::Arc;

use model::records::record::Record;

pub trait Transform: Send + Sync {
    fn apply(&self, record: &Record) -> Record;
}

pub trait TransformPipelineExt {
    fn add_if<T, F>(self, condition: bool, factory: F) -> Self
    where
        T: Transform + 'static,
        F: FnOnce() -> T;
}

#[derive(Clone)]
pub struct TransformPipeline {
    transforms: Vec<Arc<dyn Transform>>,
}

impl TransformPipeline {
    pub fn new() -> Self {
        Self {
            transforms: Vec::new(),
        }
    }

    pub fn apply(&self, record: &Record) -> Record {
        self.transforms
            .iter()
            .fold(record.clone(), |acc, transform| transform.apply(&acc))
    }

    pub fn add_transform<T: Transform + 'static>(mut self, transform: T) -> Self {
        self.transforms.push(Arc::new(transform));
        self
    }
}

impl TransformPipelineExt for TransformPipeline {
    fn add_if<T, F>(mut self, condition: bool, factory: F) -> Self
    where
        T: Transform + 'static,
        F: FnOnce() -> T,
    {
        if condition {
            self = self.add_transform(factory());
        }
        self
    }
}

impl Default for TransformPipeline {
    fn default() -> Self {
        Self::new()
    }
}
