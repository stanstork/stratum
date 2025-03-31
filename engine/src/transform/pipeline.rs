use crate::record::Record;

pub trait Transform: Send + Sync {
    fn apply(&self, record: &Record) -> Record;
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

    pub fn apply(&self, record: &Record) -> Record {
        self.transforms
            .iter()
            .fold(record.clone(), |acc, transform| transform.apply(&acc))
    }

    pub fn add_transform<T: Transform + 'static>(mut self, transform: T) -> Self {
        self.transforms.push(Box::new(transform));
        self
    }
}
