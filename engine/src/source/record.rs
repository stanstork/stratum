use bincode;
use sql_adapter::row::row::RowData;

pub trait DataRecord: Send + Sync {
    fn debug(&self);
    fn as_any(&self) -> &dyn std::any::Any;
    fn serialize(&self) -> Vec<u8>;
}

impl DataRecord for RowData {
    fn debug(&self) {
        println!("{:#?}", self);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}
impl DataRecord for Box<dyn DataRecord> {
    fn debug(&self) {
        self.as_ref().debug()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.as_ref().as_any()
    }

    fn serialize(&self) -> Vec<u8> {
        self.as_ref().serialize()
    }
}

impl DataRecord for Box<dyn DataRecord + Send + Sync> {
    fn debug(&self) {
        self.as_ref().debug()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.as_ref().as_any()
    }

    fn serialize(&self) -> Vec<u8> {
        self.as_ref().serialize()
    }
}
