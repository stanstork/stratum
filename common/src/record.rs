pub trait DataRecord: Send + Sync {
    fn debug(&self);
    fn as_any(&self) -> &dyn std::any::Any;
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(data: Vec<u8>) -> Self;
}
