use sql_adapter::row::row::RowData;

pub trait DataRecord: Send + Sync {
    fn debug(&self) -> String;
    fn as_any(&self) -> &dyn std::any::Any;
}

impl DataRecord for RowData {
    fn debug(&self) -> String {
        format!("{:#?}", self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
impl DataRecord for Box<dyn DataRecord> {
    fn debug(&self) -> String {
        self.as_ref().debug()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.as_ref().as_any()
    }
}
