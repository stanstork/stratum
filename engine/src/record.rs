use sql_adapter::row::row_data::RowData;

#[derive(Debug, Clone)]
pub enum Record {
    RowData(RowData),
}

impl Record {
    pub fn debug(&self) {
        match self {
            Record::RowData(data) => data.debug(),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Record::RowData(data) => data.serialize(),
        }
    }

    pub fn deserialize(&self, data: Vec<u8>) -> Self {
        match self {
            Record::RowData(_) => Record::RowData(RowData::deserialize(data)),
        }
    }

    pub fn to_row_data(&self) -> Option<&RowData> {
        match self {
            Record::RowData(data) => Some(data),
        }
    }
}

pub trait DataRecord: Send + Sync {
    fn debug(&self);
    fn as_any(&self) -> &dyn std::any::Any;
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(data: Vec<u8>) -> Self;
}

impl DataRecord for RowData {
    fn debug(&self) {
        println!("{:#?}", self);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_else(|_| {
            panic!("Failed to serialize: {:?}", self);
        })
    }

    fn deserialize(data: Vec<u8>) -> Self {
        serde_json::from_slice(&data).unwrap_or_else(|_| {
            panic!("Failed to deserialize: {:?}", data);
        })
    }
}
