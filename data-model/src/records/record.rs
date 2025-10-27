use crate::records::row_data::RowData;

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
