use bincode;
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
        bincode::serialize(self).unwrap()
    }

    fn deserialize(data: Vec<u8>) -> Self {
        match bincode::deserialize::<RowData>(&data) {
            Ok(data) => data,
            Err(e) => {
                panic!("Failed to deserialize: {:?}", e);
            }
        }
    }
}
