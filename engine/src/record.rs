use bincode;
use lazy_static::lazy_static;
use serde::de::DeserializeOwned;
use sql_adapter::row::row::RowData;
use std::collections::HashMap;
use tokio::sync::Mutex;

type BoxedDataRecord = Box<dyn DataRecord + Send + Sync>;

lazy_static! {
    static ref TYPE_REGISTRY: Mutex<HashMap<String, fn(Vec<u8>) -> BoxedDataRecord>> =
        Mutex::new(HashMap::new());
}

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

pub async fn register_data_record<T: DataRecord + DeserializeOwned + 'static>(type_name: &str)
where
    T: 'static,
{
    TYPE_REGISTRY
        .lock()
        .await
        .insert(type_name.to_string(), |data| {
            let deserialized: T = bincode::deserialize(&data).expect("Failed to deserialize");
            Box::new(deserialized)
        });
}

pub async fn deserialize_data_record(type_name: &str, data: Vec<u8>) -> BoxedDataRecord {
    let registry = TYPE_REGISTRY.lock().await;
    let deserializer = registry.get(type_name).expect("Type not found");
    deserializer(data)
}
