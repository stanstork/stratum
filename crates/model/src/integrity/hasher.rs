use crate::{
    core::value::Value,
    integrity::{algorithm::HashAlgorithm, canonical::serialize_value},
    records::Record,
};
use sha2::{Digest, Sha256};

pub struct RowHasher {
    /// Destination column names in lexicographic order.
    column_order: Vec<String>,
    algorithm: HashAlgorithm,
    /// Reusable buffer - cleared between rows, capacity retained.
    buf: Vec<u8>,
}

impl RowHasher {
    pub fn new(column_order: Vec<String>, algorithm: HashAlgorithm) -> Self {
        Self {
            column_order,
            algorithm,
            buf: Vec::with_capacity(4096),
        }
    }

    /// Serialize a single row into canonical bytes then hash it.
    /// Missing columns are encoded as Null.
    pub fn hash_row(&mut self, row: &Record) -> [u8; 32] {
        self.buf.clear();
        for col in &self.column_order {
            match row.get(col) {
                Some(fv) => {
                    serialize_value(fv.value.as_ref().unwrap_or(&Value::Null), &mut self.buf)
                }
                None => serialize_value(&Value::Null, &mut self.buf),
            }
        }
        hash_bytes(&self.buf, self.algorithm)
    }
}

fn hash_bytes(data: &[u8], algorithm: HashAlgorithm) -> [u8; 32] {
    match algorithm {
        HashAlgorithm::Sha256 => Sha256::digest(data).into(),
        HashAlgorithm::Blake3 => blake3::hash(data).into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::Type;
    use crate::core::value::FieldValue;
    use crate::records::OpType;

    fn make_record(fields: &[(&str, Value)]) -> Record {
        Record::new(
            "test",
            fields
                .iter()
                .map(|(name, val)| FieldValue {
                    name: name.to_string(),
                    value: Some(val.clone()),
                    data_type: Type::Text { charset: None },
                })
                .collect(),
            OpType::default(),
        )
    }

    #[test]
    fn same_row_same_hash() {
        let mut hasher = RowHasher::new(vec!["a".into(), "b".into()], HashAlgorithm::Sha256);
        let row = make_record(&[("a", Value::Int(1)), ("b", Value::String("x".into()))]);
        assert_eq!(hasher.hash_row(&row), hasher.hash_row(&row));
    }

    #[test]
    fn different_rows_different_hashes() {
        let mut hasher = RowHasher::new(vec!["a".into()], HashAlgorithm::Sha256);
        let r1 = make_record(&[("a", Value::Int(1))]);
        let r2 = make_record(&[("a", Value::Int(2))]);
        assert_ne!(hasher.hash_row(&r1), hasher.hash_row(&r2));
    }

    #[test]
    fn column_order_is_respected() {
        // column_order determines encoding order, not record field order
        let mut h_ab = RowHasher::new(vec!["a".into(), "b".into()], HashAlgorithm::Sha256);
        let mut h_ba = RowHasher::new(vec!["b".into(), "a".into()], HashAlgorithm::Sha256);
        let row = make_record(&[("a", Value::Int(1)), ("b", Value::Int(2))]);
        assert_ne!(h_ab.hash_row(&row), h_ba.hash_row(&row));
    }

    #[test]
    fn missing_column_encoded_as_null() {
        let mut h_with = RowHasher::new(vec!["a".into(), "b".into()], HashAlgorithm::Sha256);
        let mut h_null = RowHasher::new(vec!["a".into(), "b".into()], HashAlgorithm::Sha256);
        let row_missing = make_record(&[("a", Value::Int(1))]);
        let row_null = make_record(&[("a", Value::Int(1)), ("b", Value::Null)]);
        assert_eq!(h_with.hash_row(&row_missing), h_null.hash_row(&row_null));
    }
}
