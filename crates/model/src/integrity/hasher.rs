use crate::{
    core::value::Value,
    integrity::{
        algorithm::HashAlgorithm, canonical::serialize_value, coerce::coerce_value_for_hash,
    },
    records::Record,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

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
            match row.value(col) {
                Some(v) => serialize_value(v, &mut self.buf),
                None => serialize_value(&Value::Null, &mut self.buf),
            }
        }
        hash_bytes(&self.buf, self.algorithm)
    }

    /// Like [`hash_row`](Self::hash_row) but applies per-column coercions
    /// when `col_types` is non-empty.
    pub fn hash_row_coerced(
        &mut self,
        row: &Record,
        col_types: &HashMap<String, String>,
    ) -> [u8; 32] {
        if col_types.is_empty() {
            return self.hash_row(row);
        }

        self.buf.clear();

        for col in &self.column_order {
            match row.value(col) {
                Some(value) => {
                    let col_type = col_types.get(col).map(|s| s.as_str()).unwrap_or("");
                    serialize_value(&coerce_value_for_hash(value, col_type), &mut self.buf);
                }
                None => serialize_value(&Value::Null, &mut self.buf),
            }
        }

        hash_bytes(&self.buf, self.algorithm)
    }

    /// Hash a whole batch, resolving each ordered column to its
    /// position in the row's field vector **once** for the batch.
    pub fn hash_rows(
        &mut self,
        rows: &[&Record],
        col_types: &HashMap<String, String>,
    ) -> Vec<[u8; 32]> {
        if rows.is_empty() {
            return Vec::new();
        }

        // Destructure so `buf` can be borrowed mutably while `column_order` /
        // `algorithm` are read inside the row loop.
        let RowHasher {
            column_order,
            algorithm,
            buf,
        } = self;

        // All rows in a batch share one schema, so resolve each ordered column to
        // its position once via the shared schema index.
        let field_idx: Vec<Option<usize>> = column_order
            .iter()
            .map(|col| rows[0].index_of(col))
            .collect();

        // Resolve the coercion type per column once.
        let coerce = !col_types.is_empty();
        let col_types_by_pos: Vec<&str> = if coerce {
            column_order
                .iter()
                .map(|col| col_types.get(col).map(|s| s.as_str()).unwrap_or(""))
                .collect()
        } else {
            Vec::new()
        };

        rows.iter()
            .map(|row| {
                buf.clear();
                for (i, idx) in field_idx.iter().enumerate() {
                    let value = idx.and_then(|j| row.value_at(j));
                    match value {
                        Some(v) if coerce => {
                            serialize_value(&coerce_value_for_hash(v, col_types_by_pos[i]), buf)
                        }
                        Some(v) => serialize_value(v, buf),
                        None => serialize_value(&Value::Null, buf),
                    }
                }
                hash_bytes(buf, *algorithm)
            })
            .collect()
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
        Record::from_fields(
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

    #[test]
    fn hash_rows_matches_per_row_hashing() {
        use std::collections::HashMap;

        let cols = vec!["a".to_string(), "b".to_string(), "tags".to_string()];
        let make = || RowHasher::new(cols.clone(), HashAlgorithm::Sha256);

        let r1 = make_record(&[
            ("a", Value::Int(1)),
            ("b", Value::Null), // present but null
            ("tags", Value::String("x,y".into())),
        ]);
        let r2 = make_record(&[
            ("a", Value::Int(2)),
            ("b", Value::String("z".into())),
            // "tags" omitted -> encoded as Null
        ]);
        let rows: Vec<&Record> = vec![&r1, &r2];

        // No coercion: batch == per-row.
        let empty = HashMap::new();
        let batch = make().hash_rows(&rows, &empty);
        let mut single = make();
        let per_row: Vec<_> = rows
            .iter()
            .map(|r| single.hash_row_coerced(r, &empty))
            .collect();
        assert_eq!(batch, per_row);

        // With coercion (tags -> array): batch == per-row, and it changed the hash.
        let mut col_types = HashMap::new();
        col_types.insert("tags".to_string(), "text[]".to_string());
        let batch_coerced = make().hash_rows(&rows, &col_types);
        let mut single_coerced = make();
        let per_row_coerced: Vec<_> = rows
            .iter()
            .map(|r| single_coerced.hash_row_coerced(r, &col_types))
            .collect();
        assert_eq!(batch_coerced, per_row_coerced);
        assert_ne!(batch, batch_coerced);
    }
}
