use crate::{
    core::value::Value,
    integrity::{
        algorithm::HashAlgorithm,
        canonical::serialize_value,
        coerce::{coerce_array_value, is_array_like},
        row_key::{KeyedRowHash, unkeyed},
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
    /// Reusable key buffer.
    key_buf: Vec<u8>,
}

impl RowHasher {
    pub fn new(column_order: Vec<String>, algorithm: HashAlgorithm) -> Self {
        Self {
            column_order,
            algorithm,
            buf: Vec::with_capacity(4096),
            key_buf: Vec::with_capacity(64),
        }
    }

    /// Hash a batch and pair each row hash with its canonical row key.
    pub fn hash_rows(
        &mut self,
        rows: &[&Record],
        col_types: &HashMap<String, String>,
        key_columns: &[String],
    ) -> Vec<KeyedRowHash> {
        let Some(&first_row) = rows.first() else {
            return Vec::new();
        };

        let resolve = |cols: &[String]| -> Vec<Field> {
            cols.iter()
                .map(|col| Field {
                    index: first_row.index_of(col),
                    array_like: col_types.get(col).is_some_and(|t| is_array_like(t)),
                })
                .collect()
        };

        let row_fields = resolve(&self.column_order);
        let key_fields = resolve(key_columns);
        let has_keys = !key_fields.is_empty();

        let mut results = Vec::with_capacity(rows.len());

        for &row in rows {
            self.buf.clear();
            encode_fields(row, &row_fields, &mut self.buf);
            let hash = hash_bytes(&self.buf, self.algorithm);

            if !has_keys {
                results.push(unkeyed(&hash));
                continue;
            }

            self.key_buf.clear();
            encode_fields(row, &key_fields, &mut self.key_buf);
            results.push(KeyedRowHash {
                key: self.key_buf.clone(),
                hash,
            });
        }

        results
    }
}

/// One hashed column, resolved once per batch.
struct Field {
    index: Option<usize>,
    array_like: bool,
}

/// Serialize the resolved fields of one row into `buf` in the given order.
#[inline]
fn encode_fields(row: &Record, fields: &[Field], buf: &mut Vec<u8>) {
    for field in fields {
        match field.index.and_then(|j| row.value_at(j)) {
            Some(v) if field.array_like => serialize_value(&coerce_array_value(v), buf),
            Some(v) => serialize_value(v, buf),
            None => serialize_value(&Value::Null, buf),
        }
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
    use crate::integrity::row_key::describe;
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

    fn hasher(cols: &[&str]) -> RowHasher {
        RowHasher::new(
            cols.iter().map(|c| c.to_string()).collect(),
            HashAlgorithm::Sha256,
        )
    }

    /// Hash rows with no key columns - the hash half of the pair only.
    fn hashes(hasher: &mut RowHasher, rows: &[&Record]) -> Vec<[u8; 32]> {
        hasher
            .hash_rows(rows, &HashMap::new(), &[])
            .into_iter()
            .map(|k| k.hash)
            .collect()
    }

    #[test]
    fn same_row_same_hash() {
        let row = make_record(&[("a", Value::Int(1)), ("b", Value::String("x".into()))]);
        let mut h = hasher(&["a", "b"]);
        assert_eq!(hashes(&mut h, &[&row]), hashes(&mut h, &[&row]));
    }

    #[test]
    fn different_rows_different_hashes() {
        let r1 = make_record(&[("a", Value::Int(1))]);
        let r2 = make_record(&[("a", Value::Int(2))]);
        let out = hashes(&mut hasher(&["a"]), &[&r1, &r2]);
        assert_ne!(out[0], out[1]);
    }

    #[test]
    fn column_order_is_respected() {
        // column_order determines encoding order, not record field order
        let row = make_record(&[("a", Value::Int(1)), ("b", Value::Int(2))]);
        assert_ne!(
            hashes(&mut hasher(&["a", "b"]), &[&row]),
            hashes(&mut hasher(&["b", "a"]), &[&row]),
        );
    }

    #[test]
    fn missing_column_encoded_as_null() {
        let row_missing = make_record(&[("a", Value::Int(1))]);
        let row_null = make_record(&[("a", Value::Int(1)), ("b", Value::Null)]);
        assert_eq!(
            hashes(&mut hasher(&["a", "b"]), &[&row_missing]),
            hashes(&mut hasher(&["a", "b"]), &[&row_null]),
        );
    }

    #[test]
    fn column_types_coerce_before_hashing() {
        let row = make_record(&[("a", Value::Int(1)), ("tags", Value::String("x,y".into()))]);
        let rows = vec![&row];

        let plain = hasher(&["a", "tags"]).hash_rows(&rows, &HashMap::new(), &[]);

        let mut col_types = HashMap::new();
        col_types.insert("tags".to_string(), "text[]".to_string());
        let coerced = hasher(&["a", "tags"]).hash_rows(&rows, &col_types, &[]);

        assert_ne!(plain[0].hash, coerced[0].hash, "coercion changes the hash");
    }

    #[test]
    fn key_columns_produce_a_decodable_key() {
        let row = make_record(&[
            ("actor_id", Value::Int(7)),
            ("name", Value::String("BOB".into())),
        ]);
        let keys = vec!["actor_id".to_string()];
        let out = hasher(&["actor_id", "name"]).hash_rows(&[&row], &HashMap::new(), &keys);

        assert_eq!(out.len(), 1);
        assert_eq!(describe(&out[0].key, &keys), "actor_id=7");
    }

    #[test]
    fn key_is_independent_of_the_non_key_columns() {
        // Same PK, different payload: same key, different hash. This is exactly
        // what lets verify report the row as *changed* rather than as a
        // missing row plus an extra one.
        let before = make_record(&[
            ("actor_id", Value::Int(7)),
            ("name", Value::String("BOB".into())),
        ]);
        let after = make_record(&[
            ("actor_id", Value::Int(7)),
            ("name", Value::String("ALICE".into())),
        ]);
        let keys = vec!["actor_id".to_string()];
        let mut h = hasher(&["actor_id", "name"]);
        let a = h.hash_rows(&[&before], &HashMap::new(), &keys);
        let b = h.hash_rows(&[&after], &HashMap::new(), &keys);

        assert_eq!(a[0].key, b[0].key);
        assert_ne!(a[0].hash, b[0].hash);
    }

    #[test]
    fn no_key_columns_falls_back_to_the_row_hash() {
        let row = make_record(&[("a", Value::Int(1))]);
        let out = hasher(&["a"]).hash_rows(&[&row], &HashMap::new(), &[]);
        assert_eq!(out[0].key, out[0].hash.to_vec());
    }

    #[test]
    fn batch_hashing_is_row_order_independent_per_row() {
        // A row's (key, hash) pair must not depend on its position in the batch,
        // which is the property the whole order-independent receipt rests on.
        let r1 = make_record(&[
            ("actor_id", Value::Int(1)),
            ("name", Value::String("A".into())),
        ]);
        let r2 = make_record(&[
            ("actor_id", Value::Int(2)),
            ("name", Value::String("B".into())),
        ]);
        let keys = vec!["actor_id".to_string()];
        let mut h = hasher(&["actor_id", "name"]);

        let forward = h.hash_rows(&[&r1, &r2], &HashMap::new(), &keys);
        let reverse = h.hash_rows(&[&r2, &r1], &HashMap::new(), &keys);

        assert_eq!(forward[0], reverse[1]);
        assert_eq!(forward[1], reverse[0]);
    }
}
