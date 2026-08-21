use crate::{
    core::value::Value,
    integrity::{
        canonical::{describe_key, serialize_value},
        coerce::coerce_value_for_hash,
    },
    records::Record,
};
use std::collections::HashMap;

/// One migrated row, reduced to the pair verification actually compares:
/// an order-independent key and the hash of the row's contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyedRowHash {
    /// Canonical byte encoding of the row's key columns.
    pub key: Vec<u8>,
    /// Hash of the row's canonical column encoding.
    pub hash: [u8; 32],
}

/// Canonical byte encoding of a row's key columns, used as an order-independent
/// verification key.
pub fn encode_row_key(
    row: &Record,
    key_cols: &[String],
    col_types: &HashMap<String, String>,
    buf: &mut Vec<u8>,
) {
    buf.clear();

    let coerce = !col_types.is_empty();

    for col in key_cols {
        let Some(v) = row.value(col) else {
            serialize_value(&Value::Null, buf);
            continue;
        };

        if coerce {
            let col_type = col_types.get(col).map(String::as_str).unwrap_or_default();
            serialize_value(&coerce_value_for_hash(v, col_type), buf);
        } else {
            serialize_value(v, buf);
        }
    }
}

/// Key for a table with no primary key: the row hash stands in for its own key.
pub fn unkeyed(hash: &[u8; 32]) -> KeyedRowHash {
    KeyedRowHash {
        key: hash.to_vec(),
        hash: *hash,
    }
}

/// Render a stored row key for a divergence report.
pub fn describe(key: &[u8], key_columns: &[String]) -> String {
    if !key_columns.is_empty() {
        return describe_key(key, key_columns);
    }

    let mut out = String::with_capacity(17);
    out.push('#');

    for byte in key.iter().take(8) {
        use std::fmt::Write;
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{core::value::Value, records::OpType};

    fn row(vals: &[(&str, Value)]) -> Record {
        use crate::core::value::FieldValue;
        Record::from_fields(
            "t",
            vals.iter()
                .map(|(n, v)| FieldValue {
                    name: n.to_string(),
                    value: Some(v.clone()),
                    data_type: v.data_type(),
                })
                .collect(),
            OpType::Insert,
        )
    }

    #[test]
    fn key_is_stable_and_distinguishes_rows() {
        let ct = HashMap::new();
        let pk = vec!["id".to_string()];
        let mut buf = Vec::new();

        encode_row_key(&row(&[("id", Value::Int(1))]), &pk, &ct, &mut buf);
        let k1 = buf.clone();
        encode_row_key(&row(&[("id", Value::Int(1))]), &pk, &ct, &mut buf);
        let k1_again = buf.clone();
        encode_row_key(&row(&[("id", Value::Int(2))]), &pk, &ct, &mut buf);
        let k2 = buf.clone();

        assert_eq!(k1, k1_again, "same PK -> same key");
        assert_ne!(k1, k2, "different PK -> different key");
    }

    #[test]
    fn composite_key_encodes_all_columns_in_order() {
        let ct = HashMap::new();
        let pk = vec!["a".to_string(), "b".to_string()];
        let mut buf = Vec::new();

        // (1, 2) and (2, 1) must not collide.
        encode_row_key(
            &row(&[("a", Value::Int(1)), ("b", Value::Int(2))]),
            &pk,
            &ct,
            &mut buf,
        );
        let k12 = buf.clone();
        encode_row_key(
            &row(&[("a", Value::Int(2)), ("b", Value::Int(1))]),
            &pk,
            &ct,
            &mut buf,
        );
        assert_ne!(k12, buf, "column order matters in a composite key");
    }

    #[test]
    fn missing_key_column_encodes_as_null() {
        let ct = HashMap::new();
        let pk = vec!["id".to_string()];
        let mut buf = Vec::new();

        encode_row_key(&row(&[("other", Value::Int(1))]), &pk, &ct, &mut buf);
        let missing = buf.clone();
        encode_row_key(&row(&[("id", Value::Null)]), &pk, &ct, &mut buf);
        assert_eq!(missing, buf);
    }

    #[test]
    fn keys_round_trip_into_readable_text() {
        let ct = HashMap::new();
        let pk = vec!["actor_id".to_string()];
        let mut buf = Vec::new();

        encode_row_key(&row(&[("actor_id", Value::Int(42))]), &pk, &ct, &mut buf);
        assert_eq!(describe(&buf, &pk), "actor_id=42");

        let composite = vec!["store".to_string(), "sku".to_string()];
        encode_row_key(
            &row(&[
                ("store", Value::Int(7)),
                ("sku", Value::String("A-1".into())),
            ]),
            &composite,
            &ct,
            &mut buf,
        );
        assert_eq!(describe(&buf, &composite), "store=7,sku=A-1");
    }

    #[test]
    fn unkeyed_uses_the_row_hash_as_its_own_key() {
        let h = [0xab; 32];
        let entry = unkeyed(&h);
        assert_eq!(entry.key, h.to_vec());
        assert_eq!(entry.hash, h);
        // No key columns -> shown as a digest, never decoded as if it were one.
        assert_eq!(describe(&entry.key, &[]), "#abababababababab");
    }
}
