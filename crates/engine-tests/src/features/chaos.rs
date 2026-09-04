#[cfg(test)]
mod tests {
    use crate::harness::ppl::dest_connection;
    use crate::harness::{db::Dbms, fixtures::reset_postgres_schema, runner::run_ppl};
    use bigdecimal::BigDecimal;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use connectors::drivers::postgres::encoding::binary::{BinaryColumnType, PgBinaryEncoder};
    use model::core::types::Type;
    use model::core::value::{FieldValue, Value};
    use model::integrity::algorithm::HashAlgorithm;
    use model::integrity::canonical::serialize_value;
    use model::integrity::hasher::RowHasher;
    use model::records::Record;
    use std::collections::HashMap;
    use std::io::Write;
    use std::str::FromStr;

    /// A deeply nested JSON array `depth` levels down, wrapping a pathological leaf.
    fn nested_json(depth: usize) -> serde_json::Value {
        let mut v = serde_json::json!({ "~": "\u{0}\"\\", "n": f64::MAX });
        for _ in 0..depth {
            v = serde_json::Value::Array(vec![v]);
        }
        v
    }

    /// The battery of hostile values. Each is something a real source could hand us
    /// after a lossy cross-database read or a malformed file.
    fn pathological_values() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Boolean(true),
            // Extreme integers.
            Value::Int(i64::MIN),
            Value::Int(i64::MAX),
            Value::UInt(0),
            Value::UInt(u64::MAX),
            Value::Year(i16::MIN),
            Value::Year(i16::MAX),
            // Non-finite and boundary floats.
            Value::Float(f64::NAN),
            Value::Float(f64::INFINITY),
            Value::Float(f64::NEG_INFINITY),
            Value::Float(-0.0),
            Value::Float(f64::MIN),
            Value::Float(f64::MAX),
            Value::Float(f64::MIN_POSITIVE),
            // Wide decimals that overflow i64/u64 (exercises the nbase-group encoder
            // and the decimal-literal fallback).
            Value::Decimal(
                BigDecimal::from_str("123456789012345678901234567890.123456789").unwrap(),
            ),
            Value::Decimal(
                BigDecimal::from_str("-99999999999999999999999999999999999999").unwrap(),
            ),
            Value::Decimal(BigDecimal::from_str("0.00000000000000000000000000001").unwrap()),
            // Huge and adversarial strings.
            Value::String("a".repeat(300_000)),
            Value::String("\u{0}\"\\\n\t\r'; DROP TABLE users;--".to_string()),
            Value::String("héllo wörld 🌍🔥 مرحبا こんにちは".to_string()),
            // True binary (invalid UTF-8) vs. valid-UTF-8 binary (normalized to string).
            Value::Binary(vec![0xff, 0xfe, 0x00, 0x80, 0xc0, 0x01]),
            Value::Binary(b"valid ascii bytes".to_vec()),
            Value::Bits([true, false, true].repeat(334)),
            Value::Geometry(vec![0x00, 0x01, 0xff, 0xfe]),
            Value::Set(vec!["a".repeat(10_000), "\u{0}".to_string()]),
            // Deeply nested JSON with unicode/control-char keys and non-finite-ish leaves.
            Value::Json(nested_json(40)),
            Value::Json(serde_json::json!({ "z": 1, "a": 2, "m": { "nested": [true, null] } })),
            // Nested arrays.
            Value::Array(vec![
                Value::Int(i64::MIN),
                Value::Array(vec![Value::Float(f64::NAN), Value::Null]),
            ]),
            // Temporals with extreme UTC offsets (exercises the checked datetime math).
            Value::Timestamp {
                value: NaiveDateTime::MAX,
                offset_secs: Some(i32::MIN),
            },
            Value::Timestamp {
                value: NaiveDateTime::MIN,
                offset_secs: Some(i32::MAX),
            },
            Value::Date(NaiveDate::MIN),
            Value::Date(NaiveDate::MAX),
            Value::Time {
                value: NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
                offset_secs: Some(i32::MAX),
            },
        ]
    }

    /// The canonical serializer and both hash algorithms must consume every
    /// pathological value without panicking, and hashing must stay deterministic.
    #[test]
    fn chaos_canonical_serialize_and_hash_are_panic_free_and_deterministic() {
        let values = pathological_values();

        // Serialize each value on its own: the core canonical encoding must not panic.
        for v in &values {
            let mut a = Vec::new();
            let mut b = Vec::new();
            serialize_value(v, &mut a);
            serialize_value(v, &mut b);
            assert_eq!(a, b, "canonical serialization must be deterministic");
        }

        // Build one wide row out of all of them and hash it with both algorithms twice.
        let fields: Vec<FieldValue> = values
            .iter()
            .enumerate()
            .map(|(i, v)| FieldValue {
                name: format!("c{i}"),
                value: Some(v.clone()),
                data_type: Type::Boolean,
            })
            .collect();
        let column_order: Vec<String> = fields.iter().map(|f| f.name.clone()).collect();
        let record = Record::from_fields("chaos", fields, Default::default());

        let no_types = HashMap::new();
        for algo in [HashAlgorithm::Sha256, HashAlgorithm::Blake3] {
            let mut h1 = RowHasher::new(column_order.clone(), algo);
            let mut h2 = RowHasher::new(column_order.clone(), algo);
            assert_eq!(
                h1.hash_rows(&[&record], &no_types, &[]),
                h2.hash_rows(&[&record], &no_types, &[]),
                "row hashing must be deterministic for {algo:?}"
            );
        }
    }

    /// The Postgres binary (COPY) encoder must return `Ok`/`Err` for every
    /// (column type, value) pairing - including type mismatches and out-of-range
    /// values - but never panic.
    #[test]
    fn chaos_pg_binary_encoder_is_panic_free() {
        use BinaryColumnType::*;
        let encoder = PgBinaryEncoder;
        let column_types = [
            Bool,
            Int2,
            Int4,
            Int8,
            Float4,
            Float8,
            Numeric,
            Text,
            Json,
            Jsonb,
            Bytea,
            Uuid,
            Date,
            Time,
            Timestamp,
            TimestampTz,
        ];

        for value in pathological_values() {
            for &col in &column_types {
                let mut out = Vec::new();
                // A panic here fails the test; a returned Err is the correct,
                // graceful outcome for an unencodable pairing.
                let _ = encoder.write_field(col, &value, &mut out);
            }
        }
    }

    /// A CSV whose rows carry stress values: a very long string, an extreme
    /// integer, a fractional/scientific number, unicode, and embedded quotes.
    fn pathological_csv() -> tempfile::NamedTempFile {
        let long = "x".repeat(100_000);
        let contents = format!(
            "id,label,amount,note\n\
             1,{long},9223372036854775807,plain\n\
             2,\"quote\"\"inside\",-0.000001,héllo 🌍\n\
             3,scientific,1e30,\"comma, and\nnewline\"\n"
        );
        let mut file = tempfile::Builder::new()
            .suffix(".csv")
            .tempfile()
            .expect("create temp csv");
        file.write_all(contents.as_bytes()).expect("write temp csv");
        file.flush().expect("flush temp csv");
        file
    }

    /// Drive the pathological CSV through a full CSV -> Postgres pipeline. The
    /// migration must either complete or return a `MigrationError` - never panic.
    #[tokio::test(flavor = "multi_thread")]
    async fn chaos_csv_stress_to_postgres() {
        reset_postgres_schema().await;
        let csv = pathological_csv();
        let path = csv.path().to_str().expect("csv path is valid utf-8");

        let ppl = format!(
            "connection \"src\" {{ driver = \"csv\" url = \"{path}\" pk_column = \"id\" }}\n\
             {}\n\
             pipeline \"chaos_load\" {{\n\
                 from {{ connection = connection.src table = \"stress\" }}\n\
                 to   {{ connection = connection.dst table = \"stress\" }}\n\
                 settings {{ create_missing_tables = true batch_size = 2 }}\n\
             }}\n",
            dest_connection("dst", Dbms::Postgres),
        );

        // The contract is "no panic": either outcome is acceptable, as long as the
        // engine returns control instead of unwinding.
        let _ = run_ppl(&ppl, false).await;
    }
}
