use crate::{
    error::WasmError,
    exchange::{
        json_v1,
        types::{FilterDecision, PluginInput, PluginOutput},
    },
    schema::PluginField,
};
use bigdecimal::BigDecimal;
use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};
use model::{core::value::Value, records::Record};
use std::{collections::HashMap, str::FromStr};
use uuid::Uuid;

const MAGIC: u32 = 0xC0_1A_C0_01;

// Column type tags. Fixed-width types move by memcpy; varlen types use an
// Arrow-style offsets+blob. Types both host and guest can represent get a native
// tag; anything else (host-only variants, mixed columns) uses TAG_CELL.
const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1; // [u8; n]
const TAG_I64: u8 = 2; // [i64; n]
const TAG_U64: u8 = 3; // [u64; n]
const TAG_F64: u8 = 4; // [f64; n]
const TAG_STRING: u8 = 5; // varlen utf8
const TAG_BINARY: u8 = 6; // varlen raw bytes
const TAG_DATE: u8 = 7; // [i32; n] days from CE
const TAG_TIME: u8 = 8; // [i64; n] nanos from midnight
const TAG_UUID: u8 = 9; // [16 bytes; n]
const TAG_DECIMAL: u8 = 10; // varlen canonical decimal string
const TAG_TIMESTAMP: u8 = 11; // varlen canonical ISO string
const TAG_JSON: u8 = 12; // varlen compact JSON text
const TAG_CELL: u8 = 13; // varlen per-cell json_v1 enveloped value (fallback)

/// A decoded batch: row count plus one `(name, values)` pair per column.
type DecodedBatch = (usize, Vec<(String, Vec<Value>)>);

/// Serialize a batch of transform/filter inputs as columns, one per declared
/// schema field. Values are coerced toward the declared type.
pub fn serialize_input_batch(
    inputs: &[PluginInput],
    schema: &[PluginField],
) -> Result<Vec<u8>, WasmError> {
    let n_rows = inputs.len();
    let mut columns = Vec::with_capacity(schema.len());

    for field in schema {
        let tag = field.field_type.as_str();
        let mut col = Vec::with_capacity(n_rows);

        for input in inputs {
            let v = match input.get(&field.name) {
                Some(v) => json_v1::coerce_value(v, tag),
                None => Value::Null,
            };
            col.push(v);
        }

        columns.push((field.name.as_str(), col));
    }

    Ok(encode_batch(n_rows, &columns))
}

/// Serialize plugin inputs straight from source records - no per-row
/// `PluginInput`/HashMap.
pub fn serialize_input_from_records(
    rows: &[Record],
    schema: &[PluginField],
    mapping: &HashMap<String, String>,
) -> Result<Vec<u8>, WasmError> {
    let n_rows = rows.len();
    let src_schema = rows.first().map(|r| r.schema());

    // Resolve each plugin field to (source column index, coercion tag) once.
    let plan: Vec<(Option<usize>, &str)> = schema
        .iter()
        .map(|field| {
            let idx = mapping
                .get(&field.name)
                .and_then(|src| src_schema.and_then(|s| s.index_of(src)));
            (idx, field.field_type.as_str())
        })
        .collect();

    let mut columns: Vec<(&str, Vec<Value>)> = Vec::with_capacity(schema.len());

    for (field, (idx, tag)) in schema.iter().zip(&plan) {
        let mut col = Vec::with_capacity(n_rows);

        for row in rows {
            let v = idx
                .and_then(|i| row.value_at(i))
                .map(|v| json_v1::coerce_value(v, tag))
                .unwrap_or(Value::Null);
            col.push(v);
        }

        columns.push((field.name.as_str(), col));
    }

    Ok(encode_batch(n_rows, &columns))
}

/// Deserialize a batch of transform outputs: a single-column batch.
pub fn deserialize_output_batch(
    bytes: &[u8],
    plugin: &str,
) -> Result<Vec<PluginOutput>, WasmError> {
    if let Some(err) = guest_error(bytes) {
        return Err(WasmError::PluginError {
            plugin: plugin.to_string(),
            message: err,
        });
    }

    let (_, mut cols) = decode_batch(bytes, plugin)?;
    let values = cols.drain(..).next().map(|(_, v)| v).ok_or_else(|| {
        WasmError::DeserializationError(format!("{plugin}: output batch has no column"))
    })?;

    Ok(values
        .into_iter()
        .map(|value| PluginOutput { value })
        .collect())
}

/// Deserialize a batch of filter decisions: a two-column batch (`pass`: bool,
/// `reason`: string, null where the row passed).
pub fn deserialize_filter_decision_batch(
    bytes: &[u8],
    plugin: &str,
) -> Result<Vec<FilterDecision>, WasmError> {
    if let Some(err) = guest_error(bytes) {
        return Err(WasmError::PluginError {
            plugin: plugin.to_string(),
            message: err,
        });
    }

    let (n_rows, cols) = decode_batch(bytes, plugin)?;

    let pass = cols
        .iter()
        .find(|(name, _)| name == "pass")
        .map(|(_, v)| v)
        .ok_or_else(|| {
            WasmError::DeserializationError(format!("{plugin}: filter batch missing 'pass' column"))
        })?;

    let reason = cols
        .iter()
        .find(|(name, _)| name == "reason")
        .map(|(_, v)| v);

    let mut out = Vec::with_capacity(n_rows);

    for i in 0..n_rows {
        let passed = matches!(pass.get(i), Some(Value::Boolean(true)));

        if passed {
            out.push(FilterDecision::Pass);
        } else {
            let reason_str = reason
                .and_then(|r| r.get(i))
                .and_then(|v| match v {
                    Value::String(s) => Some(s.to_string()),
                    _ => None,
                })
                .unwrap_or_else(|| "rejected".to_string());

            out.push(FilterDecision::Reject {
                reason: (reason_str),
            });
        }
    }

    Ok(out)
}

/// Detects JSON error payloads for batch rejections.
fn guest_error(bytes: &[u8]) -> Option<String> {
    if bytes.len() >= 4 {
        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if magic == MAGIC {
            return None;
        }
    }
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()?
        .get("error")
        .map(|e| e.as_str().unwrap_or("unknown error").to_string())
}

fn encode_batch(n_rows: usize, columns: &[(&str, Vec<Value>)]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64 + n_rows * 8 * columns.len().max(1));
    buf.extend_from_slice(&MAGIC.to_le_bytes());
    buf.extend_from_slice(&(n_rows as u32).to_le_bytes());
    buf.extend_from_slice(&(columns.len() as u32).to_le_bytes());

    for (name, values) in columns {
        encode_column(&mut buf, name, values, n_rows);
    }

    buf
}

fn encode_column(buf: &mut Vec<u8>, name: &str, values: &[Value], n_rows: usize) {
    buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
    buf.extend_from_slice(name.as_bytes());

    let tag = column_tag(values);
    buf.push(tag);

    // Validity bitmap: only emitted when at least one cell is null.
    let any_null = values.iter().any(|v| matches!(v, Value::Null));
    if any_null && tag != TAG_NULL {
        buf.push(1);
        let mut bitmap = vec![0u8; n_rows.div_ceil(8)];
        for (i, v) in values.iter().enumerate() {
            if !matches!(v, Value::Null) {
                bitmap[i / 8] |= 1 << (i % 8);
            }
        }
        buf.extend_from_slice(&bitmap);
    } else {
        buf.push(0);
    }

    match tag {
        TAG_NULL => {}
        TAG_BOOL => buf.extend(values.iter().map(|v| matches!(v, Value::Boolean(true)) as u8)),
        TAG_I64 => {
            for v in values {
                let n = if let Value::Int(i) = v { *i } else { 0 };
                buf.extend_from_slice(&n.to_le_bytes());
            }
        }
        TAG_U64 => {
            for v in values {
                let n = if let Value::UInt(u) = v { *u } else { 0 };
                buf.extend_from_slice(&n.to_le_bytes());
            }
        }
        TAG_F64 => {
            for v in values {
                let f = if let Value::Float(f) = v { *f } else { 0.0 };
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        TAG_DATE => {
            for v in values {
                let d = if let Value::Date(d) = v { d.num_days_from_ce() } else { 0 };
                buf.extend_from_slice(&d.to_le_bytes());
            }
        }
        TAG_TIME => {
            for v in values {
                let ns = if let Value::Time { value: t, .. } = v { time_to_nanos(t) } else { 0 };
                buf.extend_from_slice(&ns.to_le_bytes());
            }
        }
        TAG_UUID => {
            for v in values {
                let bytes = if let Value::Uuid(u) = v { *u.as_bytes() } else { [0u8; 16] };
                buf.extend_from_slice(&bytes);
            }
        }
        TAG_TIMESTAMP => {
            for v in values {
                let (days, nanos, off) = match v {
                    Value::Timestamp { value: ts, offset_secs } => (
                        ts.date().num_days_from_ce(),
                        time_to_nanos(&ts.time()),
                        offset_secs.unwrap_or(i32::MIN),
                    ),
                    _ => (0, 0, i32::MIN),
                };
                buf.extend_from_slice(&days.to_le_bytes());
                buf.extend_from_slice(&nanos.to_le_bytes());
                buf.extend_from_slice(&off.to_le_bytes());
            }
        }
        TAG_STRING => encode_varlen_from(buf, values, |v| match v {
            Value::String(s) => s.as_bytes().to_vec(),
            _ => Vec::new(),
        }),
        TAG_BINARY => encode_varlen_from(buf, values, |v| match v {
            Value::Binary(b) => b.clone(),
            _ => Vec::new(),
        }),
        TAG_DECIMAL => encode_varlen_from(buf, values, |v| match v {
            Value::Decimal(d) => d.to_string().into_bytes(),
            _ => Vec::new(),
        }),
        TAG_JSON => encode_varlen_from(buf, values, |v| match v {
            Value::Json(j) => serde_json::to_vec(j).unwrap_or_default(),
            _ => Vec::new(),
        }),
        _ /* TAG_CELL */ => encode_varlen_from(buf, values, |v| {
            serde_json::to_vec(&json_v1::value_to_json(v)).unwrap_or_default()
        }),
    }
}

fn time_to_nanos(t: &NaiveTime) -> i64 {
    t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64
}

fn encode_varlen_from(buf: &mut Vec<u8>, values: &[Value], f: impl Fn(&Value) -> Vec<u8>) {
    let cells: Vec<Vec<u8>> = values.iter().map(f).collect();
    let mut offset: u32 = 0;
    buf.extend_from_slice(&offset.to_le_bytes());

    for c in &cells {
        offset += c.len() as u32;
        buf.extend_from_slice(&offset.to_le_bytes());
    }

    for c in &cells {
        buf.extend_from_slice(c);
    }
}

fn column_tag(values: &[Value]) -> u8 {
    let mut tag: Option<u8> = None;

    for v in values {
        let this = match v {
            Value::Null => continue,
            Value::Boolean(_) => TAG_BOOL,
            Value::Int(_) => TAG_I64,
            Value::UInt(_) => TAG_U64,
            Value::Float(_) => TAG_F64,
            Value::String(_) => TAG_STRING,
            Value::Binary(_) => TAG_BINARY,
            Value::Date(_) => TAG_DATE,
            Value::Time { .. } => TAG_TIME,
            Value::Uuid(_) => TAG_UUID,
            Value::Decimal(_) => TAG_DECIMAL,
            Value::Timestamp { .. } => TAG_TIMESTAMP,
            Value::Json(_) => TAG_JSON,
            _ => return TAG_CELL, // host-only variant -> JSON fallback
        };

        match tag {
            None => tag = Some(this),
            Some(t) if t == this => {}
            Some(_) => return TAG_CELL, // mixed variants
        }
    }

    tag.unwrap_or(TAG_NULL)
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
    plugin: &'a str,
}

impl<'a> Reader<'a> {
    fn err(&self, what: &str) -> WasmError {
        WasmError::DeserializationError(format!("{}: columnar_v1: {what}", self.plugin))
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], WasmError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| self.err("length overflow"))?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| self.err("truncated"))?;
        self.pos = end;
        Ok(slice)
    }

    fn u32(&mut self) -> Result<u32, WasmError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u8(&mut self) -> Result<u8, WasmError> {
        Ok(self.take(1)?[0])
    }
}

fn decode_batch(bytes: &[u8], plugin: &str) -> Result<DecodedBatch, WasmError> {
    let mut r = Reader {
        bytes,
        pos: 0,
        plugin,
    };

    if r.u32()? != MAGIC {
        return Err(r.err("bad magic"));
    }

    let n_rows = r.u32()? as usize;
    let n_cols = r.u32()? as usize;

    let mut columns = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        columns.push(decode_column(&mut r, n_rows)?);
    }

    Ok((n_rows, columns))
}

fn decode_column(r: &mut Reader<'_>, n_rows: usize) -> Result<(String, Vec<Value>), WasmError> {
    let name_len = r.u32()? as usize;
    let name = String::from_utf8(r.take(name_len)?.to_vec())
        .map_err(|_| r.err("column name not utf-8"))?;

    let tag = r.u8()?;
    let has_validity = r.u8()? != 0;

    let validity = if has_validity {
        Some(r.take(n_rows.div_ceil(8))?.to_vec())
    } else {
        None
    };

    let is_valid = |i: usize| {
        validity
            .as_ref()
            .is_none_or(|b| b[i / 8] & (1 << (i % 8)) != 0)
    };

    let mut values = Vec::with_capacity(n_rows);

    match tag {
        TAG_NULL => values.resize(n_rows, Value::Null),
        TAG_BOOL => {
            let data = r.take(n_rows)?;
            for (i, &b) in data.iter().enumerate() {
                values.push(if is_valid(i) {
                    Value::Boolean(b != 0)
                } else {
                    Value::Null
                });
            }
        }
        TAG_I64 => {
            let data = r.take(n_rows * 8)?;
            for i in 0..n_rows {
                let n = i64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap_or_default());
                values.push(if is_valid(i) {
                    Value::Int(n)
                } else {
                    Value::Null
                });
            }
        }
        TAG_U64 => {
            let data = r.take(n_rows * 8)?;
            for i in 0..n_rows {
                let n = u64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap_or_default());
                values.push(if is_valid(i) {
                    Value::UInt(n)
                } else {
                    Value::Null
                });
            }
        }
        TAG_F64 => {
            let data = r.take(n_rows * 8)?;
            for i in 0..n_rows {
                let f = f64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap_or_default());
                values.push(if is_valid(i) {
                    Value::Float(f)
                } else {
                    Value::Null
                });
            }
        }
        TAG_DATE => {
            let data = r.take(n_rows * 4)?;
            for i in 0..n_rows {
                let n = i32::from_le_bytes(data[i * 4..i * 4 + 4].try_into().unwrap_or_default());
                let v = NaiveDate::from_num_days_from_ce_opt(n).map(Value::Date);
                values.push(if is_valid(i) {
                    v.unwrap_or(Value::Null)
                } else {
                    Value::Null
                });
            }
        }
        TAG_TIME => {
            let data = r.take(n_rows * 8)?;
            for i in 0..n_rows {
                let ns = i64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap_or_default());
                let v = nanos_to_time(ns).map(|value| Value::Time {
                    value,
                    offset_secs: None,
                });
                values.push(if is_valid(i) {
                    v.unwrap_or(Value::Null)
                } else {
                    Value::Null
                });
            }
        }
        TAG_UUID => {
            let data = r.take(n_rows * 16)?;
            for i in 0..n_rows {
                let bytes: [u8; 16] = data[i * 16..i * 16 + 16].try_into().unwrap_or_default();
                values.push(if is_valid(i) {
                    Value::Uuid(Uuid::from_bytes(bytes))
                } else {
                    Value::Null
                });
            }
        }
        TAG_TIMESTAMP => {
            let data = r.take(n_rows * 16)?;
            for i in 0..n_rows {
                let base = i * 16;
                let days = i32::from_le_bytes(data[base..base + 4].try_into().unwrap_or_default());
                let nanos =
                    i64::from_le_bytes(data[base + 4..base + 12].try_into().unwrap_or_default());
                let off =
                    i32::from_le_bytes(data[base + 12..base + 16].try_into().unwrap_or_default());

                let v = match (
                    NaiveDate::from_num_days_from_ce_opt(days),
                    nanos_to_time(nanos),
                ) {
                    (Some(d), Some(t)) => Some(Value::Timestamp {
                        value: d.and_time(t),
                        offset_secs: if off == i32::MIN { None } else { Some(off) },
                    }),
                    _ => None,
                };

                values.push(if is_valid(i) {
                    v.unwrap_or(Value::Null)
                } else {
                    Value::Null
                });
            }
        }
        TAG_STRING | TAG_BINARY | TAG_DECIMAL | TAG_JSON | TAG_CELL => {
            let cells = decode_varlen(r, n_rows)?;
            for (i, cell) in cells.into_iter().enumerate() {
                if !is_valid(i) {
                    values.push(Value::Null);
                    continue;
                }

                let v = match tag {
                    TAG_STRING => Value::String(String::from_utf8_lossy(cell).into_owned()),
                    TAG_BINARY => Value::Binary(cell.to_vec()),
                    TAG_DECIMAL => BigDecimal::from_str(&String::from_utf8_lossy(cell))
                        .map(Value::Decimal)
                        .map_err(|_| r.err("invalid decimal"))?,
                    TAG_JSON => Value::Json(
                        serde_json::from_slice(cell).map_err(|_| r.err("invalid json cell"))?,
                    ),
                    _ /* TAG_CELL */ => {
                        let json: serde_json::Value =
                            serde_json::from_slice(cell).map_err(|_| r.err("json cell parse"))?;
                        json_v1::json_to_value(&json, r.plugin)?
                    }
                };
                values.push(v);
            }
        }
        other => return Err(r.err(&format!("unknown column tag {other}"))),
    }
    Ok((name, values))
}

fn nanos_to_time(ns: i64) -> Option<NaiveTime> {
    let ns = u64::try_from(ns).ok()?;
    let secs = (ns / 1_000_000_000) as u32;
    let nanos = (ns % 1_000_000_000) as u32;
    NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
}

fn decode_varlen<'a>(r: &mut Reader<'a>, n_rows: usize) -> Result<Vec<&'a [u8]>, WasmError> {
    let offsets_bytes = r.take((n_rows + 1) * 4)?;
    let offsets: Vec<usize> = (0..=n_rows)
        .map(|i| {
            u32::from_le_bytes(
                offsets_bytes[i * 4..i * 4 + 4]
                    .try_into()
                    .unwrap_or_default(),
            ) as usize
        })
        .collect();

    let blob_len = *offsets.last().unwrap_or(&0);
    let blob = r.take(blob_len)?;

    let mut cells = Vec::with_capacity(n_rows);
    for i in 0..n_rows {
        let (start, end) = (offsets[i], offsets[i + 1]);
        let cell = blob
            .get(start..end)
            .ok_or_else(|| r.err("varlen offset out of range"))?;
        cells.push(cell);
    }

    Ok(cells)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_f64_column_roundtrips_via_batch() {
        let cols = vec![(
            "amount",
            vec![Value::Float(1.5), Value::Null, Value::Float(-3.0)],
        )];
        let bytes = encode_batch(3, &cols);
        // Magic must be first so guest_error() never misfires.
        assert_eq!(&bytes[0..4], &MAGIC.to_le_bytes());
        let (n, decoded) = decode_batch(&bytes, "t").unwrap();
        assert_eq!(n, 3);
        assert_eq!(decoded[0].0, "amount");
        assert_eq!(
            decoded[0].1,
            vec![Value::Float(1.5), Value::Null, Value::Float(-3.0)]
        );
    }

    #[test]
    fn mixed_and_exotic_fall_back_to_json_cells() {
        use bigdecimal::BigDecimal;
        use std::str::FromStr;
        let cols = vec![(
            "x",
            vec![
                Value::Int(7),
                Value::String("hi".into()),
                Value::Decimal(BigDecimal::from_str("1.25").unwrap()),
            ],
        )];
        let bytes = encode_batch(3, &cols);
        let (_, decoded) = decode_batch(&bytes, "t").unwrap();
        assert_eq!(decoded[0].1[0], Value::Int(7));
        assert_eq!(decoded[0].1[1], Value::String("hi".into()));
        assert_eq!(
            decoded[0].1[2],
            Value::Decimal(BigDecimal::from_str("1.25").unwrap())
        );
    }

    #[test]
    fn input_batch_then_output_batch_roundtrip() {
        let mut a = PluginInput::new();
        a.insert("amount".to_string(), Value::Float(10.0));
        a.insert("quantity".to_string(), Value::Float(2.0));
        let schema = vec![
            PluginField {
                name: "amount".into(),
                field_type: "f64".into(),
                nullable: false,
            },
            PluginField {
                name: "quantity".into(),
                field_type: "f64".into(),
                nullable: false,
            },
        ];
        let in_bytes = serialize_input_batch(&[a], &schema).unwrap();
        let (n, cols) = decode_batch(&in_bytes, "t").unwrap();
        assert_eq!(n, 1);
        assert_eq!(cols.len(), 2);

        // Simulate a guest output column and read it back through the public API.
        let out = encode_batch(1, &[("value", vec![Value::Float(20.0)])]);
        let outputs = deserialize_output_batch(&out, "t").unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].value, Value::Float(20.0));
    }

    #[test]
    fn filter_decisions_roundtrip() {
        let out = encode_batch(
            2,
            &[
                ("pass", vec![Value::Boolean(true), Value::Boolean(false)]),
                (
                    "reason",
                    vec![Value::Null, Value::String("too small".into())],
                ),
            ],
        );
        let decisions = deserialize_filter_decision_batch(&out, "t").unwrap();
        assert!(matches!(decisions[0], FilterDecision::Pass));
        assert!(
            matches!(&decisions[1], FilterDecision::Reject { reason } if reason == "too small")
        );
    }

    #[test]
    fn guest_error_envelope_detected() {
        let err = serde_json::to_vec(&serde_json::json!({"error": "boom"})).unwrap();
        assert_eq!(guest_error(&err).as_deref(), Some("boom"));
    }

    /// Encode a single column then decode it back; asserts the values survive.
    fn roundtrip_column(values: Vec<Value>) {
        let n = values.len();
        let bytes = encode_batch(n, &[("c", values.clone())]);
        let (rn, decoded) = decode_batch(&bytes, "t").unwrap();
        assert_eq!(rn, n);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1, values, "column did not round-trip");
    }

    #[test]
    fn every_native_type_roundtrips() {
        use bigdecimal::BigDecimal;
        use chrono::{NaiveDate, NaiveTime};
        use std::str::FromStr;

        roundtrip_column(vec![Value::Boolean(true), Value::Boolean(false)]);
        roundtrip_column(vec![
            Value::Int(i64::MIN),
            Value::Int(0),
            Value::Int(i64::MAX),
        ]);
        roundtrip_column(vec![Value::UInt(0), Value::UInt(u64::MAX)]);
        roundtrip_column(vec![Value::Float(f64::MIN), Value::Float(3.5)]);
        roundtrip_column(vec![
            Value::String("hello".into()),
            Value::String("héllo 🌍".into()),
        ]);
        roundtrip_column(vec![
            Value::Binary(vec![0, 1, 2, 255, 254]),
            Value::Binary(vec![]),
        ]);
        roundtrip_column(vec![
            Value::Date(NaiveDate::from_ymd_opt(2026, 8, 10).unwrap()),
            Value::Date(NaiveDate::from_ymd_opt(1, 1, 1).unwrap()),
        ]);
        roundtrip_column(vec![
            Value::Time {
                value: NaiveTime::from_hms_nano_opt(23, 59, 59, 123_456_789).unwrap(),
                offset_secs: None,
            },
            Value::Time {
                value: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                offset_secs: None,
            },
        ]);
        roundtrip_column(vec![Value::Uuid(uuid::Uuid::from_u128(
            0x1234_5678_9abc_def0_1122_3344_5566_7788,
        ))]);
        roundtrip_column(vec![
            Value::Decimal(BigDecimal::from_str("-12345.6789").unwrap()),
            Value::Decimal(BigDecimal::from_str("0").unwrap()),
        ]);
        roundtrip_column(vec![Value::Json(
            serde_json::json!({"a": [1, 2, 3], "b": null}),
        )]);
    }

    #[test]
    fn timestamp_roundtrips_with_and_without_offset() {
        use chrono::NaiveDate;
        let ndt = NaiveDate::from_ymd_opt(2026, 8, 10)
            .unwrap()
            .and_hms_opt(14, 30, 0)
            .unwrap();
        roundtrip_column(vec![
            Value::Timestamp {
                value: ndt,
                offset_secs: None,
            },
            Value::Timestamp {
                value: ndt,
                offset_secs: Some(3600),
            },
        ]);
    }

    #[test]
    fn nulls_across_every_column_kind() {
        use chrono::NaiveDate;
        // A null interleaved with each native tag exercises the validity bitmap
        // on both the fixed-width and varlen paths.
        roundtrip_column(vec![Value::Int(1), Value::Null, Value::Int(3)]);
        roundtrip_column(vec![Value::Float(1.0), Value::Null]);
        roundtrip_column(vec![
            Value::String("x".into()),
            Value::Null,
            Value::String("".into()),
        ]);
        roundtrip_column(vec![Value::Binary(vec![9]), Value::Null]);
        roundtrip_column(vec![
            Value::Date(NaiveDate::from_ymd_opt(2020, 2, 29).unwrap()),
            Value::Null,
        ]);
        roundtrip_column(vec![Value::Uuid(uuid::Uuid::nil()), Value::Null]);
    }

    #[test]
    fn all_null_and_empty_batches() {
        // A column that is entirely null decodes back to all-null.
        roundtrip_column(vec![Value::Null, Value::Null, Value::Null]);
        // Empty batch: zero rows, still a valid frame.
        let bytes = encode_batch(0, &[("c", Vec::<Value>::new())]);
        let (n, decoded) = decode_batch(&bytes, "t").unwrap();
        assert_eq!(n, 0);
        assert!(decoded[0].1.is_empty());
        // Batch with zero columns.
        let bytes = encode_batch(0, &[]);
        let (n, decoded) = decode_batch(&bytes, "t").unwrap();
        assert_eq!(n, 0);
        assert!(decoded.is_empty());
    }

    #[test]
    fn mixed_and_host_only_variants_use_cell_fallback() {
        use bigdecimal::BigDecimal;
        use std::str::FromStr;
        // Mixed scalar variants -> CELL.
        roundtrip_column(vec![Value::Int(7), Value::String("hi".into())]);
        // Decimal alongside Int (mixed) still round-trips via CELL.
        roundtrip_column(vec![
            Value::Int(1),
            Value::Decimal(BigDecimal::from_str("1.25").unwrap()),
        ]);
    }

    #[test]
    fn truncated_input_errors_not_panics() {
        let mut bytes = encode_batch(2, &[("c", vec![Value::Int(1), Value::Int(2)])]);
        bytes.truncate(bytes.len() - 3); // chop the data tail
        assert!(decode_batch(&bytes, "t").is_err());
        // Wrong magic is rejected too.
        assert!(decode_batch(&[0, 0, 0, 0, 0, 0, 0, 0], "t").is_err());
    }
}
