//! Guest half of the `columnar_v1` binary wire format.
//!
//! This mirrors `engine-wasm/src/exchange/columnar_v1.rs` byte-for-byte - the
//! two MUST stay in lockstep (tags, layout, encodings). See that file for the
//! layout spec. The host writes input columns / reads output columns; the guest
//! reads input columns / writes output columns, all through the same
//! `encode_batch`/`decode_batch` codec.

use crate::{
    error::{PluginError, PluginResult},
    exchange::json_v1,
    filter::FilterDecision,
    input::PluginInput,
    value::Value,
};
use bigdecimal::BigDecimal;
use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};
use std::str::FromStr;
use uuid::Uuid;

const MAGIC: u32 = 0xC0_1A_C0_01;

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

/// Decode the host's input columns into one `PluginInput` per row.
pub fn decode_input_batch(bytes: &[u8]) -> PluginResult<Vec<PluginInput>> {
    let (n_rows, columns) = decode_batch(bytes)?;
    let mut rows = Vec::with_capacity(n_rows);

    for i in 0..n_rows {
        let mut input = PluginInput::new();
        for (name, values) in &columns {
            if let Some(v) = values.get(i) {
                input.insert(name.clone(), v.clone());
            }
        }
        rows.push(input);
    }

    Ok(rows)
}

/// Encode transform outputs as a single-column batch (`value`).
pub fn encode_output_batch(values: &[Value]) -> Vec<u8> {
    encode_batch(values.len(), &[("value", values.to_vec())])
}

/// Encode filter decisions as a two-column batch (`pass`: bool, `reason`:
/// string, null where the row passed).
pub fn encode_filter_batch(decisions: &[FilterDecision]) -> Vec<u8> {
    let n = decisions.len();
    let mut pass = Vec::with_capacity(n);
    let mut reason = Vec::with_capacity(n);

    for d in decisions {
        match d {
            FilterDecision::Pass => {
                pass.push(Value::Boolean(true));
                reason.push(Value::Null);
            }
            FilterDecision::Reject { reason: r } => {
                pass.push(Value::Boolean(false));
                reason.push(Value::String(r.clone()));
            }
        }
    }

    encode_batch(n, &[("pass", pass), ("reason", reason)])
}

// Symmetric codec - identical layout to the host.

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
}

impl<'a> Reader<'a> {
    fn err(&self, what: &str) -> PluginError {
        PluginError::invalid_input(format!("columnar_v1: {what}"))
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], PluginError> {
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

    fn u32(&mut self) -> Result<u32, PluginError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u8(&mut self) -> Result<u8, PluginError> {
        Ok(self.take(1)?[0])
    }
}

fn decode_batch(bytes: &[u8]) -> PluginResult<DecodedBatch> {
    let mut r = Reader { bytes, pos: 0 };

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

fn decode_column(r: &mut Reader<'_>, n_rows: usize) -> PluginResult<(String, Vec<Value>)> {
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
                let v = nanos_to_time(ns).map(|value| Value::Time { value });
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
                        json_v1::json_to_value(&json)?
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

fn decode_varlen<'a>(r: &mut Reader<'a>, n_rows: usize) -> PluginResult<Vec<&'a [u8]>> {
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
