use crate::sql::metadata::column::ColumnMetadata;
use bigdecimal::BigDecimal;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use model::core::value::Value;
use std::{fmt, str::FromStr};

/// The value could not be encoded in binary for its target column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinaryEncodeError;

impl fmt::Display for BinaryEncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Failed to encode value to PostgreSQL binary format")
    }
}

impl std::error::Error for BinaryEncodeError {}

/// PGCOPY signature: `PGCOPY\n\xff\r\n\0`.
const SIGNATURE: [u8; 11] = [
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00,
];

const PG_EPOCH_DATE: NaiveDate = match NaiveDate::from_ymd_opt(2000, 1, 1) {
    Some(d) => d,
    None => unreachable!(),
};
const PG_EPOCH_DT: NaiveDateTime = match PG_EPOCH_DATE.and_hms_opt(0, 0, 0) {
    Some(dt) => dt,
    None => unreachable!(),
};
const MIDNIGHT: NaiveTime = match NaiveTime::from_hms_opt(0, 0, 0) {
    Some(t) => t,
    None => unreachable!(),
};

/// Appends scalars in PostgreSQL binary wire order (big-endian) to a byte buffer.
trait PgBufferExt {
    fn put_i16(&mut self, v: i16);
    fn put_i32(&mut self, v: i32);
    fn put_i64(&mut self, v: i64);
    fn put_u16(&mut self, v: u16);
    fn put_f32(&mut self, v: f32);
    fn put_f64(&mut self, v: f64);
}

impl PgBufferExt for Vec<u8> {
    fn put_i16(&mut self, v: i16) {
        self.extend_from_slice(&v.to_be_bytes());
    }
    fn put_i32(&mut self, v: i32) {
        self.extend_from_slice(&v.to_be_bytes());
    }
    fn put_i64(&mut self, v: i64) {
        self.extend_from_slice(&v.to_be_bytes());
    }
    fn put_u16(&mut self, v: u16) {
        self.extend_from_slice(&v.to_be_bytes());
    }
    fn put_f32(&mut self, v: f32) {
        self.extend_from_slice(&v.to_be_bytes());
    }
    fn put_f64(&mut self, v: f64) {
        self.extend_from_slice(&v.to_be_bytes());
    }
}

/// The binary layout a destination column expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryColumnType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    /// `text`/`varchar`/`char`/`name`/enum - sent as raw UTF-8 bytes.
    Text,
    /// `json` - sent as raw UTF-8 bytes (same as text on the wire).
    Json,
    /// `jsonb` - a `0x01` version byte followed by the UTF-8 JSON text.
    Jsonb,
    Bytea,
    Uuid,
    Date,
    Time,
    Timestamp,
    TimestampTz,
}

impl BinaryColumnType {
    /// Classify a destination column's PG type. Returns `None` for any type we do
    /// not binary-encode, which keeps the table on the CSV fallback.
    pub fn classify(col: &ColumnMetadata) -> Option<Self> {
        use BinaryColumnType::*;

        // Enum columns carry the enum *type name* as `data_type`; their binary
        // representation is just the label text.
        if col.is_enum() {
            return Some(Text);
        }

        let dt = col.data_type.trim().to_ascii_lowercase();

        // Arrays (`integer[]`, or the internal `_int4` spelling) have their own
        // container format we don't emit - fall back to CSV.
        if dt.ends_with("[]") || dt.starts_with('_') {
            return None;
        }

        // Temporal types can carry a typmod *inside* the name
        // (`timestamp(6) without time zone`), so match on substrings before the
        // generic `(` typmod stripping below.
        if dt.starts_with("timestamptz") {
            return Some(TimestampTz);
        }
        if dt.starts_with("timestamp") {
            return Some(if dt.contains("with time zone") {
                TimestampTz
            } else {
                Timestamp
            });
        }
        if dt.starts_with("timetz") {
            return None; // time with time zone: uncommon, fall back to CSV
        }
        if dt.starts_with("time") {
            return if dt.contains("with time zone") {
                None
            } else {
                Some(Time)
            };
        }

        // Strip any `(...)` typmod for the remaining fixed-name types.
        let base = dt.split('(').next().unwrap_or(&dt).trim();
        Some(match base {
            "boolean" | "bool" => Bool,
            "smallint" | "int2" => Int2,
            "integer" | "int" | "int4" => Int4,
            "bigint" | "int8" => Int8,
            "real" | "float4" => Float4,
            "double precision" | "float8" => Float8,
            "numeric" | "decimal" => Numeric,
            "text" | "character varying" | "varchar" | "character" | "char" | "bpchar" | "name"
            | "citext" => Text,
            "uuid" => Uuid,
            "bytea" => Bytea,
            "json" => Json,
            "jsonb" => Jsonb,
            "date" => Date,
            _ => return None,
        })
    }
}

/// Encoder for the PostgreSQL binary COPY stream.
pub struct PgBinaryEncoder;

impl PgBinaryEncoder {
    /// Write the fixed file header (signature + flags + header extension length).
    pub fn write_header(out: &mut Vec<u8>) {
        out.extend_from_slice(&SIGNATURE);
        out.put_i32(0); // flags (no OIDs)
        out.put_i32(0); // header extension area length
    }

    /// Write the end-of-data trailer (`-1` field count).
    pub fn write_trailer(out: &mut Vec<u8>) {
        out.put_i16(-1);
    }

    /// Begin a tuple by writing its field count.
    pub fn begin_row(out: &mut Vec<u8>, num_fields: usize) {
        out.put_i16(num_fields as i16);
    }

    /// Write a NULL field (`-1` length, no body).
    pub fn write_null(out: &mut Vec<u8>) {
        out.put_i32(-1);
    }

    /// Write one field: a 4-byte length prefix followed by the value's binary
    /// body. The length is back-patched after the body is written, so no scratch
    /// allocation is needed per value.
    pub fn write_field(
        &self,
        col: BinaryColumnType,
        value: &Value,
        out: &mut Vec<u8>,
    ) -> Result<(), BinaryEncodeError> {
        if matches!(value, Value::Null) {
            Self::write_null(out);
            return Ok(());
        }

        let len_pos = out.len();
        out.extend_from_slice(&[0u8; 4]); // length placeholder

        Self::write_body(col, value, out)?;

        let body_len = (out.len() - len_pos - 4) as i32;
        out[len_pos..len_pos + 4].copy_from_slice(&body_len.to_be_bytes());

        Ok(())
    }

    fn write_body(
        col: BinaryColumnType,
        value: &Value,
        out: &mut Vec<u8>,
    ) -> Result<(), BinaryEncodeError> {
        use BinaryColumnType::*;
        match col {
            Bool => out.push(as_bool(value)? as u8),
            Int2 => {
                let v = i16::try_from(as_i64(value)?).map_err(|_| BinaryEncodeError)?;
                out.put_i16(v);
            }
            Int4 => {
                let v = i32::try_from(as_i64(value)?).map_err(|_| BinaryEncodeError)?;
                out.put_i32(v);
            }
            Int8 => out.put_i64(as_i64(value)?),
            Float4 => out.put_f32(as_f64(value)? as f32),
            Float8 => out.put_f64(as_f64(value)?),
            Numeric => write_numeric(&as_decimal(value)?, out),
            Text => out.extend_from_slice(as_text(value)?.as_bytes()),
            Json => write_json_text(value, out)?,
            Jsonb => {
                out.push(1); // jsonb version header
                write_json_text(value, out)?;
            }
            Bytea => match value {
                Value::Binary(b) => out.extend_from_slice(b),
                _ => return Err(BinaryEncodeError),
            },
            Uuid => match value {
                Value::Uuid(u) => out.extend_from_slice(u.as_bytes()),
                _ => return Err(BinaryEncodeError),
            },
            Date => match value {
                Value::Date(d) => {
                    let days = (*d - PG_EPOCH_DATE).num_days();
                    let days = i32::try_from(days).map_err(|_| BinaryEncodeError)?;
                    out.put_i32(days);
                }
                _ => return Err(BinaryEncodeError),
            },
            Time => match value {
                Value::Time { value: t, .. } => {
                    let micros = t
                        .signed_duration_since(MIDNIGHT)
                        .num_microseconds()
                        .ok_or(BinaryEncodeError)?;
                    out.put_i64(micros);
                }
                _ => return Err(BinaryEncodeError),
            },
            Timestamp => match value {
                Value::Timestamp { value: ts, .. } => out.put_i64(timestamp_micros(*ts)?),
                _ => return Err(BinaryEncodeError),
            },
            TimestampTz => match value {
                Value::Timestamp {
                    value: ts,
                    offset_secs,
                } => {
                    // timestamptz is stored as UTC microseconds from the PG epoch.
                    let utc = match offset_secs {
                        Some(off) => ts
                            .checked_sub_signed(Duration::seconds(*off as i64))
                            .unwrap_or(*ts),
                        None => *ts,
                    };
                    out.put_i64(timestamp_micros(utc)?);
                }
                _ => return Err(BinaryEncodeError),
            },
        }
        Ok(())
    }
}

#[inline]
fn timestamp_micros(ts: NaiveDateTime) -> Result<i64, BinaryEncodeError> {
    (ts - PG_EPOCH_DT)
        .num_microseconds()
        .ok_or(BinaryEncodeError)
}

fn as_bool(value: &Value) -> Result<bool, BinaryEncodeError> {
    Ok(match value {
        Value::Boolean(b) => *b,
        Value::Int(n) => *n != 0,
        Value::UInt(n) => *n != 0,
        _ => return Err(BinaryEncodeError),
    })
}

fn as_i64(value: &Value) -> Result<i64, BinaryEncodeError> {
    Ok(match value {
        Value::Int(n) => *n,
        Value::UInt(n) => i64::try_from(*n).map_err(|_| BinaryEncodeError)?,
        Value::Year(y) => *y as i64,
        Value::Boolean(b) => *b as i64,
        _ => return Err(BinaryEncodeError),
    })
}

fn as_f64(value: &Value) -> Result<f64, BinaryEncodeError> {
    Ok(match value {
        Value::Float(f) => *f,
        Value::Int(n) => *n as f64,
        Value::UInt(n) => *n as f64,
        Value::Decimal(d) => d.to_string().parse().map_err(|_| BinaryEncodeError)?,
        _ => return Err(BinaryEncodeError),
    })
}

fn as_text(value: &Value) -> Result<&str, BinaryEncodeError> {
    match value {
        Value::String(s) => Ok(s),
        Value::Enum { value, .. } => Ok(value),
        _ => Err(BinaryEncodeError),
    }
}

fn as_decimal(value: &Value) -> Result<BigDecimal, BinaryEncodeError> {
    Ok(match value {
        Value::Decimal(d) => d.clone(),
        Value::Int(n) => BigDecimal::from(*n),
        Value::UInt(n) => BigDecimal::from_str(&n.to_string()).map_err(|_| BinaryEncodeError)?,
        Value::Float(f) => BigDecimal::from_str(&f.to_string()).map_err(|_| BinaryEncodeError)?,
        _ => return Err(BinaryEncodeError),
    })
}

fn write_json_text(value: &Value, out: &mut Vec<u8>) -> Result<(), BinaryEncodeError> {
    match value {
        Value::Json(j) => out.extend_from_slice(j.to_string().as_bytes()),
        Value::String(s) => out.extend_from_slice(s.as_bytes()),
        _ => return Err(BinaryEncodeError),
    }
    Ok(())
}

/// Encode a decimal into PostgreSQL's `numeric` binary body:
/// `int16 ndigits, int16 weight, int16 sign, int16 dscale, int16[ndigits]`,
/// where each digit is a base-10000 group, most significant first.
fn write_numeric(d: &BigDecimal, out: &mut Vec<u8>) {
    const NUMERIC_POS: u16 = 0x0000;
    const NUMERIC_NEG: u16 = 0x4000;

    // value = unscaled * 10^-exp
    let (unscaled, exp) = d.as_bigint_and_exponent();
    let s = unscaled.to_string();
    let negative = s.starts_with('-');
    let digits = s.trim_start_matches('-');
    let dscale = exp.max(0).min(i16::MAX as i64) as u16;

    // Split the decimal digit string around the implied point into an integer
    // part and a fractional part.
    let (int_digits, frac_digits): (String, String) = if exp <= 0 {
        // integer value with `-exp` implicit trailing zeros
        let mut int = String::from(digits);
        int.extend(std::iter::repeat_n('0', (-exp) as usize));
        (int, String::new())
    } else {
        let e = exp as usize;
        if e >= digits.len() {
            let mut frac = String::with_capacity(e);
            frac.extend(std::iter::repeat_n('0', e - digits.len()));
            frac.push_str(digits);
            (String::new(), frac)
        } else {
            let split = digits.len() - e;
            (digits[..split].to_string(), digits[split..].to_string())
        }
    };

    let int_groups = nbase_groups(&int_digits, true);
    let frac_groups = nbase_groups(&frac_digits, false);

    // Weight of the most significant group: the top integer group sits at
    // 10000^(num_int_groups-1); with no integer groups the first (fractional)
    // group is at 10000^-1.
    let mut weight = int_groups.len() as i32 - 1;
    let mut all: Vec<u16> = int_groups;
    all.extend(frac_groups);

    // Trim leading zero groups (each drop lowers the leading weight by one).
    while all.len() > 1 && all[0] == 0 {
        all.remove(0);
        weight -= 1;
    }
    // A zero value has no digits and weight 0.
    if all.iter().all(|&g| g == 0) {
        all.clear();
        weight = 0;
    }
    // Trim trailing zero groups (weight is unaffected).
    while all.last() == Some(&0) {
        all.pop();
    }

    let sign = if negative && !all.is_empty() {
        NUMERIC_NEG
    } else {
        NUMERIC_POS
    };

    out.put_i16(all.len() as i16);
    out.put_i16(weight as i16);
    out.put_u16(sign);
    out.put_u16(dscale);

    for g in all {
        out.put_u16(g);
    }
}

/// Chunk a decimal digit string into base-10000 groups. Integer parts pad on the
/// left (aligning the least-significant group to the point); fractional parts pad
/// on the right.
fn nbase_groups(digits: &str, pad_left: bool) -> Vec<u16> {
    if digits.is_empty() {
        return Vec::new();
    }

    let pad = (4 - digits.len() % 4) % 4;
    let mut padded = String::with_capacity(digits.len() + pad);

    if pad_left {
        padded.extend(std::iter::repeat_n('0', pad));
        padded.push_str(digits);
    } else {
        padded.push_str(digits);
        padded.extend(std::iter::repeat_n('0', pad));
    }

    padded
        .as_bytes()
        .chunks(4)
        .map(|c| {
            c.iter()
                .fold(0u16, |acc, &b| acc * 10 + u16::from(b.saturating_sub(b'0')))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(data_type: &str) -> ColumnMetadata {
        ColumnMetadata {
            data_type: data_type.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn classify_common_types() {
        use BinaryColumnType::*;
        assert_eq!(BinaryColumnType::classify(&col("integer")), Some(Int4));
        assert_eq!(BinaryColumnType::classify(&col("bigint")), Some(Int8));
        assert_eq!(BinaryColumnType::classify(&col("smallint")), Some(Int2));
        assert_eq!(BinaryColumnType::classify(&col("boolean")), Some(Bool));
        assert_eq!(
            BinaryColumnType::classify(&col("character varying(255)")),
            Some(Text)
        );
        assert_eq!(
            BinaryColumnType::classify(&col("numeric(12,2)")),
            Some(Numeric)
        );
        assert_eq!(BinaryColumnType::classify(&col("uuid")), Some(Uuid));
        assert_eq!(BinaryColumnType::classify(&col("date")), Some(Date));
    }

    #[test]
    fn classify_timestamp_tz_variants() {
        use BinaryColumnType::*;
        assert_eq!(
            BinaryColumnType::classify(&col("timestamp without time zone")),
            Some(Timestamp)
        );
        assert_eq!(
            BinaryColumnType::classify(&col("timestamp(6) without time zone")),
            Some(Timestamp)
        );
        assert_eq!(
            BinaryColumnType::classify(&col("timestamp with time zone")),
            Some(TimestampTz)
        );
        assert_eq!(
            BinaryColumnType::classify(&col("time without time zone")),
            Some(Time)
        );
        // time with time zone is not binary-encoded here
        assert_eq!(
            BinaryColumnType::classify(&col("time with time zone")),
            None
        );
    }

    #[test]
    fn classify_unsupported_falls_back() {
        assert_eq!(BinaryColumnType::classify(&col("integer[]")), None);
        assert_eq!(BinaryColumnType::classify(&col("inet")), None);
        assert_eq!(BinaryColumnType::classify(&col("point")), None);
    }

    #[test]
    fn classify_enum_is_text() {
        let mut c = col("mpaa_rating");
        c.full_column_type = Some("enum('G','PG','R')".to_string());
        assert_eq!(BinaryColumnType::classify(&c), Some(BinaryColumnType::Text));
    }

    fn numeric_body(s: &str) -> Vec<u8> {
        let mut out = Vec::new();
        write_numeric(&BigDecimal::from_str(s).unwrap(), &mut out);
        out
    }

    fn parse_numeric(body: &[u8]) -> (i16, i16, u16, u16, Vec<u16>) {
        let ndigits = i16::from_be_bytes([body[0], body[1]]);
        let weight = i16::from_be_bytes([body[2], body[3]]);
        let sign = u16::from_be_bytes([body[4], body[5]]);
        let dscale = u16::from_be_bytes([body[6], body[7]]);
        let digits = (0..ndigits as usize)
            .map(|i| u16::from_be_bytes([body[8 + i * 2], body[9 + i * 2]]))
            .collect();
        (ndigits, weight, sign, dscale, digits)
    }

    #[test]
    fn numeric_zero() {
        let (ndigits, weight, sign, dscale, digits) = parse_numeric(&numeric_body("0"));
        assert_eq!((ndigits, weight, sign, dscale), (0, 0, 0x0000, 0));
        assert!(digits.is_empty());
    }

    #[test]
    fn numeric_one_point_five() {
        // 1.50 -> digits [1, 5000], weight 0, dscale 2
        let (ndigits, weight, sign, dscale, digits) = parse_numeric(&numeric_body("1.50"));
        assert_eq!((ndigits, weight, sign, dscale), (2, 0, 0x0000, 2));
        assert_eq!(digits, vec![1, 5000]);
    }

    #[test]
    fn numeric_large_with_fraction() {
        // 12345.678 -> [1, 2345, 6780], weight 1, dscale 3
        let (ndigits, weight, sign, dscale, digits) = parse_numeric(&numeric_body("12345.678"));
        assert_eq!((ndigits, weight, sign, dscale), (3, 1, 0x0000, 3));
        assert_eq!(digits, vec![1, 2345, 6780]);
    }

    #[test]
    fn numeric_small_fraction_and_negative() {
        // 0.001 -> [10] wait: 0.001 = 10 * 10000^-1? no. checked below
        let (ndigits, weight, _sign, dscale, digits) = parse_numeric(&numeric_body("0.001"));
        // 0.001 -> single group 10 at weight -1 (0.0010)
        assert_eq!((ndigits, weight, dscale), (1, -1, 3));
        assert_eq!(digits, vec![10]);

        let (_n, _w, sign, _d, _digits) = parse_numeric(&numeric_body("-42.5"));
        assert_eq!(sign, 0x4000);
    }

    #[test]
    fn header_signature_is_correct() {
        let mut out = Vec::new();
        PgBinaryEncoder::write_header(&mut out);
        assert_eq!(&out[..11], b"PGCOPY\n\xff\r\n\0");
        assert_eq!(&out[11..15], &0i32.to_be_bytes()); // flags
        assert_eq!(&out[15..19], &0i32.to_be_bytes()); // header ext
    }

    #[test]
    fn field_length_is_backpatched() {
        let enc = PgBinaryEncoder;
        let mut out = Vec::new();
        enc.write_field(BinaryColumnType::Int4, &Value::Int(1), &mut out)
            .unwrap();
        // 4-byte length prefix of 4, then the big-endian int
        assert_eq!(&out[..4], &4i32.to_be_bytes());
        assert_eq!(&out[4..8], &1i32.to_be_bytes());

        // NULL is a -1 length and no body
        let mut nout = Vec::new();
        enc.write_field(BinaryColumnType::Int4, &Value::Null, &mut nout)
            .unwrap();
        assert_eq!(nout, (-1i32).to_be_bytes());
    }

    #[test]
    fn int_out_of_range_errors() {
        let enc = PgBinaryEncoder;
        let mut out = Vec::new();
        // 100000 does not fit smallint
        assert!(
            enc.write_field(BinaryColumnType::Int2, &Value::Int(100_000), &mut out)
                .is_err()
        );
    }
}
