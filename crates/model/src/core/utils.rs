use std::fmt::Write;

/// Escape CSV per PostgreSQL COPY CSV rules:
/// - field is wrapped in double quotes
/// - internal `"` becomes `""`
/// - commas, newlines, tabs are safe because quoting protects them
pub fn escape_csv_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');

    for ch in s.chars() {
        if ch == '"' {
            out.push('"'); // double the quote
        }
        out.push(ch);
    }

    out.push('"');
    out
}

pub fn escape_copy_text(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\n' => escaped.push_str(r"\n"),
            '\r' => escaped.push_str(r"\r"),
            '\t' => escaped.push_str(r"\t"),
            '\\' => escaped.push_str(r"\\"),
            '\0' => escaped.push_str(r"\000"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

pub fn encode_bytea(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + 2 * bytes.len());
    out.push_str("\\x");
    for b in bytes {
        write!(&mut out, "{:02x}", b).unwrap();
    }
    out
}
