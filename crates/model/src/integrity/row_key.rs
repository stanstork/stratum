use crate::integrity::canonical::describe_key;

/// One migrated row, reduced to the pair verification actually compares:
/// an order-independent key and the hash of the row's contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyedRowHash {
    /// Canonical byte encoding of the row's key columns.
    pub key: Vec<u8>,
    /// Hash of the row's canonical column encoding.
    pub hash: [u8; 32],
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
