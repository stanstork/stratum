use super::{StateStoreError, storage};
use model::integrity::row_key::KeyedRowHash;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

pub(super) const META_LEN: usize = 2 + 8 + HASH_LEN;
pub(super) const HASH_LEN: usize = 32;

pub(super) struct RecordReader {
    inner: BufReader<File>,
}

impl RecordReader {
    pub(super) fn open(path: &Path) -> Result<Self, StateStoreError> {
        let file = File::open(path).map_err(storage)?;
        Ok(Self {
            inner: BufReader::with_capacity(256 * 1024, file),
        })
    }

    fn read_meta(&mut self) -> Result<Option<(u64, [u8; HASH_LEN], usize)>, StateStoreError> {
        let mut meta = [0u8; META_LEN];
        match self.inner.read_exact(&mut meta) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(storage(e)),
        }

        // Fixed-width fields out of a fixed-size buffer using infallible try_into slices
        let key_len = u16::from_le_bytes([meta[0], meta[1]]) as usize;
        let order = u64::from_le_bytes([
            meta[2], meta[3], meta[4], meta[5], meta[6], meta[7], meta[8], meta[9],
        ]);

        let mut hash = [0u8; HASH_LEN];
        hash.copy_from_slice(&meta[META_LEN - HASH_LEN..]);

        Ok(Some((order, hash, key_len)))
    }

    /// Appends the key directly to a shared arena, returning metadata.
    pub(super) fn read_onto(
        &mut self,
        arena: &mut Vec<u8>,
    ) -> Result<Option<(u64, [u8; HASH_LEN], usize)>, StateStoreError> {
        if let Some((order, hash, key_len)) = self.read_meta()? {
            let start = arena.len();
            arena.resize(start + key_len, 0);
            self.inner
                .read_exact(&mut arena[start..])
                .map_err(storage)?;
            Ok(Some((order, hash, key_len)))
        } else {
            Ok(None)
        }
    }

    /// Reads the key into an existing reused buffer, returning metadata.
    pub(super) fn read_into(
        &mut self,
        key_buf: &mut Vec<u8>,
    ) -> Result<Option<(u64, [u8; HASH_LEN])>, StateStoreError> {
        if let Some((order, hash, key_len)) = self.read_meta()? {
            key_buf.resize(key_len, 0);
            self.inner.read_exact(key_buf).map_err(storage)?;
            Ok(Some((order, hash)))
        } else {
            Ok(None)
        }
    }
}

/// Streaming a sealed set yields keys and hashes; `order` has already served its
/// purpose by the time a set is sealed, so it is dropped here.
impl Iterator for RecordReader {
    type Item = Result<KeyedRowHash, StateStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut key = Vec::new();
        match self.read_into(&mut key) {
            Ok(Some((_, hash))) => Some(Ok(KeyedRowHash { key, hash })),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub(super) fn encode(
    entry: &KeyedRowHash,
    order: u64,
    out: &mut Vec<u8>,
) -> Result<(), StateStoreError> {
    encode_raw(&entry.key, order, &entry.hash, out)
}

pub(super) fn encode_raw(
    key: &[u8],
    order: u64,
    hash: &[u8; HASH_LEN],
    out: &mut Vec<u8>,
) -> Result<(), StateStoreError> {
    let key_len = u16::try_from(key.len()).map_err(|_| {
        StateStoreError::Serialization(format!(
            "row key is {} bytes, which exceeds the {} byte limit",
            key.len(),
            u16::MAX
        ))
    })?;

    out.reserve(42 + key.len());
    out.extend_from_slice(&key_len.to_le_bytes());
    out.extend_from_slice(&order.to_le_bytes());
    out.extend_from_slice(hash);
    out.extend_from_slice(key);

    Ok(())
}
