use super::record::{HASH_LEN, RecordReader, encode_raw};
use super::{StateStoreError, storage};
use crate::ticker::{PROGRESS_INTERVAL, Ticker};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::info;

/// Records between clock reads while merging - this loop runs once per record
/// across the whole set.
const CLOCK_SAMPLE: u64 = 1 << 16;

/// K-way merge of sorted runs into one deduplicated output.
pub(super) fn merge_runs(runs: &[PathBuf], out_path: &Path) -> Result<(), StateStoreError> {
    let mut readers: Vec<RecordReader> = runs
        .iter()
        .map(|p| RecordReader::open(p))
        .collect::<Result<_, _>>()?;

    let mut heap = BinaryHeap::with_capacity(readers.len());

    for (run, reader) in readers.iter_mut().enumerate() {
        let mut key = Vec::new();
        if let Some((order, hash)) = reader.read_into(&mut key)? {
            heap.push(Reverse(HeapItem {
                key,
                order,
                hash,
                run,
            }));
        }
    }

    let mut out = BufWriter::with_capacity(256 * 1024, File::create(out_path).map_err(storage)?);
    let mut buf = Vec::with_capacity(128);
    let mut last_key = Vec::new();

    let mut merged = 0u64;
    let mut ticker = Ticker::new(PROGRESS_INTERVAL).sampling(CLOCK_SAMPLE);

    while let Some(Reverse(mut item)) = heap.pop() {
        merged += 1;
        if ticker.report(merged) {
            info!(
                records = merged,
                runs = readers.len(),
                "merging sorted runs"
            );
        }

        if last_key != item.key {
            buf.clear();
            encode_raw(&item.key, item.order, &item.hash, &mut buf)?;
            out.write_all(&buf).map_err(storage)?;

            // Retain this key to deduplicate future pops and reuse allocations
            std::mem::swap(&mut last_key, &mut item.key);
        }

        item.key.clear();
        if let Some((order, hash)) = readers[item.run].read_into(&mut item.key)? {
            item.order = order;
            item.hash = hash;
            heap.push(Reverse(item));
        }
    }

    out.flush().map_err(storage)
}

/// A record in flight through the merge heap, ordered by `(key, order)` alone.
struct HeapItem {
    key: Vec<u8>,
    order: u64,
    hash: [u8; HASH_LEN],
    run: usize,
}

impl HeapItem {
    fn sort_key(&self) -> (&[u8], u64) {
        (&self.key, self.order)
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| other.order.cmp(&self.order))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key() == other.sort_key()
    }
}

impl Eq for HeapItem {}
