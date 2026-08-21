use super::record::{HASH_LEN, RecordReader, encode_raw};
use super::{StateStoreError, storage};
use crate::ticker::{PROGRESS_INTERVAL, Ticker};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::info;

const SORT_BUDGET_BYTES: usize = 64 * 1024 * 1024; // 64 MiB

/// Upper bound on chunks sorted concurrently.
const MAX_SORT_WORKERS: usize = 8;

/// Ceiling on sorted runs, so a very large table cannot turn into hundreds of
/// open files at merge time.
const MAX_RUNS: usize = 128;

/// Bytes of `items` bookkeeping per buffered record, used to charge the budget
/// for more than just the key and hash themselves.
const ITEM_OVERHEAD: usize = std::mem::size_of::<Item>();

/// Combine an input's rank with a record's stored order.
#[inline]
fn effective_order(rank: u32, stored: u64) -> u64 {
    ((rank as u64) << 32) | (stored & 0xffff_ffff)
}

/// One buffered record.
#[derive(Clone, Copy)]
struct Item {
    key_off: u32,
    key_len: u16,
    /// `(rank << 32) | seq` - later writes sort last within a key.
    order: u64,
    hash: [u8; HASH_LEN],
}

#[inline(always)]
fn get_key<'a>(keys: &'a [u8], item: &Item) -> &'a [u8] {
    let start = item.key_off as usize;
    &keys[start..start + item.key_len as usize]
}

/// Read the inputs in bounded chunks, sorting each into a run file.
pub(super) fn write_sorted_runs(
    inputs: &[(PathBuf, u32)],
    dir: &Path,
) -> Result<Vec<PathBuf>, StateStoreError> {
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(MAX_SORT_WORKERS);

    let total_bytes: u64 = inputs
        .iter()
        .filter_map(|(path, _)| fs::metadata(path).ok())
        .map(|meta| meta.len())
        .sum();

    let chunk_budget = (SORT_BUDGET_BYTES / workers)
        .max(total_bytes as usize / MAX_RUNS)
        .max(4 * 1024 * 1024);

    let mut reader = ChunkReader::new(inputs, chunk_budget);
    let mut runs: Vec<PathBuf> = Vec::new();

    let mut rows_sorted = 0u64;
    let mut ticker = Ticker::new(PROGRESS_INTERVAL);

    loop {
        // Fill one chunk per worker, then sort and write them concurrently.
        let mut chunks: Vec<Chunk> = Vec::with_capacity(workers);
        while chunks.len() < workers {
            match reader.next_chunk()? {
                Some(chunk) => {
                    rows_sorted += chunk.items.len() as u64;
                    chunks.push(chunk);
                }
                None => break,
            }
        }

        if ticker.report(rows_sorted) {
            info!(rows = rows_sorted, runs = runs.len(), "sorting row hashes");
        }

        if chunks.is_empty() {
            return Ok(runs);
        }

        let base = runs.len();

        let written: Vec<Result<PathBuf, StateStoreError>> = std::thread::scope(|scope| {
            let handles: Vec<_> = chunks
                .into_iter()
                .enumerate()
                .map(|(i, mut chunk)| scope.spawn(move || flush_run(&mut chunk, dir, base + i)))
                .collect();

            handles
                .into_iter()
                .map(|handle| {
                    handle.join().unwrap_or_else(|_| {
                        Err(StateStoreError::Storage("sort worker panicked".to_string()))
                    })
                })
                .collect()
        });

        for path in written {
            runs.push(path?);
        }
    }
}

/// One chunk of buffered records.
struct Chunk {
    keys: Vec<u8>,
    items: Vec<Item>,
}

/// Reads the seal's inputs end to end, handing back one budget-sized chunk at a time.
struct ChunkReader<'a> {
    inputs: &'a [(PathBuf, u32)],
    input_index: usize,
    reader: Option<RecordReader>,
    budget: usize,
}

impl<'a> ChunkReader<'a> {
    fn new(inputs: &'a [(PathBuf, u32)], budget: usize) -> Self {
        Self {
            inputs,
            input_index: 0,
            reader: None,
            budget,
        }
    }

    fn next_chunk(&mut self) -> Result<Option<Chunk>, StateStoreError> {
        let mut keys: Vec<u8> = Vec::with_capacity(self.budget / 4);
        let mut items: Vec<Item> = Vec::new();
        let mut charged = 0usize;

        while let Some((path, rank)) = self.inputs.get(self.input_index) {
            let reader = match &mut self.reader {
                Some(reader) => reader,
                None => self.reader.insert(RecordReader::open(path)?),
            };

            match reader.read_onto(&mut keys)? {
                Some((order, hash, key_len)) => {
                    let key_off = (keys.len() - key_len) as u32;
                    items.push(Item {
                        key_off,
                        key_len: key_len as u16,
                        order: effective_order(*rank, order),
                        hash,
                    });

                    charged += key_len + ITEM_OVERHEAD;
                    if charged >= self.budget {
                        return Ok(Some(Chunk { keys, items }));
                    }
                }
                None => {
                    self.reader = None;
                    self.input_index += 1;
                }
            }
        }

        if items.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Chunk { keys, items }))
        }
    }
}

/// Sort one chunk and write it out, then reclaim its buffers.
fn flush_run(chunk: &mut Chunk, dir: &Path, index: usize) -> Result<PathBuf, StateStoreError> {
    let Chunk { keys, items } = chunk;
    items.sort_unstable_by(|a, b| {
        get_key(keys, a)
            .cmp(get_key(keys, b))
            .then_with(|| b.order.cmp(&a.order))
    });

    items.dedup_by(|a, b| get_key(keys, a) == get_key(keys, b));

    let path = dir.join(format!("run-{index:03}.tmp"));
    let mut out = BufWriter::with_capacity(256 * 1024, File::create(&path).map_err(storage)?);
    let mut buf = Vec::with_capacity(128);

    for item in items.iter() {
        buf.clear();
        encode_raw(get_key(keys, item), item.order, &item.hash, &mut buf)?;
        out.write_all(&buf).map_err(storage)?;
    }

    out.flush().map_err(storage)?;
    Ok(path)
}
