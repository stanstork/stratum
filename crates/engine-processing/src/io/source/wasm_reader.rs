use crate::io::source::reader::SourceReader;
use async_trait::async_trait;
use connectors::error::DriverError;
use engine_wasm::runtime::instance::PluginInstance;
use model::pagination::{cursor::Cursor, page::FetchResult};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

pub struct WasmSourceReader {
    plugin: Arc<Mutex<PluginInstance>>,
    plugin_name: String,
    table: String,
}

impl WasmSourceReader {
    pub fn new(plugin: PluginInstance, table: impl Into<String>) -> Self {
        let plugin_name = plugin.plugin_name().to_string();
        Self {
            plugin: Arc::new(Mutex::new(plugin)),
            plugin_name,
            table: table.into(),
        }
    }
}

#[async_trait]
impl SourceReader for WasmSourceReader {
    async fn fetch(&self, batch_size: usize, cursor: Cursor) -> Result<FetchResult, DriverError> {
        let cursor_str = match &cursor {
            Cursor::None => None,
            Cursor::Opaque(s) => Some(s.as_str()),
            other => {
                return Err(DriverError::QueryError(format!(
                    "wasm source '{}' received non-opaque cursor: {:?}",
                    self.plugin_name, other
                )));
            }
        };

        let started = Instant::now();
        let page = {
            let mut guard = self
                .plugin
                .lock()
                .expect("wasm source plugin mutex poisoned");
            guard.call_read_page(cursor_str, batch_size).map_err(|e| {
                DriverError::QueryError(format!(
                    "wasm source '{}' read_page failed: {}",
                    self.plugin_name, e
                ))
            })?
        };
        let took_ms = started.elapsed().as_millis();

        let row_count = page.records.len();
        let next_cursor = page.next_cursor.map(Cursor::Opaque);
        let reached_end = !page.has_more;

        // Stamp records with the source table so entity-keyed transforms match.
        let mut rows = page.records;
        for row in &mut rows {
            row.schema = self.table.clone();
        }

        Ok(FetchResult {
            rows,
            next_cursor,
            reached_end,
            row_count,
            took_ms,
        })
    }
}
