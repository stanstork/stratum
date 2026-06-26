//! A minimal source plugin that emits a fixed range of synthetic rows.
//!
//! Config (JSON, host-provided at init time):
//! ```json
//! { "total": "10", "page_size": "3" }
//! ```
//!
//! Behavior: each call to `__stratum_read_page` returns up to `page_size` rows
//! starting at the cursor offset. The cursor is the next offset encoded as a
//! decimal string; `None` means "start from 0". `has_more` is `false` on the
//! final page so the host stops paging.

use stratum_plugin_sdk::{
    PluginResult, Record, SourcePage, source_config, stratum_source,
};

#[stratum_source(
    name = "test_source",
    version = "1.0.0",
    output_schema = [
        { name = "id", type = "i64", nullable = false },
        { name = "label", type = "string", nullable = false },
    ]
)]
fn read_page(cursor: Option<String>) -> PluginResult<SourcePage> {
    let cfg = source_config()?;
    let total: i64 = cfg.get("total").and_then(|s| s.parse().ok()).unwrap_or(10);
    let page_size: i64 = cfg.get("page_size").and_then(|s| s.parse().ok()).unwrap_or(3);

    let start: i64 = cursor
        .as_deref()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let end = (start + page_size).min(total);

    let mut records = Vec::with_capacity((end - start).max(0) as usize);
    for i in start..end {
        let mut row = Record::with_capacity(2);
        row.set("id", i);
        row.set("label", format!("row-{}", i));
        records.push(row);
    }

    let has_more = end < total;
    let next_cursor = if has_more { Some(end.to_string()) } else { None };

    Ok(SourcePage {
        records,
        next_cursor,
        has_more,
    })
}
