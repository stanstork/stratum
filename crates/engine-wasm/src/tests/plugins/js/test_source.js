// JS counterpart of plugins/test_source (emits a fixed range of synthetic rows).
//
// Config (JSON, host-provided at init time):
//   { "total": "10", "page_size": "3" }
//
// Each call to readPage returns up to `page_size` rows starting at the cursor
// offset. The cursor is the next offset as a decimal string; null means "start
// from 0". `has_more` is false on the final page so the host stops paging.
const { source } = require("@stratum/plugin-sdk");

source("test_source", {
  version: "1.0.0",
  output: {
    id: "i64",
    label: "string",
  },
  readPage(config, cursor) {
    const total = parseInt(config.total ?? "10", 10);
    const pageSize = parseInt(config.page_size ?? "3", 10);

    const start = cursor != null ? parseInt(cursor, 10) : 0;
    const end = Math.min(start + pageSize, total);

    const records = [];
    for (let i = start; i < end; i++) {
      records.push({ id: i, label: `row-${i}` });
    }

    const hasMore = end < total;
    return {
      records,
      next_cursor: hasMore ? String(end) : null,
      has_more: hasMore,
    };
  },
});
