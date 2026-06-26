// Source - synthetic row generator with cursor paging. Emits ids 0..TOTAL in
// pages of PAGE_SIZE.
// Test:  plugin test counter_source.js --json            -> first page (3 rows)
//        plugin test counter_source.js --cursor 3 --json  -> next page
const { source } = require("@stratum/plugin-sdk");

const TOTAL = 7;
const PAGE_SIZE = 3;

source("counter_source", {
  version: "1.0.0",
  output: { id: "i64", label: "string" },
  readPage(_config, cursor) {
    const start = cursor != null ? parseInt(cursor, 10) : 0;
    const end = Math.min(start + PAGE_SIZE, TOTAL);

    const records = [];
    for (let i = start; i < end; i++) {
      records.push({ id: i, label: `item-${i}` });
    }

    const hasMore = end < TOTAL;
    return {
      records,
      next_cursor: hasMore ? String(end) : null,
      has_more: hasMore,
    };
  },
});
