"use strict";

// Read/write files within the directories granted via `allow_fs_read` /
// `allow_fs_write`. Access outside those preopened directories fails.
// `readText` returns null when the file is missing or unreadable; `writeText`
// returns true on success.
module.exports = {
    readText: (path) => globalThis.__host_fs_read(String(path)) ?? null,
    writeText: (path, contents) => globalThis.__host_fs_write(String(path), String(contents)),
};
