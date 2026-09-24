"use strict";

// Read environment variables granted via `allow_env`. Only the declared names
// are visible; `get` returns null for an unset or ungranted name.
module.exports = {
    get: (name) => globalThis.__host_env(String(name)) ?? null,
};
