// Source - fetches rows from an HTTP JSON API using the `http` capability.
// This is the "API as a data source" pattern: instead of reading a database,
// the plugin pulls records over HTTP and hands them to the pipeline.
//
// Demonstrates the HTTP host function end to end:
//   - `allow_http` gates outbound HTTP (denied by default),
//   - `allow_http_hosts` restricts WHICH hosts may be reached (see the config),
//   - `resp.status` is the REAL HTTP status returned by the host (not a fake
//     200), so the plugin can react to failures.
//
// Endpoint: JSONPlaceholder /users returns an array of {id, name, email, ...}.
// Needs network + `allow_http = true`. For an offline run, point API_URL at a
// local stub that returns the same JSON shape.
//
// Test (needs network + the capability, so run via the config, not `plugin test`):
//   pag apply -c examples/plugins/configs/api_source.ppl -e .env
const { source, http } = require("@paganel/plugin-sdk");

const API_URL = "https://jsonplaceholder.typicode.com/users";

source("api_source", {
  version: "1.0.0",
  output: { id: "i64", name: "string", email: "string" },
  readPage(_config, cursor) {
    // The whole collection comes back in one call, so we emit it once and stop.
    // A genuinely paginated API would thread `cursor` into the query string and
    // keep returning a next_cursor until the API signals the end.
    if (cursor != null) {
      return { records: [], next_cursor: null, has_more: false };
    }

    const resp = http.get(API_URL);
    if (resp.status !== 200) {
      // A blocked host (not in allow_http_hosts) or a denied capability surfaces
      // here as status 0; a real API error surfaces as its actual 4xx/5xx code.
      throw new Error(`api_source: GET ${API_URL} -> HTTP ${resp.status}`);
    }

    const records = JSON.parse(resp.body).map((u) => ({
      id: u.id,
      name: u.name,
      email: u.email,
    }));
    return { records, next_cursor: null, has_more: false };
  },
});
