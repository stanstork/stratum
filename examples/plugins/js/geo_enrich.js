// Transform - uses the HTTP capability to enrich a row. Requires the decl to
// grant `allow_http = true`; without it the host denies __host_http_request.
// Good for testing the capability gate: run once with allow_http and once
// without, and confirm the second fails cleanly.
//
// NOTE: needs network + a reachable endpoint. For an offline test, point it at
// a local stub. Returns the resolved country code for an IP.
const { transform, http } = require("@stratum/plugin-sdk");

transform("geo_enrich", {
  version: "1.0.0",
  output: "string",
  input: { ip: "string" },
  compute({ ip }) {
    const resp = http.get(`https://ipapi.co/${ip}/country/`);
    // resp = { status, headers, body }
    if (resp.status !== 200) {
      throw new Error(`geo lookup failed: HTTP ${resp.status}`);
    }
    return (resp.body || "").trim();
  },
});
