// Filter - string validation. Passes rows whose `email` looks like an address.
// Rejected rows carry a reason (surfaced in DLQ / logs).
// Test:  plugin test email_valid.js --input '{"email":"a@b.com"}'  -> PASS
//        plugin test email_valid.js --input '{"email":"nope"}'     -> REJECT
const { filter } = require("@stratum/plugin-sdk");

const RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;

filter("email_valid", {
  version: "1.0.0",
  input: { email: "string" },
  evaluate({ email }) {
    if (typeof email === "string" && RE.test(email)) {
      return { pass: true };
    }
    return { pass: false, reason: `invalid email: ${email}` };
  },
});
