const { test } = require("node:test");
const assert = require("node:assert/strict");
const sdk = require("../src");
const registry = require("../src/registry");

test("transform descriptor produces canonical metadata", () => {
    sdk.transform("discount", {
        version: "1.0.0",
        input: { total: "f64", tier: "string" },
        output: "f64",
        compute: ({ total, tier }) => tier === "gold" ? total * 0.85 : total,
    });

    const md = JSON.parse(registry.__stratum_get_metadata());
    assert.equal(md.name, "discount");
    assert.equal(md.type, "transform");
    assert.equal(md.output_type, "f64");
    assert.deepEqual(md.input_schema, [
        { name: "total", type: "f64", nullable: false },
        { name: "tier", type: "string", nullable: false },
    ]);
});