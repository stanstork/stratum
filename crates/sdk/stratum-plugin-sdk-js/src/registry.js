"use strict";

const STORE = {
    role: null,            // "transform" | "filter" | "source" | "sink"
    descriptor: null,      // {name, version, input, output, ...}
    handler: null,         // user function
    prepareHandler: null,  // sink only (optional)
    finalizeHandler: null, // sink only (optional)
    config: {},            // plugin config (SMQL `config { }`), set by the host at init
};

// Install the plugin's static config.
function __stratum_set_config(configJson) {
    STORE.config = configJson ? JSON.parse(configJson) : {};
}

function setOrFail(role, descriptor, handler) {
    if (STORE.role !== null) {
        throw new Error(
            `Plugin SDK: ${STORE.role}() already called; only one role per plugin is allowed`
        );
    }

    STORE.role = role;
    STORE.descriptor = descriptor;
    STORE.handler = handler;
}

// Optional sink lifecycle hooks. `sink()` calls these when the author supplies
// `prepare`/`finalize`; the runtime invokes them via __stratum_lifecycle.
function setSinkPrepare(fn) {
    STORE.prepareHandler = fn;
}

function setSinkFinalize(fn) {
    STORE.finalizeHandler = fn;
}

// The runtime (and the compiler's metadata extractor) query these. They close
// over STORE, which the user's role registration populates later in the bundle.
function __stratum_get_metadata() {
    if (!STORE.descriptor) throw new Error("Plugin SDK: no role registered");
    return JSON.stringify(toMetadata(STORE.role, STORE.descriptor));
}

function __stratum_dispatch(role, inputJson) {
    if (STORE.role !== role) {
        throw new Error(`expected role '${STORE.role}', host called '${role}'`);
    }
    const d = STORE.descriptor;
    const raw = JSON.parse(inputJson);

    switch (role) {
        case "transform": {
            const result = STORE.handler(unwrapRow(raw), STORE.config || {});
            return JSON.stringify({ type: typeOf(d.output), value: result });
        }
        case "filter": {
            const decision = STORE.handler(unwrapRow(raw), STORE.config || {});
            return JSON.stringify(decision); // {pass, reason?}
        }
        case "source": {
            // raw = {cursor}. (Config plumbing into JS is not wired yet.)
            const page = STORE.handler(STORE.config || {}, raw.cursor ?? null);
            const records = (page.records || []).map((r) => wrapRow(d.output, r));
            return JSON.stringify({
                records,
                next_cursor: page.next_cursor ?? null,
                has_more: !!page.has_more,
            });
        }
        case "sink": {
            // raw = {records:[{field: envelope}]}.
            const records = (raw.records || []).map(unwrapRow);
            const res = STORE.handler(STORE.config || {}, { records });
            return JSON.stringify({ rows_written: res.rows_written });
        }
        default:
            throw new Error(`unknown role ${role}`);
    }
}

// Run a sink lifecycle hook. `hook` is "prepare" | "finalize". Absent hooks are
// a no-op (they're optional). Returns "" on success; throwing propagates to the
// host as a lifecycle error.
function __stratum_lifecycle(hook, inputJson) {
    const fn =
        hook === "prepare" ? STORE.prepareHandler :
            hook === "finalize" ? STORE.finalizeHandler :
                null;
    if (typeof fn !== "function") return ""; // optional hook not registered
    const config = inputJson ? JSON.parse(inputJson) : (STORE.config || {});
    fn(config);
    return "";
}

function typeOf(spec) {
    return typeof spec === "string" ? spec : spec.type;
}

function unwrapValue(env) {
    return env && typeof env === "object" && "type" in env
        ? (env.value === undefined ? null : env.value)
        : env;
}

function unwrapRow(row) {
    const out = {};
    for (const k in row) out[k] = unwrapValue(row[k]);
    return out;
}

function inferType(v) {
    if (typeof v === "boolean") return "bool";
    if (typeof v === "number") return Number.isInteger(v) ? "i64" : "f64";
    return "string";
}

function wrapRow(schema, row) {
    const out = {};
    for (const k in row) {
        const t = schema && schema[k] ? typeOf(schema[k]) : inferType(row[k]);
        out[k] = { type: t, value: row[k] };
    }
    return out;
}

module.exports = {
    setOrFail,
    setSinkPrepare,
    setSinkFinalize,
    __stratum_get_metadata,
    __stratum_dispatch,
    __stratum_lifecycle,
    __stratum_set_config,
};

// Expose the hooks on globalThis as a load-time side effect. The Stratum JS
// runtime evaluates the bundle and then reads these off globalThis (see
// stratum-plugin-js-runtime/src/bootstrap.rs); the compiler's QuickJS-based
// metadata extractor does the same. esbuild bundles this module exactly once,
// so both globals reference the single STORE the plugin registers into.
if (typeof globalThis !== "undefined") {
    globalThis.__stratum_get_metadata = __stratum_get_metadata;
    globalThis.__stratum_dispatch = __stratum_dispatch;
    globalThis.__stratum_lifecycle = __stratum_lifecycle;
    globalThis.__stratum_set_config = __stratum_set_config;
}

function toMetadata(role, descriptor) {
    // Translates author-facing descriptor into the canonical wire schema.
    const base = {
        name: descriptor.name,
        version: descriptor.version,
        type: role,
        exchange_format: "json_v1",
        runtime: "js", // host sizes resource limits off this (QuickJS needs headroom)
    };

    switch (role) {
        case "transform":
            return {
                ...base,
                input_schema: schemaToFields(descriptor.input),
                output_type: descriptor.output,
            };
        case "filter":
            return { ...base, input_schema: schemaToFields(descriptor.input) };
        case "source":
            return { ...base, output_schema: schemaToFields(descriptor.output) };
        case "sink":
            return { ...base, input_schema: schemaToFields(descriptor.input) };
        default:
            throw new Error(`unknown role ${role}`);
    }
}

function schemaToFields(schema) {
    return Object.entries(schema).map(([name, spec]) => {
        if (typeof spec === "string") {
            return { name, type: spec, nullable: false };
        }
        return {
            name,
            type: spec.type,
            nullable: !!spec.nullable,
        };
    });
}