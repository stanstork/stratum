"use strict";

const { setOrFail, setSinkPrepare, setSinkFinalize } = require("./registry");
const http = require("./http");
const log = require("./log");

function transform(name, opts) {
    validateOpts(name, opts, ["version", "input", "output", "compute"]);
    setOrFail("transform",
        { name, version: opts.version, input: opts.input, output: opts.output },
        opts.compute);
}

function filter(name, opts) {
    validateOpts(name, opts, ["version", "input", "evaluate"]);
    setOrFail("filter",
        { name, version: opts.version, input: opts.input },
        opts.evaluate);
}

function source(name, opts) {
    validateOpts(name, opts, ["version", "output", "readPage"]);
    setOrFail("source",
        { name, version: opts.version, output: opts.output },
        opts.readPage);
}

function sink(name, opts) {
    validateOpts(name, opts, ["version", "input", "writeBatch"]);
    setOrFail("sink",
        { name, version: opts.version, input: opts.input },
        opts.writeBatch);
    // Optional lifecycle hooks
    if (opts.prepare) setSinkPrepare(opts.prepare);
    if (opts.finalize) setSinkFinalize(opts.finalize);
}

function validateOpts(name, opts, required) {
    if (typeof name !== "string" || !name) throw new Error("plugin name required");
    for (const k of required) {
        if (!(k in opts)) throw new Error(`${k} is required for plugin "${name}"`);
    }
}

module.exports = { transform, filter, source, sink, http, log };