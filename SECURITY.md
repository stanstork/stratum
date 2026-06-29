# Security Policy

Stratum moves data between databases, handles connection credentials, and
executes user-supplied WASM/JavaScript plugins. This document describes how to
report vulnerabilities and the security model you can rely on.

> **Status:** Stratum is pre-1.0 software under active development. It has not
> undergone a third-party security audit. Review the model below before running
> it against production systems.

## Reporting a vulnerability

Please **do not** open a public issue for security problems.

Report privately via GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability):
on the repository's **Security** tab, click **Report a vulnerability**.

Please include reproduction steps, affected version/commit, and impact. I aim to
acknowledge reports within a few days. Fixes land on `main`; there are no
maintained release branches yet (pre-1.0), so upgrade to the latest commit for
security fixes.

## Supported versions

| Version | Supported |
|---------|-----------|
| `main` (latest) | ✅ |
| Older commits / tags | ❌ |

## Credential handling

- Database credentials are supplied through the SMQL `env(...)` function and are
  **not** persisted in Stratum's state store (`~/.stratum/state/`).
- Connection URLs and any field whose name matches a sensitive pattern
  (`password`, `secret`, `token`, `key`, `api_key`, `auth`, `credential`, …) are
  **masked** in generated plans and log output (e.g.
  `mysql://user:****@host/db`). See
  `crates/engine-planner/src/builder/utils/masking.rs`.
- The `test-conn` command does not log raw connection URLs.

If you find a path where credentials reach logs, plan output, or state
unmasked, please report it.

## Plugin sandbox model

WASM and JavaScript (QuickJS) plugins run inside a Wasmtime sandbox with
**capabilities denied by default**. See `crates/engine-wasm/src/runtime/`.

**Host capabilities** (`HostCapabilities`, default values):

| Capability | Default | Notes |
|------------|---------|-------|
| Logging (`log_*`) | enabled | routed to the host tracing logs |
| Outbound HTTP | **disabled** | host function is a stub; no network egress |
| Key-value store | **disabled** | not implemented |
| Custom metrics | **disabled** | not implemented |

**Resource limits** (`ResourceLimits`):

| Limit | Row plugins (transform/filter) | I/O plugins (source/sink) |
|-------|--------------------------------|---------------------------|
| Memory | 64 MB | 128 MB |
| Execution fuel | 1,000,000 | 100,000,000 |
| Wall-clock timeout | 1,000 ms | 30,000 ms |
| Max output | 1 MB | 16 MB |

- **No filesystem access** and **no network access** are granted to guests. The
  WASI context inherits only stdio; no directories are preopened and no outbound
  HTTP is wired up.
- **CPU/loop protection:** execution is bounded by Wasmtime fuel and a
  wall-clock timeout, so an infinite loop or runaway plugin is terminated rather
  than hanging the migration.
- **Memory protection:** guest memory growth is capped via `StoreLimits`.

**Trust boundaries you should be aware of:**

- A plugin is **arbitrary code you choose to load**. The sandbox limits what a
  plugin *can reach* (no FS/network, bounded CPU/memory), but you should still
  only run plugins you trust, especially `.wasm` binaries you did not build.
- Compiling a `.js` plugin to WASM shells out to `esbuild` (via `npx`) on first
  use. That toolchain runs on the host with your privileges; pre-compile to
  `.wasm` in untrusted environments to avoid it.

## Database & input safety

- Stratum executes SQL derived from **your own SMQL configuration** against
  **your own databases** - the operator who writes the config is the trust
  anchor. Treat SMQL config files like any other infrastructure code.
- Row data read from a source is carried as values/COPY payloads, not spliced
  into DDL.

## Scope

In scope: credential leakage, sandbox escapes, plugins gaining unauthorized
FS/network access, resource limits that fail to bound a plugin, and unmasked
secrets in logs/plans/state.

Out of scope: issues that require a malicious SMQL config you authored yourself,
or running an untrusted `.wasm` plugin you deliberately loaded (the sandbox
mitigates but does not eliminate this risk - see trust boundaries above).
