# How Paganel compares

Different tools solve different jobs. If one of these fits your case better, use it. This table is here to save you the evaluation time; it is not a ranking or a pitch. Most of these tools are mature and well maintained, and several do things Paganel does not. ("Verification" below means checking the migrated *data* arrived intact, not checksumming migration scripts.)

| | DB→DB data + schema | Cross-engine (MySQL↔PG) | In-flight transforms | Dry run / plan | Crash-safe resume | Row-level verification | CDC / incremental | Single binary, air-gap OK |
|---|---|---|---|---|---|---|---|---|
| **Paganel** | ✅ tables, indexes, FKs, sequences, ENUMs | ✅ MySQL↔PG today (CSV as a source); more connectors on roadmap | ✅ expressions, validation, WASM/JS plugins | ✅ `plan`: tables, row estimates, exact DDL, type warnings, sample rows | ✅ checkpoint + WAL | ✅ keyed row hashes; `verify` names the divergent row | 🔜 snapshot today; catch-up & CDC on roadmap | ✅ |
| [pgloader](https://github.com/dimitri/pgloader) | data + basic schema, into PG only | ✅ | limited casts | ➖ `--dry-run` checks connections only | ❌ restart = redo | ❌ row counts | ❌ | v3 binary / v4 needs JVM |
| [pgcopydb](https://github.com/dimitri/pgcopydb) | PG→PG only, full fidelity | ❌ | ❌ | ❌ | partial | ➖ per-table checksum | ✅ follow mode | ✅ (needs libpq) |
| [Sling](https://github.com/slingdata-io/sling-cli) | data only (plain tables) | ✅ 30+ connectors | light | ❌ | ❌ | ❌ | incremental | ✅ (open core) |
| [ingestr](https://github.com/bruin-data/ingestr) | data only | ✅ many | ❌ | ❌ | ❌ | ❌ | incremental | ❌ Python |
| [AWS DMS](https://aws.amazon.com/dms/) | data; schema via SCT | ✅ many | limited rules | ✅ premigration assessment | ✅ managed | ✅ row-level | ✅ mature | ❌ AWS only |
| [Airbyte](https://airbyte.com) | data only | ✅ 600+ connectors | via dbt, post-load | ➖ connection check | per-stream state | ❌ | ✅ | ❌ Docker/K8s |
| [Debezium](https://debezium.io) | ❌ emits change events | sources: many | via Kafka SMTs | ❌ | Kafka offsets | ❌ | ✅ the standard | ❌ Kafka infra |
| [reladiff](https://github.com/erezsh/reladiff) | ❌ verification only, moves nothing | ✅ diffs across engines | — | — | — | ✅ row-level, after the fact | — | ❌ Python |
| [Flyway / Liquibase / Atlas](https://atlasgo.io) | schema versioning in one DB; no data movement | — | — | ✅ SQL preview (`update-sql`, `--dry-run`) | — | script checksums, not data | — | varies |

✅ yes · ➖ partial · ❌ no · 🔜 planned · — not applicable

### Beyond copying rows

Three capabilities that don't fit the grid above, because none of the other tools has them:

| Capability | Paganel | Closest alternative |
|---|---|---|
| **Sandboxed plugins** | per-row transforms/filters/sources/sinks in Rust or JS, compiled to WASM, with fuel/memory/timeout caps, network/FS access denied by default, and a batched host boundary (Rust runs near-native) | Airbyte CDK builds a whole unsandboxed connector; dlt runs arbitrary Python with full host access; Bloblang maps well but isn't capability-gated user code |
| **Graph expansion** | point at a root table; the FK graph is discovered and migrated with depth/exclude control. A `where` filter on the root cascades, so only *referenced* rows migrate ("orders since January, and only the customers those orders touch") | subsetting tools (Jailer, Tonic) do FK closure but don't migrate cross-engine, transform, resume, or verify |
| **In-flight validation + DLQ** | per-row `assert`/`warn` rules (expressions or sandboxed plugin checks) with skip/fail/warn routing and a dead-letter table or JSONL file; results are recorded in the receipt | the industry pattern is validating *after* load (dbt tests, Great Expectations), when bad rows are already in the destination; DMS offers a raw exceptions table |

**Where others win:** pgcopydb for pure PG→PG clones · Sling/ingestr/Airbyte for connector breadth and warehouse targets · Debezium/DMS for production CDC today · reladiff to diff two datasets some other tool produced · Flyway-class tools for schema evolution over time (complementary; use both). If `pg_dump | psql` covers your case, use that.

**Where Paganel fits:** one binary that migrates schema *and* data cross-engine, survives `kill -9` mid-run, and hands you a receipt: per-row keyed hashes built during the write (~0.4 µs/row) so `verify` can later prove the destination still matches what was written, down to the exact primary key, with no access to the source and no network. All of it is declared in one PPL file that the tool can parse, so `plan` shows the blast radius before anything runs and a reviewer reads filters, transforms, and validation rules in one place ([why a DSL](why-ppl.md)). Each of those exists somewhere; having them in one tool is the uncommon part.

---

This table reflects each tool's documentation as of September 2026. Every one of them ships new features, so a cell here can go stale. It gets re-checked from time to time; if you spot something wrong or outdated, open an issue.
