# How Paganel compares

Different tools solve different jobs. If one of these fits your case better, use it. This table is here to save you evaluation time. Most of these tools are mature and well maintained, and several do things Paganel does not. ("Verification" below means checking the migrated *data* arrived intact, not checksumming migration scripts.)

| | DB→DB data + schema | Cross-engine (MySQL↔PG) | In-flight transforms | Dry run / plan | Crash-safe resume | Row-level verification | CDC / incremental | Single binary, air-gap OK |
|---|---|---|---|---|---|---|---|---|
| **Paganel** | ✅ tables, indexes, FKs, sequences, ENUMs | ✅ MySQL↔PG today (CSV as a source); more connectors on roadmap | ✅ expressions, validation, WASM/JS plugins | ✅ `plan`: tables, row estimates, exact DDL, type warnings, sample rows | ✅ checkpoint + WAL | ✅ keyed row hashes; `verify` names the divergent row | 🔜 snapshot today; catch-up & CDC on roadmap | ✅ |
| [pgloader](https://github.com/dimitri/pgloader) | data + basic schema, into PG only | ✅ | limited casts | ➖ `--dry-run` checks connections only | ❌ restart = redo | ❌ row counts | ❌ | v3 binary / v4 needs JVM |
| [pgcopydb](https://github.com/dimitri/pgcopydb) | PG→PG only, full fidelity | ❌ | ❌ | ❌ | partial | ➖ per-table checksum | ✅ follow mode | ✅ (needs libpq) |
| [Sling](https://github.com/slingdata-io/sling-cli) | data only (plain tables) | ✅ 30+ connectors | light | ❌ | ❌ | ❌ | incremental | ✅ (open core) |
| [ingestr](https://github.com/bruin-data/ingestr) | data only | ✅ many | ❌ | ❌ | ❌ | ❌ | incremental | ❌ Python |
| [AWS DMS](https://aws.amazon.com/dms/) | data; schema via SCT | ✅ many | limited rules | ✅ premigration assessment | ✅ managed | ✅ row-level | ✅ mature | ❌ AWS only |
| [Fivetran HVR](https://fivetran.com/docs/hvr6) | data + basic schema (refresh can create tables) | ✅ many | ✅ per-column SQL expressions | ➖ | ✅ log-based | ✅ row-by-row *compare*, honors the expressions | ✅ mature | ❌ hub server + repository DB + agents; self-hosted, enterprise-licensed |
| [BladePipe](https://www.bladepipe.com) | data + schema | ✅ many | ✅ custom Java, filters | ➖ pre-checks | ✅ | ✅ field-by-field *compare* + correction (extra target rows not detected) | ✅ | ❌ Java + MySQL metadata + console/worker; fully private only in the on-prem edition (the free edition's console is cloud-hosted) |
| [SQLines Data](https://www.sqlines.com/sqldata) | data + schema | ✅ many | ✅ SQL expressions in the extract | ❌ | not documented | ✅ row *compare* (`-vopt=rows`); whether it honors computed columns is not documented | ❌ | ✅ one executable, runs fully offline; paid license for the maintained build (an Apache-2.0 source drop is on GitHub) |
| [Airbyte](https://airbyte.com) | data only | ✅ 600+ connectors | via dbt, post-load | ➖ connection check | per-stream state | ❌ | ✅ | ❌ Docker/K8s |
| [Debezium](https://debezium.io) | ❌ emits change events | sources: many | via Kafka SMTs | ❌ | Kafka offsets | ❌ | ✅ the standard | ❌ Kafka infra |
| [reladiff](https://github.com/erezsh/reladiff) | ❌ verification only, moves nothing | ✅ diffs across engines | — | — | — | ✅ row-level, after the fact | — | ❌ Python |
| [Flyway / Liquibase / Atlas](https://atlasgo.io) | schema versioning in one DB; no data movement | — | — | ✅ SQL preview (`update-sql`, `--dry-run`) | — | script checksums, not data | — | varies |

✅ yes · ➖ partial · ❌ no · 🔜 planned · — not applicable

### Verification: a receipt vs a live compare

Several commercial tools above also verify row by row (DMS, HVR, BladePipe,
SQLines Data; Striim through its separate Validata product). They all do it the
same way: read the source and the target again and compare them, so the source
has to stay reachable and quiescent, and the check costs a second full read of
both sides. Paganel's `--integrity` is a different shape: the receipt is built
*while writing*, so `verify` runs later against the destination alone, with no
source connection, no second source read, and a 64-hex root per table you can
record outside the tool ([verification.md](verification.md)). The trade-off is
stated there too: it proves the destination matches what was written, not that
the source was read correctly. Among open-source, single-binary tools, none of
the others verify rows at all.

### Beyond copying rows

Three capabilities that don't fit the grid above, because none of the tools in it has them in this form. The closest matches live outside the grid, so they're named here:

| Capability | Paganel | Closest alternative |
|---|---|---|
| **Sandboxed plugins** | per-row transforms/filters/sources/sinks in Rust or JS, compiled to WASM, with fuel/memory/timeout caps, network/FS access denied by default, and a batched host boundary (Rust runs near-native). A plugin is called as a `select` column or as a `validate` check (a whole field or a whole check, not nested inside a larger expression), and a rejecting check's reason becomes the DLQ message | user code inside transforms is old: pgloader's `CAST … USING` calls any Common Lisp function loaded with `--load`, Striim's TQL calls imported Java functions inside `SELECT`, Talend calls Java routines inside tMap expressions and filters, Debezium runs Groovy/JS in a filter SMT, all as unsandboxed host code in the tool's own process. Redpanda Connect has a `wasm` processor (wazero-based, documented as work-in-progress, with no documented memory/fuel/timeout caps or capability gating); Airbyte CDK builds a whole unsandboxed connector; dlt and BladePipe run arbitrary Python/Java with full host access; DMS expressions are SQLite functions, not user code |
| **Graph expansion** | point at a root table; the FK graph is discovered and migrated with depth/exclude control. A `where` filter on the root cascades, so only *referenced* rows migrate ("orders since January, and only the customers those orders touch") | subsetting/anonymization tools do FK closure with filters: Greenmask (PostgreSQL only, dump-based, with anonymizing transformers), Neosync (Postgres/MySQL, anonymizing transformers, same engine on both sides as documented), Tonic Structural (same connector required on both sides), Jailer (emits sorted SQL). None of them migrate cross-engine, resume, or verify |
| **In-flight validation + DLQ** | per-row `assert`/`warn` rules (expressions or sandboxed plugin checks) with skip/fail/warn routing and a dead-letter table or JSONL file; results are recorded in the receipt | the ELT-era pattern is validating *after* load (dbt tests, Great Expectations), when bad rows are already in the destination. Classic ETL suites (Talend reject flows, Informatica reject files, SSIS error outputs) do route bad rows in flight, but they are full platforms, not a migration binary. In the grid: DMS offers a raw exceptions table; Debezium can drop rows with a scripted Filter SMT and Kafka Connect sends *conversion* failures to a dead-letter topic; dlt's schema contracts discard rows on schema violations, not on value rules |

**Where others win:** pgcopydb for pure PG->PG clones · Sling/ingestr/Airbyte for connector breadth and warehouse targets · Debezium/DMS/HVR/BladePipe for production CDC today, with HVR and BladePipe also comparing rows in-product · reladiff to diff two datasets some other tool produced · Flyway-class tools for schema evolution over time (complementary; use both). If dump-and-restore covers your case (`pg_dump | psql`, `mysqldump | mysql`), use that.

**Where Paganel fits:** one binary that migrates schema *and* data cross-engine, survives `kill -9` mid-run, and hands you a receipt. The receipt is per-row keyed hashes built during the write (~0.5 µs/row), so `verify` can later prove the destination still matches what was written, down to the exact primary key, with no access to the source and no network. All of it is declared in one PPL file that the tool can parse, so `plan` shows the blast radius before anything runs and a reviewer reads filters, transforms, and validation rules in one place ([why a DSL](why-ppl.md)). Each of those exists somewhere; having them in one tool is the uncommon part.

---

This table reflects each tool's documentation as of September 2026. Every one of them ships new features, so a cell here can go stale. It gets re-checked from time to time; if you spot something wrong or outdated, open an issue.
