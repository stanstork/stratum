# Why PPL?

Every migration tool needs some way for the user to say what should happen to
the data. Paganel's is PPL, a small declarative language built for this one job.
This page explains why Paganel uses a DSL instead of YAML, JSON, CLI flags, or
embedded SQL, and what that choice costs.

A migration config should be a document the tool can analyze and a human can
review. Everything else about PPL follows from that.

---

## What a migration config has to express

A realistic migration is more than "copy table A to table B":

- connections and credentials (without leaking secrets into the file)
- which tables, and which *rows*: filters, sometimes involving related tables
- how columns map, rename, and get computed
- data-quality rules, and what happens to rows that fail them
- error handling: retries, dead-letter routing
- schema behavior: what gets created, in what order (FKs!)
- execution: dependencies between pipelines, parallelism, pagination

One more requirement shapes all of the above: the tool has to understand the
config well enough to work with it. Show a plan before anything moves. Diff two
versions. Record in a verification receipt which rules ran. Push a filter
through a foreign-key graph. A config the tool can only *execute*, but not
*reason about*, caps every one of those features.

## The four ways tools do this

Migration tools take one of four approaches to config. Each is good at
something, and each breaks down in its own way as migrations grow.

### 1. CLI flags

```bash
tool copy --source-uri 'mysql://…' --source-table customer \
          --dest-uri 'postgres://…' --incremental-key updated_at
```

**Great at:** time-to-first-copy. One table, one command, no file.
**Decays when:** there are forty tables, a filter, or a second engineer. There
is nothing to review in a PR, nothing to diff, nothing to re-run in six months.
The command lives in someone's shell history.

### 2. YAML stream lists

```yaml
streams:
  sakila.customer:
    object: public.dim_customer
    mode: incremental
    sql: |
      select customer_id, lower(trim(email)) as email
      from sakila.customer where active = 1
```

**Great at:** listing many tables declaratively; defaults and wildcards.
**Decays when:** you need logic. The moment a filter or transform appears, it
escapes into an embedded SQL string, and from that point the tool can no longer
see it. It can't tell you which columns the migration produces, can't validate
the expression against the schema, can't include the logic in a plan or a
receipt. The YAML is structured; the part that matters isn't. (Not YAML's fault. It
has no expression language, and migrations need one.)

### 3. JSON rule engines

```json
{ "rules": [
  { "rule-type": "selection", "rule-id": "1",
    "object-locator": { "schema-name": "sakila", "table-name": "customer" },
    "filters": [{ "column-name": "active",
      "filter-conditions": [{ "filter-operator": "eq", "value": "1" }] }] } ] }
```

**Great at:** being written by machines; enterprise tools generate these from
consoles and APIs.
**Decays when:** a human has to write or review one. Hand-maintained rule IDs,
deeply nested locators, and expressiveness that stops exactly where you need it
(per-table column filters; no joins, no computed values).

### 4. Purpose-built DSLs

The oldest tool in this category settled this years ago:
pgloader's `LOAD DATABASE … WITH … CAST …` command is a real DSL, and its users,
largely DBAs, never found that strange. DBAs will learn a language if it does
enough for them.

PPL is the same idea with a syntax closer to SQL and HCL:

```ppl
pipeline "customers" {
  from { connection = connection.src table = "customers" }
  to   { connection = connection.dst table = "dim_customers" mode = "replace" }

  where "active" { customers.deleted_at is null }

  select {
    id    = customers.id
    email = lower(trim(customers.email))
    tier  = when {
      customers.lifetime_value > 10000 then "gold"
      else "standard"
    }
  }

  validate {
    assert "has_email" { check = email is not null  action = skip }
  }
}
```

**Great at:** expressing the whole migration (filters, joins, transforms,
validation, error policy, schema behavior) as *structure* the tool can parse,
type-check, and reason about.
**Decays when:** the language grows without discipline. Every config DSL is one
enthusiastic release away from becoming a bad programming language. The answer
to that is below.

## What structure buys you

None of this works with an embedded SQL string, because each feature needs
Paganel to understand the config:

- **`plan` shows blast radius before anything moves.** Which tables, estimated
  rows, the exact DDL, type-conversion warnings, what will be verified. Possible
  only because filters and mappings are AST, not opaque strings.
- **Configs are reviewable.** A migration is a PR: a colleague reads the
  `where` block, the validation rules, and the error routing in one file, in a
  syntax built for reading. `plan` acts as the CI check.
- **Receipts can attest what ran.** Verification receipts record which
  validation rules and which config produced the data. "These rows passed these
  named rules" only means something when the rules are inspectable objects.
- **Filters propagate.** In a graph migration (`with references { data =
  cascade }`), the `where` on the root table restricts every discovered table to
  referenced rows only. A filter inside a SQL string can't be pushed through a
  foreign-key graph the tool discovers at runtime.
- **Everything composes.** A plugin's output is a column; a `when` branches on
  it; a validation rule checks the result; the DLQ catches failures; the receipt
  records all of it. These are one type system, not four tools taped together.

## Deliberate limits

PPL avoids becoming a programming language on purpose:

- **Not Turing-complete.** No loops, no user-defined functions, no recursion.
  A config's behavior must be decidable by reading it.
- **Real logic goes in plugins.** When a transform outgrows expressions, write
  it in Rust or JavaScript as a WASM plugin: sandboxed, capability-gated,
  resource-capped. The escape hatch is a real
  language in a cage, not string interpolation.
- **Raw SQL where raw SQL is right.** `before`/`after` hooks take plain SQL
  arrays for the things DBAs legitimately do around a bulk load (drop and
  recreate indexes, disable triggers, `VACUUM ANALYZE`). What doesn't need
  wrapping is left unwrapped.
- **You shouldn't have to start from a blank file.** The biggest real cost of
  any DSL is the blank-page problem. The runnable
  [`examples/configs/`](../examples/configs/) ship complete, commented configs
  for every feature - schema mapping, DAG dependencies, joins, validation, DLQ -
  so the normal workflow is *copy the closest example and edit*, not *learn and
  type*.

## The trade-offs, honestly

A DSL has real costs:

- **There is a learning curve.** Smaller than it looks (the reference is one
  page; the syntax is deliberately close to SQL and HCL), but it exists, and
  YAML's false familiarity is an adoption advantage PPL gives up.
- **Editor support has to be built.** Syntax highlighting and LSP niceties
  don't come free the way they do for YAML/JSON. This is on the roadmap;
  today you get precise parser errors instead.
- **A parser has to be maintained.** That's real engineering budget spent on
  syntax instead of features. The analyzability payoffs above justify it;
  that's a judgment, not a proof.

## Frequently asked

**Why not YAML with a schema?** A schema validates YAML's *shape*, not its
*meaning*. The parts of a migration that matter (expressions, conditions,
computed columns) would still live in strings, and everything in "What
structure buys you" would still be impossible.

**Why not just SQL?** SQL describes queries against one engine. A migration
spans two engines with different type systems, plus concerns SQL has no words
for: verification policy, error routing, dependency ordering, schema-creation
behavior. You'd end up with SQL *plus* a config format around it, two
languages instead of one.

**Why not embed Lua/Starlark/JS for the whole config?** Then configs become
programs: their behavior depends on execution, plans become approximations, and
review means simulating code in your head. JS is embedded where computation
belongs, in sandboxed plugins with declared capabilities, and the document
stays declarative.

**Is PPL stable?** Pre-1.0, and breaking changes are called out in the
changelog. The grammar will settle before the tool does.
