WITH primary_keys AS (
  SELECT a.attname AS column_name, k.ord::int AS pk_position
  FROM pg_constraint con
  JOIN pg_class rel ON rel.oid = con.conrelid
  JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
  JOIN unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
  JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum = k.attnum
  WHERE nsp.nspname = '{schema}' AND rel.relname = '{table}' AND con.contype = 'p'
),
unique_constraints AS (
  SELECT a.attname AS column_name
  FROM pg_constraint con
  JOIN pg_class rel ON rel.oid = con.conrelid
  JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
  JOIN unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
  JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum = k.attnum
  WHERE nsp.nspname = '{schema}' AND rel.relname = '{table}' AND con.contype = 'u'
)
SELECT
  c.ordinal_position,
  c.column_name,
  format_type(t.oid, NULL) AS data_type,
  c.is_nullable,
  c.column_default IS NOT NULL AS has_default,
  c.column_default,
  c.character_maximum_length,
  c.numeric_precision,
  c.numeric_scale,
  EXISTS (SELECT 1 FROM primary_keys pk WHERE pk.column_name = c.column_name) AS is_primary_key,
  (SELECT pk.pk_position FROM primary_keys pk WHERE pk.column_name = c.column_name) AS pk_position,
  EXISTS (SELECT 1 FROM unique_constraints uq WHERE uq.column_name = c.column_name) AS is_unique,
  -- A column auto-increments if it defaults to nextval(...) (serial/bigserial,
  -- including standalone sequences that pg_get_serial_sequence misses because
  -- they aren't OWNED BY the column) or is an IDENTITY column.
  (COALESCE(c.column_default LIKE 'nextval(%', false) OR c.is_identity = 'YES') AS is_auto_increment,
  col_description((c.table_schema || '.' || c.table_name)::regclass, c.ordinal_position::int) AS column_comment,
  c.collation_name,
  NULL::text AS character_set_name,
  (c.is_generated = 'ALWAYS') AS is_generated,
  c.generation_expression AS generated_expression,
  -- For enum columns, expose the variants in MySQL's `enum('a','b')` form so
  -- cross-dialect conversion can emit an inline ENUM(...) type. NULL otherwise.
  CASE WHEN t.typtype = 'e' THEN
    'enum(' || (
      SELECT string_agg(quote_literal(e.enumlabel), ',' ORDER BY e.enumsortorder)
      FROM pg_enum e WHERE e.enumtypid = t.oid
    ) || ')'
  END AS full_column_type
FROM information_schema.columns AS c
LEFT JOIN pg_type t ON t.typname = c.udt_name
WHERE c.table_schema = '{schema}' AND c.table_name = '{table}'
ORDER BY c.ordinal_position::int;
