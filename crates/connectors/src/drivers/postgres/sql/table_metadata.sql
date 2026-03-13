WITH primary_keys AS (
  SELECT kcu.table_name, kcu.column_name
  FROM information_schema.table_constraints AS tc
  JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
  WHERE tc.table_schema = 'public' AND tc.table_name = '{table}' AND tc.constraint_type = 'PRIMARY KEY'
),
unique_constraints AS (
  SELECT kcu.table_name, kcu.column_name
  FROM information_schema.table_constraints AS tc
  JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
  WHERE tc.table_schema = 'public' AND tc.table_name = '{table}' AND tc.constraint_type = 'UNIQUE'
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
  EXISTS (SELECT 1 FROM unique_constraints uq WHERE uq.column_name = c.column_name) AS is_unique,
  pg_get_serial_sequence('{table}', c.column_name) IS NOT NULL AS is_auto_increment,
  col_description((c.table_schema || '.' || c.table_name)::regclass, c.ordinal_position::int) AS column_comment,
  c.collation_name,
  NULL::text AS character_set_name,
  (c.is_generated = 'ALWAYS') AS is_generated,
  c.generation_expression AS generated_expression
FROM information_schema.columns AS c
LEFT JOIN pg_type t ON t.typname = c.udt_name
WHERE c.table_schema = 'public' AND c.table_name = '{table}'
ORDER BY c.ordinal_position::int;
