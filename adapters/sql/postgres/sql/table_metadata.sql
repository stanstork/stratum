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
),
foreign_keys AS (
  SELECT kcu.column_name, ccu.table_name AS referenced_table, ccu.column_name AS referenced_column,
         rc.delete_rule AS on_delete, rc.update_rule AS on_update
  FROM information_schema.key_column_usage AS kcu
  JOIN information_schema.referential_constraints AS rc 
    ON kcu.constraint_name = rc.constraint_name 
    AND kcu.constraint_schema = rc.constraint_schema
  JOIN information_schema.constraint_column_usage AS ccu 
    ON rc.constraint_name = ccu.constraint_name 
    AND rc.constraint_schema = ccu.constraint_schema
  WHERE kcu.constraint_schema = 'public' AND kcu.table_name = '{table}'
)
SELECT
  c.ordinal_position,
  c.column_name,
  c.data_type,
  c.is_nullable,
  c.column_default IS NOT NULL AS has_default,
  c.column_default,
  c.character_maximum_length,
  c.numeric_precision,
  c.numeric_scale,
  EXISTS (SELECT 1 FROM primary_keys pk WHERE pk.column_name = c.column_name) AS is_primary_key,
  EXISTS (SELECT 1 FROM unique_constraints uq WHERE uq.column_name = c.column_name) AS is_unique,
  pg_get_serial_sequence('{table}', c.column_name) IS NOT NULL AS is_auto_increment,
  fk.referenced_table,
  fk.referenced_column,
  fk.on_delete,
  fk.on_update
FROM information_schema.columns AS c
LEFT JOIN foreign_keys AS fk ON c.column_name = fk.column_name
WHERE c.table_schema = 'public' AND c.table_name = '{table}'
ORDER BY c.ordinal_position::int;
