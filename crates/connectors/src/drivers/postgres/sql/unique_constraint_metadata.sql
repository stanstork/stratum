SELECT c.conname as constraint_name,
       t.relname as table_name,
       string_agg(a.attname, ',' ORDER BY array_position(c.conkey, a.attnum)) as columns
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
JOIN pg_namespace n ON t.relnamespace = n.oid
JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord) ON true
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = u.attnum
WHERE t.relname = $1
  AND n.nspname = 'public'
  AND c.contype = 'u'
GROUP BY c.conname, t.relname
