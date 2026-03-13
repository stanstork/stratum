SELECT c.conname as constraint_name,
       t.relname as table_name,
       pg_get_constraintdef(c.oid) as definition
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
JOIN pg_namespace n ON t.relnamespace = n.oid
WHERE t.relname = $1
  AND n.nspname = 'public'
  AND c.contype = 'c'
