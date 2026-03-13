SELECT reltuples::bigint AS estimate
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = $1 AND n.nspname = 'public'