SELECT
    i.relname as index_name,
    a.attname as column_name,
    ix.indisunique as is_unique,
    ix.indisprimary as is_primary,
    am.amname as index_type,
    pg_get_expr(ix.indpred, ix.indrelid) as condition
FROM pg_class t
JOIN pg_index ix ON t.oid = ix.indrelid
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_am am ON i.relam = am.oid
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'public'
    AND t.relname = '{table}'
ORDER BY i.relname, array_position(ix.indkey, a.attnum)