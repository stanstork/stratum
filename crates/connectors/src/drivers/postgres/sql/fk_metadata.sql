SELECT
    con.conname AS constraint_name,
    sch.nspname AS schema_name,
    tbl.relname AS table_name,
    STRING_AGG(att.attname, ',' ORDER BY u.pos) AS columns,
    ref_sch.nspname AS referenced_schema,
    ref_tbl.relname AS referenced_table,
    STRING_AGG(ref_att.attname, ',' ORDER BY u.pos) AS referenced_columns,
    CASE con.confdeltype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
    END AS on_delete,
    CASE con.confupdtype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
    END AS on_update,
    BOOL_OR(att_col.is_nullable = 'YES') AS is_nullable,
    con.condeferrable AS is_deferrable,
    con.condeferred AS initially_deferred
FROM pg_constraint con
JOIN pg_class tbl ON con.conrelid = tbl.oid
JOIN pg_namespace sch ON tbl.relnamespace = sch.oid
JOIN pg_class ref_tbl ON con.confrelid = ref_tbl.oid
JOIN pg_namespace ref_sch ON ref_tbl.relnamespace = ref_sch.oid
CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS u(attnum, pos)
JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = u.attnum
CROSS JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS ref_u(attnum, pos)
JOIN pg_attribute ref_att
  ON ref_att.attrelid = con.confrelid
 AND ref_att.attnum = ref_u.attnum
 AND ref_u.pos = u.pos
LEFT JOIN information_schema.columns att_col
  ON att_col.table_schema = sch.nspname
 AND att_col.table_name   = tbl.relname
 AND att_col.column_name  = att.attname
WHERE con.contype = 'f'
  AND sch.nspname = 'public'
  AND tbl.relname = $1
GROUP BY
    con.conname,
    sch.nspname,
    tbl.relname,
    ref_sch.nspname,
    ref_tbl.relname,
    con.confdeltype,
    con.confupdtype,
    con.condeferrable,
    con.condeferred
ORDER BY con.conname;