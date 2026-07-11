WITH target AS (
    SELECT c.oid, c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $2::text
      AND c.relname = $1::text
),
own_fks AS (
    SELECT con.oid
    FROM pg_constraint con
    JOIN target t ON con.conrelid = t.oid
    WHERE con.contype = 'f'
),
representative AS (
    SELECT i.inhrelid AS oid
    FROM pg_inherits i
    JOIN target t ON i.inhparent = t.oid
    WHERE NOT EXISTS (SELECT 1 FROM own_fks)
    ORDER BY i.inhrelid
    LIMIT 1
),
source_rel AS (
    SELECT oid FROM target
    UNION ALL
    SELECT oid FROM representative
),
fk AS (
    SELECT
        con.conname,
        con.conrelid,
        con.confrelid,
        con.confdeltype,
        con.confupdtype,
        con.condeferrable,
        con.condeferred,
        STRING_AGG(att.attname, ',' ORDER BY u.pos) AS columns,
        STRING_AGG(ref_att.attname, ',' ORDER BY u.pos) AS referenced_columns,
        STRING_AGG(att.attname, '_' ORDER BY u.pos) AS column_slug
    FROM pg_constraint con
    JOIN source_rel s ON con.conrelid = s.oid
    CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS u(attnum, pos)
    JOIN pg_attribute att
      ON att.attrelid = con.conrelid AND att.attnum = u.attnum
    CROSS JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS ref_u(attnum, pos)
    JOIN pg_attribute ref_att
      ON ref_att.attrelid = con.confrelid
     AND ref_att.attnum = ref_u.attnum
     AND ref_u.pos = u.pos
    WHERE con.contype = 'f'
    GROUP BY
        con.oid, con.conname, con.conrelid, con.confrelid,
        con.confdeltype, con.confupdtype, con.condeferrable, con.condeferred
)
SELECT
    CASE
        WHEN fk.conrelid = (SELECT oid FROM target) THEN fk.conname
        ELSE $1::text || '_' || fk.column_slug || '_fkey'
    END AS constraint_name,
    $2::text AS schema_name,
    $1::text AS table_name,
    fk.columns,
    ref_sch.nspname AS referenced_schema,
    ref_tbl.relname AS referenced_table,
    fk.referenced_columns,
    CASE fk.confdeltype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
    END AS on_delete,
    CASE fk.confupdtype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
    END AS on_update,
    COALESCE(nl.is_nullable, false) AS is_nullable,
    fk.condeferrable AS is_deferrable,
    fk.condeferred AS initially_deferred
FROM fk
JOIN pg_class ref_tbl ON ref_tbl.oid = fk.confrelid
JOIN pg_namespace ref_sch ON ref_sch.oid = ref_tbl.relnamespace
-- Nullability is taken from the parent's columns, not the partition's.
LEFT JOIN LATERAL (
    SELECT bool_or(col.is_nullable = 'YES') AS is_nullable
    FROM unnest(string_to_array(fk.columns, ',')) AS x(colname)
    JOIN information_schema.columns col
      ON col.table_schema = $2::text
     AND col.table_name = $1::text
     AND col.column_name = x.colname
) nl ON true
ORDER BY constraint_name;
