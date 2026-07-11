WITH RECURSIVE root_of AS (
    SELECT c.oid AS rel, c.oid AS root
    FROM pg_class c
    WHERE c.relkind IN ('r', 'p')
      AND NOT c.relispartition

    UNION ALL

    SELECT c.oid, r.root
    FROM pg_class c
    JOIN pg_inherits i ON i.inhrelid = c.oid
    JOIN root_of r ON r.rel = i.inhparent
)
SELECT DISTINCT rt.relname AS referencing_table
FROM pg_constraint con
JOIN root_of ro ON ro.rel = con.conrelid
JOIN pg_class rt ON rt.oid = ro.root
-- to_regclass (rather than a ::regclass cast) yields NULL instead of erroring
-- when the table does not exist, so an unknown name simply returns no rows.
WHERE con.contype = 'f'
  AND con.confrelid = to_regclass($1::text)
ORDER BY referencing_table;
