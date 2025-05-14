SELECT
    tc.table_name AS referencing_table,
    kcu.column_name AS referencing_column,
    rc.update_rule AS on_update,
    rc.delete_rule AS on_delete
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.referential_constraints rc
    ON rc.constraint_name = tc.constraint_name
    AND rc.constraint_schema = tc.table_schema
WHERE rc.unique_constraint_name IN (
    SELECT constraint_name
    FROM information_schema.table_constraints
    WHERE table_name = $1
      AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')
);
