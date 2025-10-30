SELECT 
    CAST(kcu.TABLE_NAME AS CHAR) AS referencing_table,
    CAST(kcu.COLUMN_NAME AS CHAR) AS referencing_column,
    rc.UPDATE_RULE AS on_update,
    rc.DELETE_RULE AS on_delete
FROM information_schema.KEY_COLUMN_USAGE kcu
JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
    ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
    AND kcu.TABLE_NAME = rc.TABLE_NAME
WHERE kcu.REFERENCED_TABLE_NAME = ?
