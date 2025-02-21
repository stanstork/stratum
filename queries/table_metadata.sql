WITH primary_keys AS (
    SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME
    FROM information_schema.TABLE_CONSTRAINTS tc
    JOIN information_schema.KEY_COLUMN_USAGE kcu 
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        AND tc.TABLE_NAME = kcu.TABLE_NAME
    WHERE tc.TABLE_NAME = ? 
    AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
),
unique_constraints AS (
    SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME
    FROM information_schema.TABLE_CONSTRAINTS tc
    JOIN information_schema.KEY_COLUMN_USAGE kcu 
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        AND tc.TABLE_NAME = kcu.TABLE_NAME
    WHERE tc.TABLE_NAME = ?
    AND tc.CONSTRAINT_TYPE = 'UNIQUE'
),
foreign_keys AS (
    SELECT 
        kcu.COLUMN_NAME, 
        kcu.REFERENCED_TABLE_NAME AS referenced_table, 
        kcu.REFERENCED_COLUMN_NAME AS referenced_column,
        rc.DELETE_RULE AS on_delete,
        rc.UPDATE_RULE AS on_update
    FROM information_schema.KEY_COLUMN_USAGE kcu
    JOIN information_schema.REFERENTIAL_CONSTRAINTS rc 
        ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
        AND kcu.TABLE_NAME = rc.TABLE_NAME
    WHERE kcu.TABLE_NAME = ?
)
SELECT 
    c.ORDINAL_POSITION,
    c.COLUMN_NAME, 
    CAST(c.DATA_TYPE AS CHAR) AS DATA_TYPE, 
    c.IS_NULLABLE = 'YES' AS IS_NULLABLE,
    c.COLUMN_DEFAULT IS NOT NULL AS HAS_DEFAULT,
    c.COLUMN_DEFAULT,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.NUMERIC_SCALE,
    EXISTS (SELECT 1 FROM primary_keys pk WHERE pk.COLUMN_NAME = c.COLUMN_NAME) AS IS_PRIMARY_KEY,
    EXISTS (SELECT 1 FROM unique_constraints uq WHERE uq.COLUMN_NAME = c.COLUMN_NAME) AS IS_UNIQUE,
    c.EXTRA LIKE '%auto_increment%' AS IS_AUTO_INCREMENT,
    CAST(fk.referenced_table AS CHAR) AS REFERENCED_TABLE,
    fk.referenced_column AS REFERENCED_COLUMN,
    CAST(fk.on_delete AS CHAR) AS ON_DELETE,
    CAST(fk.on_update AS CHAR) AS ON_UPDATE
FROM information_schema.COLUMNS c
LEFT JOIN foreign_keys fk 
    ON c.COLUMN_NAME = fk.COLUMN_NAME
WHERE c.TABLE_NAME = ?
ORDER BY c.ORDINAL_POSITION;
