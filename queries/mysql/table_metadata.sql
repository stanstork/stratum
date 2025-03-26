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
    c.ORDINAL_POSITION AS ordinal_position,
    c.COLUMN_NAME AS column_name,
    CAST(c.DATA_TYPE AS CHAR) AS data_type,
    c.IS_NULLABLE AS is_nullable,
    c.COLUMN_DEFAULT IS NOT NULL AS has_default,
    c.COLUMN_DEFAULT AS default_value,
    c.CHARACTER_MAXIMUM_LENGTH AS character_maximum_length,
    c.NUMERIC_PRECISION AS numeric_precision,
    c.NUMERIC_SCALE AS numeric_scale,
    EXISTS (SELECT 1 FROM primary_keys pk WHERE pk.COLUMN_NAME = c.COLUMN_NAME) AS is_primary_key,
    EXISTS (SELECT 1 FROM unique_constraints uq WHERE uq.COLUMN_NAME = c.COLUMN_NAME) AS is_unique,
    c.EXTRA LIKE '%auto_increment%' AS is_auto_increment,
    CAST(fk.referenced_table AS CHAR) AS referenced_table,
    fk.referenced_column AS referenced_column,
    CAST(fk.on_delete AS CHAR) AS on_delete,
    CAST(fk.on_update AS CHAR) AS on_update
FROM information_schema.COLUMNS c
LEFT JOIN foreign_keys fk 
    ON c.COLUMN_NAME = fk.COLUMN_NAME
WHERE c.TABLE_NAME = ?
ORDER BY c.ORDINAL_POSITION;
