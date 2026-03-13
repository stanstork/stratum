WITH primary_keys AS (
    SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME
    FROM information_schema.TABLE_CONSTRAINTS tc
    JOIN information_schema.KEY_COLUMN_USAGE kcu
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        AND tc.TABLE_NAME = kcu.TABLE_NAME
    WHERE tc.TABLE_NAME = ?
    AND tc.TABLE_SCHEMA = DATABASE()
    AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
),
unique_constraints AS (
    SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME
    FROM information_schema.TABLE_CONSTRAINTS tc
    JOIN information_schema.KEY_COLUMN_USAGE kcu
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        AND tc.TABLE_NAME = kcu.TABLE_NAME
    WHERE tc.TABLE_NAME = ?
    AND tc.TABLE_SCHEMA = DATABASE()
    AND tc.CONSTRAINT_TYPE = 'UNIQUE'
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
    c.COLUMN_COMMENT AS column_comment,
    c.COLLATION_NAME AS collation_name,
    c.CHARACTER_SET_NAME AS character_set_name,
    (c.EXTRA LIKE '%GENERATED%') AS is_generated,
    (c.EXTRA LIKE '%STORED%') AS is_stored,
    c.GENERATION_EXPRESSION AS generated_expression,
    c.COLUMN_TYPE AS full_column_type
FROM information_schema.COLUMNS c
WHERE c.TABLE_NAME = ?
AND c.TABLE_SCHEMA = DATABASE()
ORDER BY c.ORDINAL_POSITION;
