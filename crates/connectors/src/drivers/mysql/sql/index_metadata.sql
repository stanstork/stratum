SELECT
    INDEX_NAME as index_name,
    COLUMN_NAME as column_name,
    CASE WHEN NON_UNIQUE = 0 THEN true ELSE false END as is_unique,
    CASE WHEN INDEX_NAME = 'PRIMARY' THEN true ELSE false END as is_primary,
    INDEX_TYPE as index_type,
    CASE WHEN COLLATION = 'D' THEN 'Desc' ELSE 'Asc' END as sort_order
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = ?
ORDER BY INDEX_NAME, SEQ_IN_INDEX