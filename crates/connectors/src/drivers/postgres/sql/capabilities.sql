-- This query checks for various PostgreSQL server capabilities.
-- Note: '1' indicates true/available, '0' indicates false/unavailable.

-- 'copy_streaming': Checks for COPY ... FROM STDIN. This is a core feature.
SELECT
    'copy_streaming' AS capability,
    1 AS enabled
UNION ALL
-- 'upsert_native': Checks for INSERT ... ON CONFLICT (added in 9.5).
SELECT
    'upsert_native' AS capability,
    CASE
        WHEN current_setting('server_version_num')::int >= 90500 THEN 1
        ELSE 0
    END AS enabled
UNION ALL
-- 'transactions': PostgreSQL is fully transactional. This is a core feature.
SELECT
    'transactions' AS capability,
    1 AS enabled
UNION ALL
-- 'merge_statements': Checks for ANSI MERGE support (added in 15).
SELECT
    'merge_statements' AS capability,
    CASE
        WHEN current_setting('server_version_num')::int >= 150000 THEN 1
        ELSE 0
    END AS enabled
UNION ALL
-- 'ddl_online': Checks for non-blocking DDL (e.g., transactional DDL, CREATE INDEX CONCURRENTLY).
-- This is a core design feature of PostgreSQL.
SELECT
    'ddl_online' AS capability,
    1 AS enabled
UNION ALL
-- 'temp_tables': Checks for CREATE TEMPORARY TABLE support. This is a core feature.
SELECT
    'temp_tables' AS capability,
    1 AS enabled;