-- This query checks for various MySQL server capabilities.
-- Note: '1' indicates true/available, '0' indicates false/unavailable.

-- 'copy_streaming': Checks for LOAD DATA INFILE. This is a core feature.
SELECT
    'copy_streaming' AS capability,
    1 AS enabled
UNION ALL
-- 'upsert_native': Checks for INSERT...ON DUPLICATE KEY UPDATE or REPLACE. These are core features.
SELECT
    'upsert_native' AS capability,
    1 AS enabled
UNION ALL
-- 'transactions': Checks if the InnoDB storage engine (which provides transactions) is available.
SELECT
    'transactions' AS capability,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM information_schema.ENGINES
            WHERE ENGINE = 'InnoDB' AND SUPPORT IN ('YES', 'DEFAULT')
        ) THEN 1
        ELSE 0
    END AS enabled
UNION ALL
-- 'merge_statements': Checks for ANSI MERGE support. MySQL does not support this.
SELECT
    'merge_statements' AS capability,
    0 AS enabled
UNION ALL
-- 'ddl_online': Checks for Online DDL capabilities, which were significantly enhanced in MySQL 5.6.
SELECT
    'ddl_online' AS capability,
    CASE
        -- Check if major version > 5
        WHEN SUBSTRING_INDEX(VERSION(), '.', 1) + 0 > 5 THEN 1
        -- Check if major version = 5 and minor version >= 6
        WHEN SUBSTRING_INDEX(VERSION(), '.', 1) + 0 = 5
             AND SUBSTRING_INDEX(SUBSTRING_INDEX(VERSION(), '.', 2), '.', -1) + 0 >= 6 THEN 1
        ELSE 0
    END AS enabled
UNION ALL
-- 'temp_tables': Checks for CREATE TEMPORARY TABLE support. This is a core feature.
SELECT
    'temp_tables' AS capability,
    1 AS enabled;