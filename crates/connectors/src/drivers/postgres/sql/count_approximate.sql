WITH sample AS (
  SELECT 1
  FROM {schema}.{table} TABLESAMPLE SYSTEM (1)
),
total AS (
  SELECT reltuples
  FROM pg_class
  WHERE oid = '{schema}.{table}'::regclass
)
SELECT
  (SELECT COUNT(*) FROM sample) * 100 AS sampled_estimate,
  (SELECT reltuples FROM total)       AS stats_estimate;
