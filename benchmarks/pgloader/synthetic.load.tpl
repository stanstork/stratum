-- pgloader: synthetic single-table load, MySQL -> PostgreSQL.
--
-- Mirrors ../stratum/synthetic.smql: create the destination table and copy
-- the data (one table, PK only - no secondary indexes or FKs exist).
-- @@ placeholders are substituted by run.sh.

LOAD DATABASE
     FROM @@SYNTH_MYSQL_URL@@
     INTO @@SYNTH_PG_URL@@

WITH include drop, create tables, create indexes, reset sequences

ALTER SCHEMA 'bench' RENAME TO 'public'
;
