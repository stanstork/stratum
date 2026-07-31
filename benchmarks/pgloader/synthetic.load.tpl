-- pgloader synthetic load, MySQL to PostgreSQL. See ../README.md for scope.
-- Scoped to the single orders table (INCLUDING ONLY) so pgloader copies the
-- same one table stratum does. Placeholders are substituted by run.sh.
LOAD DATABASE
     FROM @@SYNTH_MYSQL_URL@@
     INTO @@SYNTH_PG_URL@@

WITH include drop, create tables, reset sequences

INCLUDING ONLY TABLE NAMES MATCHING ~/^orders$/

ALTER SCHEMA 'bench' RENAME TO 'public'
;
