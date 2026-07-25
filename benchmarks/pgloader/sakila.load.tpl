-- pgloader: full Sakila migration, MySQL -> PostgreSQL.
--
-- Measured scope - identical for both tools (see ../stratum/sakila.smql):
-- create destination tables, copy every row. Secondary indexes and FK
-- constraints are excluded on both sides so the tools do the same work.
--
-- `film_text` is excluded on both sides (an orphan full-text table nothing
-- references). @@ placeholders are substituted by run.sh.

LOAD DATABASE
     FROM @@SAKILA_MYSQL_URL@@
     INTO @@SAKILA_PG_URL@@

WITH include drop, create tables, create no indexes, no foreign keys, reset sequences

EXCLUDING TABLE NAMES MATCHING 'film_text'

ALTER SCHEMA 'sakila' RENAME TO 'public'
;
