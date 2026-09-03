-- pgloader full Sakila load, MySQL to PostgreSQL. See ../README.md for scope.
-- Copies every row of all 15 data tables (film_text excluded on both sides).
-- SCOPE CAVEAT: pgloader v4 always builds indexes and foreign keys,
-- while paganel builds tables and primary keys only, 
-- so the two are not scope-matched on Sakila. Placeholders come from run.sh.
-- The CAST maps the address.location GEOMETRY column to bytea, since the bench
-- PostgreSQL has no PostGIS (v4 dropped the default geometry cast v3 had).
LOAD DATABASE
     FROM @@SAKILA_MYSQL_URL@@
     INTO @@SAKILA_PG_URL@@

WITH include drop, create tables, reset sequences

CAST type geometry to bytea drop typemod

EXCLUDING TABLE NAMES MATCHING 'film_text'

ALTER SCHEMA 'sakila' RENAME TO 'public'
;
