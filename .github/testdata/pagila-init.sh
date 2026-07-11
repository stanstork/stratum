#!/bin/bash
# Load the Pagila sample database into a `pagila` database.
#
# Runs from the Postgres image's initdb hook, where $POSTGRES_USER is the
# superuser. The dumps assign ownership to a `postgres` role, so create it first.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-SQL
	DO \$\$ BEGIN
	  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'postgres') THEN
	    CREATE ROLE postgres SUPERUSER LOGIN;
	  END IF;
	END \$\$;
	CREATE DATABASE pagila OWNER "$POSTGRES_USER";
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname pagila -f /pagila-schema.sql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname pagila -f /pagila-data.sql
