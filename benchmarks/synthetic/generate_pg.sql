-- Deterministic synthetic generator for a PostgreSQL `orders` SOURCE table,
-- used by the PG -> MySQL reverse benchmark. Mirrors synthetic/schema.sql's 
-- column mix with native PostgreSQL types. Every value is a pure function 
-- of the row number, so a given size reproduces the same table byte-for-byte.
--
-- Invoked by run.sh as:  psql ... -v rows=<N> -f generate_pg.sql   (db: bench_src)
-- One INSERT ... SELECT generate_series(): server-side, no client round trips.
\set ON_ERROR_STOP on

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    id           BIGINT NOT NULL,
    customer_id  INTEGER NOT NULL,
    status       VARCHAR(16) NOT NULL,
    amount       NUMERIC(12,2) NOT NULL,
    quantity     SMALLINT NOT NULL,
    discount_pct DOUBLE PRECISION,
    currency     CHAR(3) NOT NULL,
    sku          VARCHAR(32) NOT NULL,
    note         VARCHAR(255),
    tags         VARCHAR(64),
    is_gift      SMALLINT NOT NULL,
    order_uuid   CHAR(36) NOT NULL,
    created_at   TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL,
    ship_date    DATE,
    PRIMARY KEY (id)
);

INSERT INTO orders
SELECT
    m                                                        AS id,
    1 + (m * 2654435761) % 5000000                           AS customer_id,
    (ARRAY['pending','paid','shipped','cancelled','refunded'])[1 + m % 5] AS status,
    ROUND((0.99 + ((m * 37) % 999900) / 100.0)::numeric, 2)  AS amount,
    (1 + m % 20)::smallint                                   AS quantity,
    CASE WHEN m % 4 = 0 THEN NULL
         ELSE ROUND(((m % 500) / 10.0)::numeric, 1)::double precision END AS discount_pct,
    (ARRAY['USD','EUR','GBP'])[1 + m % 3]                    AS currency,
    'SKU-' || lpad(((m * 7919) % 10000000)::text, 7, '0')    AS sku,
    CASE WHEN m % 10 < 3 THEN NULL
         ELSE 'order note ' || md5(m::text) END              AS note,
    CASE WHEN m % 5 = 0 THEN NULL
         ELSE 'tag' || (m % 50) || ',tag' || (m % 13) END    AS tags,
    (m % 7 = 0)::int::smallint                               AS is_gift,
    lower(
        substr(md5(m::text), 1, 8)  || '-' ||
        substr(md5(m::text), 9, 4)  || '-' ||
        substr(md5(m::text), 13, 4) || '-' ||
        substr(md5(m::text), 17, 4) || '-' ||
        substr(md5(m::text), 21, 12)
    )                                                        AS order_uuid,
    (to_timestamp(1500000000 + (m * 97)  % 250000000) AT TIME ZONE 'UTC') AS created_at,
    (to_timestamp(1500000000 + (m * 131) % 260000000) AT TIME ZONE 'UTC') AS updated_at,
    CASE WHEN m % 6 = 0 THEN NULL
         ELSE (to_timestamp(1500000000 + (m * 61) % 250000000) AT TIME ZONE 'UTC')::date END AS ship_date
-- `m` must be bigint: with an int4 series, products like `m * 7919` overflow
-- int4 (> 2.1e9) past ~271k rows ("integer out of range").
FROM generate_series(1::bigint, (:rows)::bigint) AS g(m);
