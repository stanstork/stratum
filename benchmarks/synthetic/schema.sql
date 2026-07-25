-- Synthetic benchmark table: one wide-ish OLTP-style table with a realistic
-- mix of types (integers, decimal, float, enum, char/varchar, nullable text,
-- boolean, uuid-shaped char, timestamp/datetime/date) and NULL sprinkling.
-- Average row width lands around ~200 bytes, i.e. a typical "orders" row.
--
-- All values are DETERMINISTIC functions of the row number (see generate.sql),
-- so any two runs at the same BENCH_ROWS produce byte-identical data.
CREATE DATABASE IF NOT EXISTS bench;

DROP TABLE IF EXISTS bench.orders;
CREATE TABLE bench.orders (
    id           BIGINT UNSIGNED NOT NULL,
    customer_id  INT NOT NULL,
    status       ENUM('pending','paid','shipped','cancelled','refunded') NOT NULL,
    amount       DECIMAL(12,2) NOT NULL,
    quantity     SMALLINT NOT NULL,
    discount_pct FLOAT NULL,
    currency     CHAR(3) NOT NULL,
    sku          VARCHAR(32) NOT NULL,
    note         VARCHAR(255) NULL,
    tags         VARCHAR(64) NULL,
    is_gift      TINYINT(1) NOT NULL,
    order_uuid   CHAR(36) NOT NULL,
    created_at   TIMESTAMP NOT NULL,
    updated_at   DATETIME NOT NULL,
    ship_date    DATE NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB;
