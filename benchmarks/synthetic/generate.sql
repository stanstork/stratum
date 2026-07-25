-- Deterministic data generator for bench.orders.
--
-- Strategy: build a one-million-row sequence table once (cross join of a
-- 10-row digit table), then INSERT ... SELECT from it in 1M-row batches,
-- computing every column as a pure function of the absolute row number `m`.
-- Server-side generation avoids client round trips entirely; expect roughly
-- 100-400k rows/s depending on hardware.
--
-- Invoked by run.sh as:  CALL bench.gen_orders(<total_rows>);
USE bench;

-- Fixed session state so generated values are machine-independent.
SET time_zone = '+00:00';
SET unique_checks = 0;

DROP TABLE IF EXISTS seq_1m;
CREATE TABLE seq_1m (n INT UNSIGNED NOT NULL PRIMARY KEY);

INSERT INTO seq_1m (n)
SELECT d1.d + d2.d * 10 + d3.d * 100 + d4.d * 1000 + d5.d * 10000 + d6.d * 100000
FROM (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d1,
     (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d2,
     (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d3,
     (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d4,
     (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d5,
     (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d6;

DROP PROCEDURE IF EXISTS gen_orders;
DELIMITER //
CREATE PROCEDURE gen_orders(IN total BIGINT)
BEGIN
    DECLARE b BIGINT DEFAULT 0;
    DECLARE batches BIGINT;
    SET time_zone = '+00:00';
    SET batches = CEIL(total / 1000000);

    WHILE b < batches DO
        INSERT INTO orders
            (id, customer_id, status, amount, quantity, discount_pct, currency,
             sku, note, tags, is_gift, order_uuid, created_at, updated_at, ship_date)
        SELECT
            -- m = absolute 0-based row number; id is 1-based
            (b * 1000000 + n) + 1,
            1 + ((b * 1000000 + n) * 2654435761) % 5000000,
            ELT(1 + (b * 1000000 + n) % 5, 'pending', 'paid', 'shipped', 'cancelled', 'refunded'),
            ROUND(0.99 + (((b * 1000000 + n) * 37) % 999900) / 100, 2),
            1 + (b * 1000000 + n) % 20,
            IF((b * 1000000 + n) % 4 = 0, NULL, ROUND(((b * 1000000 + n) % 500) / 10, 1)),
            ELT(1 + (b * 1000000 + n) % 3, 'USD', 'EUR', 'GBP'),
            CONCAT('SKU-', LPAD(((b * 1000000 + n) * 7919) % 10000000, 7, '0')),
            IF((b * 1000000 + n) % 10 < 3, NULL, CONCAT('order note ', MD5(b * 1000000 + n))),
            IF((b * 1000000 + n) % 5 = 0, NULL,
               CONCAT('tag', (b * 1000000 + n) % 50, ',tag', (b * 1000000 + n) % 13)),
            (b * 1000000 + n) % 7 = 0,
            LOWER(CONCAT(
                SUBSTR(MD5(b * 1000000 + n), 1, 8), '-',
                SUBSTR(MD5(b * 1000000 + n), 9, 4), '-',
                SUBSTR(MD5(b * 1000000 + n), 13, 4), '-',
                SUBSTR(MD5(b * 1000000 + n), 17, 4), '-',
                SUBSTR(MD5(b * 1000000 + n), 21, 12))),
            FROM_UNIXTIME(1500000000 + ((b * 1000000 + n) * 97) % 250000000),
            FROM_UNIXTIME(1500000000 + ((b * 1000000 + n) * 131) % 260000000),
            IF((b * 1000000 + n) % 6 = 0, NULL,
               DATE(FROM_UNIXTIME(1500000000 + ((b * 1000000 + n) * 61) % 250000000)))
        FROM seq_1m
        WHERE (b * 1000000 + n) < total;

        SET b = b + 1;
    END WHILE;
END//
DELIMITER ;
