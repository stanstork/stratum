WITH index_details AS (
    SELECT
        i.schemaname AS schema_name,
        i.tablename AS table_name,
        i.indexname AS index_name,
        ix.indisunique AS is_unique,
        ix.indisprimary AS is_primary,
        am.amname AS index_type,
        pg_get_expr(ix.indpred, ix.indrelid) AS index_condition,
        ts.spcname AS tablespace,
        c.reloptions AS options,
        pg_relation_size(quote_ident(i.schemaname) || '.' || quote_ident(i.indexname)) AS size_bytes,
        obj_description(idx.oid, 'pg_class') AS comment
    FROM pg_indexes i
    JOIN pg_class idx ON idx.relname = i.indexname
    JOIN pg_index ix ON ix.indexrelid = idx.oid
    JOIN pg_class tbl ON tbl.oid = ix.indrelid
    JOIN pg_am am ON am.oid = idx.relam
    LEFT JOIN pg_tablespace ts ON ts.oid = idx.reltablespace
    LEFT JOIN pg_class c ON c.oid = idx.oid
    WHERE i.schemaname = $1  -- parameter: schema name
      AND i.tablename = $2   -- parameter: table name
),
index_columns AS (
    SELECT
        i.schemaname AS schema_name,
        i.tablename AS table_name,
        i.indexname AS index_name,
        a.attnum,
        CASE 
            WHEN a.attnum > 0 THEN a.attname
            ELSE pg_get_indexdef(idx.oid, a.attnum, true)  -- expression
        END AS column_name,
        CASE 
            WHEN opt.option & 1 = 1 THEN 'DESC'
            ELSE 'ASC'
        END AS sort_order,
        CASE 
            WHEN opt.option & 2 = 2 THEN 'FIRST'
            WHEN opt.option & 4 = 4 THEN 'LAST'
            ELSE 'DEFAULT'
        END AS nulls_order,
        CASE 
            WHEN a.attnum > 0 THEN false
            ELSE true
        END AS is_expression,
        opc.opcname AS opclass,
        NULL::integer AS prefix_length  -- Not applicable to PostgreSQL
    FROM pg_indexes i
    JOIN pg_class idx ON idx.relname = i.indexname
    JOIN pg_index ix ON ix.indexrelid = idx.oid
    CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS keys(attnum, ord)
    LEFT JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = keys.attnum
    LEFT JOIN LATERAL unnest(ix.indoption) WITH ORDINALITY AS opt(option, ord_opt) 
        ON opt.ord_opt = keys.ord
    LEFT JOIN LATERAL unnest(ix.indclass) WITH ORDINALITY AS cls(opcoid, ord_cls) 
        ON cls.ord_cls = keys.ord
    LEFT JOIN pg_opclass opc ON opc.oid = cls.opcoid
    WHERE i.schemaname = $1
      AND i.tablename = $2
    ORDER BY i.indexname, keys.ord
)
SELECT 
    d.schema_name,
    d.table_name,
    d.index_name,
    d.is_unique,
    d.is_primary,
    d.index_type,
    d.index_condition,
    d.tablespace,
    -- Extract fill_factor from reloptions
    (SELECT CAST(regexp_replace(unnest, '.*=', '') AS INTEGER)
     FROM unnest(d.options)
     WHERE unnest LIKE 'fillfactor=%') AS fill_factor,
    d.size_bytes,
    d.comment,
    -- Aggregate columns with metadata as JSON
    json_agg(
        json_build_object(
            'name', ic.column_name,
            'sort_order', ic.sort_order,
            'nulls_order', ic.nulls_order,
            'is_expression', ic.is_expression,
            'opclass', ic.opclass,
            'prefix_length', ic.prefix_length
        ) ORDER BY ic.attnum
    ) AS columns
FROM index_details d
JOIN index_columns ic 
    ON ic.schema_name = d.schema_name 
    AND ic.table_name = d.table_name 
    AND ic.index_name = d.index_name
GROUP BY 
    d.schema_name, d.table_name, d.index_name,
    d.is_unique, d.is_primary, d.index_type,
    d.index_condition, d.tablespace, d.options,
    d.size_bytes, d.comment;
