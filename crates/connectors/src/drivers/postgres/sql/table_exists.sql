SELECT EXISTS (
    SELECT FROM information_schema.tables
    WHERE  table_schema = $2
    AND    table_name   = $1
)