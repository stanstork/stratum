SELECT EXISTS (
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'test'
    AND table_name = $1
)