SELECT (data_length + index_length) as size_bytes
FROM information_schema.TABLES
WHERE table_schema = DATABASE() AND table_name = ?