-- Scratch MySQL destination for the source x destination test matrix.
-- Emptied by each test; never seeded.
CREATE DATABASE IF NOT EXISTS stratum_dest;
CREATE USER IF NOT EXISTS 'user'@'%' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON stratum_dest.* TO 'user'@'%';
FLUSH PRIVILEGES;
