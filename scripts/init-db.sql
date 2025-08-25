-- PostgreSQL initialization script
-- This ensures the database exists and has proper settings

-- Create database if not exists (this runs only on first bootstrap)
SELECT 'CREATE DATABASE new_self_trading_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'new_self_trading_db')\gexec

-- Connect to the application database
\c new_self_trading_db

-- Set database parameters for better reliability
ALTER DATABASE new_self_trading_db SET synchronous_commit = 'on';
ALTER DATABASE new_self_trading_db SET wal_level = 'replica';
ALTER DATABASE new_self_trading_db SET max_wal_senders = 1;
ALTER DATABASE new_self_trading_db SET wal_keep_size = '512MB';

-- Create a marker table to verify database persistence
CREATE TABLE IF NOT EXISTS db_init_marker (
    id SERIAL PRIMARY KEY,
    initialized_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    version INTEGER DEFAULT 1
);

-- Insert or update marker
INSERT INTO db_init_marker (id, version) 
VALUES (1, 1) 
ON CONFLICT (id) 
DO UPDATE SET initialized_at = NOW(), version = db_init_marker.version + 1;

-- Log initialization
DO $$
DECLARE
    v_version INTEGER;
    v_init_time TIMESTAMP WITH TIME ZONE;
BEGIN
    SELECT version, initialized_at INTO v_version, v_init_time 
    FROM db_init_marker WHERE id = 1;
    
    RAISE NOTICE 'Database initialized. Version: %, Last init: %', v_version, v_init_time;
END $$;
