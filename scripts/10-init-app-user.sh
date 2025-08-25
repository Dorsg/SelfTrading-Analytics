#!/bin/bash
set -euo pipefail

# This script runs once on a fresh Postgres data dir.
# It creates a dedicated, least-privileged DB role for the app and grants
# privileges only on the target database. It never drops or truncates data.

APP_DB="${POSTGRES_DB:-new_self_trading_db}"
APP_USER="app_user"
APP_PASS="${APP_USER_PASSWORD:-$(tr -dc A-Za-z0-9 </dev/urandom | head -c 24)}"

export PGPASSWORD="${POSTGRES_PASSWORD}"

psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" <<SQL
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = '${APP_USER}'
  ) THEN
    CREATE ROLE ${APP_USER} LOGIN PASSWORD '${APP_PASS}';
  END IF;
END$$;

GRANT CONNECT ON DATABASE ${APP_DB} TO ${APP_USER};
\connect ${APP_DB}

-- Ensure schema and object privileges
GRANT USAGE, CREATE ON SCHEMA public TO ${APP_USER};
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ${APP_USER};
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO ${APP_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ${APP_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO ${APP_USER};
SQL

echo "[init] Created/updated role '${APP_USER}'."
echo "APP_USER=${APP_USER}" > /docker-entrypoint-initdb.d/app_user.env
echo "APP_USER_PASSWORD=${APP_PASS}" >> /docker-entrypoint-initdb.d/app_user.env

# Persist a copy inside the data directory so it survives container image updates
echo "APP_USER=${APP_USER}" > /var/lib/postgresql/data/app_user.env
echo "APP_USER_PASSWORD=${APP_PASS}" >> /var/lib/postgresql/data/app_user.env


