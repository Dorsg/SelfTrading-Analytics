#!/bin/bash
# Startup safeguard script to ensure database integrity

set -e

echo "=== Database Startup Safeguard ==="
echo "Timestamp: $(date)"

# Wait for database to be ready
echo "Waiting for database to be ready..."
until pg_isready -h db -p 5432 -U postgres; do
  echo "Database is not ready - sleeping"
  sleep 2
done

echo "Database server is ready"

# Run Python safeguard script
echo "Running database initialization safeguard..."
python -m backend.database.db_init_safeguard

echo "=== Safeguard completed successfully ==="
