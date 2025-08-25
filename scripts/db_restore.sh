#!/bin/bash

# Database restore script for SelfTrading
# Usage: ./db_restore.sh <backup_file.sql.gz>

set -e

DB_NAME="new_self_trading_db"
DB_USER="postgres"

if [ $# -eq 0 ]; then
    echo "Usage: $0 <backup_file.sql.gz>"
    echo "Available backups:"
    ls -la /root/SelfTrading/backups/db/selftrading_backup_*.sql.gz 2>/dev/null || echo "No backups found"
    exit 1
fi

BACKUP_FILE="$1"

if [ ! -f "$BACKUP_FILE" ]; then
    echo "ERROR: Backup file '$BACKUP_FILE' not found!"
    exit 1
fi

echo "WARNING: This will completely replace the current database with the backup!"
echo "Backup file: $BACKUP_FILE"
echo "Target database: $DB_NAME"
read -p "Are you sure you want to continue? (y/N): " confirm

if [[ ! $confirm =~ ^[Yy]$ ]]; then
    echo "Restore cancelled."
    exit 0
fi

echo "Starting database restore at $(date)"

# Stop dependent services
echo "Stopping dependent services..."
docker-compose -f docker-compose.prod.yml stop api_gateway scheduler db-guardian

# Drop and recreate database
echo "Recreating database..."
docker exec trading-db psql -U "$DB_USER" -c "DROP DATABASE IF EXISTS $DB_NAME;"
docker exec trading-db psql -U "$DB_USER" -c "CREATE DATABASE $DB_NAME;"

# Restore from backup
echo "Restoring from backup..."
if [[ "$BACKUP_FILE" == *.gz ]]; then
    gunzip -c "$BACKUP_FILE" | docker exec -i trading-db psql -U "$DB_USER" -d "$DB_NAME"
else
    docker exec -i trading-db psql -U "$DB_USER" -d "$DB_NAME" < "$BACKUP_FILE"
fi

if [ $? -eq 0 ]; then
    echo "Database restore completed successfully!"
else
    echo "ERROR: Database restore failed!"
    exit 1
fi

# Restart services
echo "Restarting services..."
docker-compose -f docker-compose.prod.yml start

echo "Restore process completed at $(date)"
echo "Please verify the application is working correctly."




