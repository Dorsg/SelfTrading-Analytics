#!/bin/bash

# Database backup script for SelfTrading
# This script creates regular backups to prevent data loss

set -e

BACKUP_DIR="/root/SelfTrading/backups/db"
DB_NAME="new_self_trading_db"
DB_USER="postgres"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="$BACKUP_DIR/selftrading_backup_$TIMESTAMP.sql"
RETENTION_DAYS=7

# Ensure backup directory exists
mkdir -p "$BACKUP_DIR"

echo "Starting database backup at $(date)"

# Create backup
docker exec trading-db pg_dump -U "$DB_USER" -d "$DB_NAME" > "$BACKUP_FILE"

if [ $? -eq 0 ]; then
    echo "Database backup completed successfully: $BACKUP_FILE"
    gzip "$BACKUP_FILE"
    echo "Backup compressed: $BACKUP_FILE.gz"
else
    echo "ERROR: Database backup failed!"
    exit 1
fi

# Clean up old backups (keep only last 7 days)
find "$BACKUP_DIR" -name "selftrading_backup_*.sql.gz" -mtime +$RETENTION_DAYS -delete
echo "Old backups cleaned up (retention: $RETENTION_DAYS days)"

echo "Backup process completed at $(date)"




