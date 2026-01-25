#!/bin/bash

# Simple script to restore XRAY PostgreSQL database from backup
# Usage: ./restore_xray_db_simple.sh [--env-file <path>] [backup_path]
# Based on original: sudo -u postgres pg_restore -j 8 -F d /mnt/pgbackup/tpcc/ -d $XRAY_PG_DB_NAME

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Default values
XRAY_ENV_FILE="/home/nutanix/bin/xray_set_env"
BACKUP_PATH="/mnt/pgbackup/tpcc"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --env-file)
            XRAY_ENV_FILE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [--env-file <path>] [backup_path]"
            echo "  --env-file <path>  Path to XRAY environment file (default: /home/nutanix/bin/xray_set_env)"
            echo "  backup_path       Path to backup directory (default: /mnt/pgbackup/tpcc)"
            echo "  -h, --help        Show this help message"
            exit 0
            ;;
        *)
            # Assume it's the backup path if no other options match
            BACKUP_PATH="$1"
            shift
            ;;
    esac
done

echo -e "${BLUE}XRAY Database Restore (Simple Mode)${NC}"
echo "Backup path: $BACKUP_PATH"

# Source environment
source "$XRAY_ENV_FILE" 2>/dev/null || {
    echo -e "${RED}ERROR: Cannot source XRAY environment from: $XRAY_ENV_FILE${NC}"
    exit 1
}

# Check if variables are set
if [[ -z "$XRAY_PG_DB_NAME" ]]; then
    echo -e "${RED}ERROR: XRAY_PG_DB_NAME not set${NC}"
    exit 1
fi

echo "Target database: $XRAY_PG_DB_NAME"

# Check tablespace variable
if [[ -n "$XRAY_PG_DB_TS" ]]; then
    echo "Target tablespace: $XRAY_PG_DB_TS"
    # Verify tablespace exists
    if ! sudo -u postgres psql -t -c "SELECT 1 FROM pg_tablespace WHERE spcname = '$XRAY_PG_DB_TS';" | grep -q 1; then
        echo -e "${RED}ERROR: Tablespace '$XRAY_PG_DB_TS' does not exist${NC}"
        exit 1
    fi
    TABLESPACE_CLAUSE="TABLESPACE $XRAY_PG_DB_TS"
else
    echo -e "${YELLOW}WARNING: XRAY_PG_DB_TS not set - using default tablespace${NC}"
    TABLESPACE_CLAUSE=""
fi

# Validate backup
if [[ ! -d "$BACKUP_PATH" ]]; then
    echo -e "${RED}ERROR: Backup directory not found: $BACKUP_PATH${NC}"
    exit 1
fi

if ! sudo test -f "$BACKUP_PATH/toc.dat"; then
    echo -e "${RED}ERROR: Invalid backup format (missing toc.dat)${NC}"
    exit 1
fi

# Check if database exists
if ! sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$XRAY_PG_DB_NAME"; then
    echo -e "${YELLOW}Database '$XRAY_PG_DB_NAME' does not exist - creating it...${NC}"
    
    if [[ -n "$TABLESPACE_CLAUSE" ]]; then
        CREATE_CMD="CREATE DATABASE \"$XRAY_PG_DB_NAME\" $TABLESPACE_CLAUSE;"
        echo "Creating with tablespace: $XRAY_PG_DB_TS"
    else
        CREATE_CMD="CREATE DATABASE \"$XRAY_PG_DB_NAME\";"
    fi
    
    if ! sudo -u postgres psql -c "$CREATE_CMD"; then
        echo -e "${RED}ERROR: Failed to create database '$XRAY_PG_DB_NAME'${NC}"
        exit 1
    fi
    echo "Database created successfully"
fi

# Terminate connections
echo "Terminating active connections..."
sudo -u postgres psql -c "
SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE datname = '$XRAY_PG_DB_NAME' 
  AND pid <> pg_backend_pid();" >/dev/null 2>&1 || true

sleep 2

# Perform restore
echo "Starting restore..."
START_TIME=$(date +%s)

if sudo -u postgres pg_restore -j 8 -F d "$BACKUP_PATH" -d "$XRAY_PG_DB_NAME"; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    MINUTES=$((DURATION / 60))
    SECONDS=$((DURATION % 60))
    
    echo -e "${GREEN}SUCCESS: Database restored in ${MINUTES}m ${SECONDS}s${NC}"
    
    # Quick verification
    TABLES=$(sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | xargs || echo "0")
    echo "Tables restored: $TABLES"
    if [[ -n "$XRAY_PG_DB_TS" ]]; then
        echo "Tablespace used: $XRAY_PG_DB_TS"
    fi
    
    # Show table sizes and row counts
    echo -e "${GREEN}Table sizes and row counts:${NC}"
    sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -c "
    SELECT 
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
        pg_total_relation_size(schemaname||'.'||tablename) AS size_bytes,
        (SELECT reltuples::bigint FROM pg_class WHERE relname = tablename) AS estimated_rows
    FROM pg_tables 
    WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
    " 2>/dev/null || echo "Could not retrieve table sizes"
    
    exit 0
else
    echo -e "${RED}FAILED: Database restore failed${NC}"
    exit 1
fi