#!/bin/bash

# Simple script to show XRAY PostgreSQL database configuration
# Usage: ./show_xray_db_config_simple.sh [--env-file <path>]

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Default values
XRAY_ENV_FILE="/home/nutanix/bin/xray_set_env"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --env-file)
            XRAY_ENV_FILE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [--env-file <path>]"
            echo "  --env-file <path>  Path to XRAY environment file (default: /home/nutanix/bin/xray_set_env)"
            echo "  -h, --help        Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}XRAY Database Configuration (Simple)${NC}"

# Source environment
source "$XRAY_ENV_FILE" 2>/dev/null || {
    echo -e "${RED}ERROR: Cannot source XRAY environment from: $XRAY_ENV_FILE${NC}"
    exit 1
}

# Check if variable is set
if [[ -z "$XRAY_PG_DB_NAME" ]]; then
    echo -e "${RED}ERROR: XRAY_PG_DB_NAME not set${NC}"
    exit 1
fi

echo "Database: $XRAY_PG_DB_NAME"
if [[ -n "$XRAY_PG_DB_TS" ]]; then
    echo "Expected Tablespace: $XRAY_PG_DB_TS"
fi

# Check if database exists
if ! sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$XRAY_PG_DB_NAME"; then
    echo -e "${RED}ERROR: Database '$XRAY_PG_DB_NAME' does not exist${NC}"
    exit 1
fi

# Get basic database info
echo -e "${GREEN}Database Status: EXISTS${NC}"

# Get database size and tablespace
DB_INFO=$(sudo -u postgres psql -t -c "
SELECT 
    d.datname,
    pg_size_pretty(pg_database_size(d.datname)) as size,
    COALESCE(t.spcname, 'pg_default') as tablespace,
    COALESCE(pg_tablespace_location(t.oid), 'default location') as location
FROM pg_database d 
LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid 
WHERE d.datname = '$XRAY_PG_DB_NAME';
" 2>/dev/null)

if [[ -n "$DB_INFO" ]]; then
    echo "$DB_INFO" | while IFS='|' read -r db_name size tablespace location; do
        echo "Size: $(echo $size | xargs)"
        echo "Tablespace: $(echo $tablespace | xargs)"
        echo "Tablespace Location: $(echo $location | xargs)"
    done
fi

# Get PostgreSQL data directory and WAL location
DATA_DIR=$(sudo -u postgres psql -t -c "SHOW data_directory;" 2>/dev/null | xargs)
if [[ -n "$DATA_DIR" ]]; then
    echo "Data Directory: $DATA_DIR"
    echo "WAL Directory: $DATA_DIR/pg_wal"
    
    # Check if WAL directory is a symlink
    if sudo test -L "$DATA_DIR/pg_wal"; then
        WAL_TARGET=$(sudo readlink -f "$DATA_DIR/pg_wal" 2>/dev/null || echo "Unknown")
        echo "WAL Target: $WAL_TARGET"
    fi
fi

# Get table count
TABLE_COUNT=$(sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -t -c "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public';" 2>/dev/null | xargs || echo "Unknown")
echo "Tables: $TABLE_COUNT"

# Get key PostgreSQL settings including shared_buffers
SHARED_BUFFERS=$(sudo -u postgres psql -t -c "SHOW shared_buffers;" 2>/dev/null | xargs || echo "Unknown")
echo "Shared Buffers: $SHARED_BUFFERS"

# Check tablespace match
if [[ -n "$XRAY_PG_DB_TS" ]]; then
    ACTUAL_TABLESPACE=$(echo "$DB_INFO" | cut -d'|' -f3 | xargs)
    if [[ "$ACTUAL_TABLESPACE" == "$XRAY_PG_DB_TS" ]]; then
        echo -e "${GREEN}Tablespace Match: ✓ YES${NC}"
    else
        echo -e "${YELLOW}Tablespace Match: ✗ NO (Expected: $XRAY_PG_DB_TS, Actual: $ACTUAL_TABLESPACE)${NC}"
    fi
fi

echo -e "${GREEN}Configuration check completed${NC}"