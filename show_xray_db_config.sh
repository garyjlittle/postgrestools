#!/bin/bash

# Script to show current configuration of XRAY PostgreSQL database
# Author: Generated script
# Usage: ./show_xray_db_config.sh [--env-file <path>]

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

print_section() {
    local title=$1
    echo
    print_status $CYAN "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    print_status $CYAN "${BOLD}$title${NC}"
    print_status $CYAN "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

print_header() {
    echo "=================================================="
    print_status $BLUE "${BOLD}XRAY PostgreSQL Database Configuration${NC}"
    echo "=================================================="
    echo
}

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

print_header

# Source the XRAY environment variables
if [[ -f "$XRAY_ENV_FILE" ]]; then
    source "$XRAY_ENV_FILE"
    print_status $GREEN "✓ Sourced XRAY environment variables from: $XRAY_ENV_FILE"
else
    print_status $RED "✗ XRAY environment file not found at: $XRAY_ENV_FILE"
    exit 1
fi

# Check if XRAY_PG_DB_NAME is set
if [[ -z "$XRAY_PG_DB_NAME" ]]; then
    print_status $RED "✗ XRAY_PG_DB_NAME environment variable is not set"
    exit 1
fi

echo
print_status $BLUE "Target Database: ${BOLD}$XRAY_PG_DB_NAME${NC}"
if [[ -n "$XRAY_PG_DB_TS" ]]; then
    print_status $BLUE "Expected Tablespace: ${BOLD}$XRAY_PG_DB_TS${NC}"
fi

# Check if PostgreSQL is running
print_section "PostgreSQL Service Status"
if pgrep -x "postgres" > /dev/null; then
    print_status $GREEN "✓ PostgreSQL is running"
    
    # Get PostgreSQL version and basic info
    PG_VERSION=$(sudo -u postgres psql -h "$XRAY_DB_SERVER" -t -c "SELECT version();" 2>/dev/null | xargs || echo "Unknown")
    print_status $BLUE "PostgreSQL Version: $PG_VERSION"
    
    # Get PostgreSQL process info
    PG_PROCESSES=$(pgrep -x "postgres" | wc -l)
    print_status $BLUE "Active processes: $PG_PROCESSES"
else
    print_status $RED "✗ PostgreSQL is not running"
    exit 1
fi

# Check if database exists
print_section "Database Existence and Basic Info"
if sudo -u postgres psql -h "$XRAY_DB_SERVER" -lqt | cut -d \| -f 1 | grep -qw "$XRAY_PG_DB_NAME"; then
    print_status $GREEN "✓ Database '$XRAY_PG_DB_NAME' exists"
    
    # Get database basic information
    DB_INFO=$(sudo -u postgres psql -h "$XRAY_DB_SERVER" -d "$XRAY_PG_DB_NAME" -t -c "
    SELECT 
        current_database() as db_name,
        pg_size_pretty(pg_database_size(current_database())) as db_size,
        pg_database_size(current_database()) as db_size_bytes,
        (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()) as active_connections,
        pg_encoding_to_char(encoding) as encoding,
        datcollate as collation,
        datctype as ctype
    FROM pg_database 
    WHERE datname = current_database();
    " 2>/dev/null)
    
    if [[ -n "$DB_INFO" ]]; then
        echo "$DB_INFO" | while IFS='|' read -r db_name db_size db_size_bytes active_conns encoding collation ctype; do
            echo "  - Database Name: $(echo $db_name | xargs)"
            echo "  - Size: $(echo $db_size | xargs) ($(echo $db_size_bytes | xargs) bytes)"
            echo "  - Active Connections: $(echo $active_conns | xargs)"
            echo "  - Encoding: $(echo $encoding | xargs)"
            echo "  - Collation: $(echo $collation | xargs)"
            echo "  - Character Type: $(echo $ctype | xargs)"
        done
    fi
else
    print_status $RED "✗ Database '$XRAY_PG_DB_NAME' does not exist"
    print_status $YELLOW "Available databases:"
    sudo -u postgres psql -h "$XRAY_DB_SERVER" -l
    exit 1
fi

# Get database tablespace information
print_section "Database Tablespace Configuration"
DB_TABLESPACE_INFO=$(sudo -u postgres psql -h "$XRAY_DB_SERVER" -t -c "
SELECT 
    d.datname,
    COALESCE(t.spcname, 'pg_default') as tablespace_name,
    COALESCE(pg_tablespace_location(t.oid), 'default location') as tablespace_location,
    CASE 
        WHEN t.spcname IS NULL THEN 'Default tablespace'
        ELSE 'Custom tablespace'
    END as tablespace_type
FROM pg_database d 
LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid 
WHERE d.datname = '$XRAY_PG_DB_NAME';
" 2>/dev/null)

if [[ -n "$DB_TABLESPACE_INFO" ]]; then
    echo "$DB_TABLESPACE_INFO" | while IFS='|' read -r db_name ts_name ts_location ts_type; do
        echo "  - Database: $(echo $db_name | xargs)"
        echo "  - Tablespace: $(echo $ts_name | xargs)"
        echo "  - Location: $(echo $ts_location | xargs)"
        echo "  - Type: $(echo $ts_type | xargs)"
    done
    
    # Check if using expected tablespace
    ACTUAL_TABLESPACE=$(echo "$DB_TABLESPACE_INFO" | cut -d'|' -f2 | xargs)
    ACTUAL_TABLESPACE_LOCATION=$(echo "$DB_TABLESPACE_INFO" | cut -d'|' -f3 | xargs)
    if [[ -n "$XRAY_PG_DB_TS" ]]; then
        if [[ "$ACTUAL_TABLESPACE" == "$XRAY_PG_DB_TS" ]]; then
            print_status $GREEN "✓ Database is using expected tablespace: $XRAY_PG_DB_TS"
        else
            print_status $YELLOW "⚠ Database tablespace mismatch!"
            echo "  - Expected: $XRAY_PG_DB_TS"
            echo "  - Actual: $ACTUAL_TABLESPACE"
        fi
    fi
fi


# Get PostgreSQL configuration and WAL information
print_section "PostgreSQL Configuration and WAL Location"

# Get data directory and WAL location
PG_CONFIG=$(sudo -u postgres psql -h "$XRAY_DB_SERVER" -t -c "
SELECT 
    name,
    setting,
    unit,
    context
FROM pg_settings 
WHERE name IN (
    'data_directory',
    'wal_level',
    'archive_mode',
    'archive_command',
    'max_wal_size',
    'min_wal_size',
    'checkpoint_completion_target',
    'shared_buffers',
    'effective_cache_size',
    'work_mem',
    'maintenance_work_mem'
)
ORDER BY name;
" 2>/dev/null)

if [[ -n "$PG_CONFIG" ]]; then
    print_status $BLUE "Key PostgreSQL Settings:"
    echo "$PG_CONFIG" | while IFS='|' read -r name setting unit context; do
        name=$(echo $name | xargs)
        setting=$(echo $setting | xargs)
        unit=$(echo $unit | xargs)
        context=$(echo $context | xargs)
        
        if [[ -n "$unit" && "$unit" != "" ]]; then
            echo "  - $name: $setting $unit ($context)"
        else
            echo "  - $name: $setting ($context)"
        fi
    done
fi

# Get WAL directory location
DATA_DIR=$(sudo -u postgres psql -h "$XRAY_DB_SERVER" -t -c "SHOW data_directory;" 2>/dev/null | xargs)
if [[ -n "$DATA_DIR" ]]; then
    echo
    print_status $BLUE "PostgreSQL Data and WAL Locations:"
    echo "  - Data Directory: $DATA_DIR"
    echo "  - WAL Directory: $DATA_DIR/pg_wal"
    
    # Check if WAL directory exists and get size
    if sudo test -L "$DATA_DIR/pg_wal"; then
        WAL_TARGET=$(sudo readlink -f "$DATA_DIR/pg_wal" 2>/dev/null || echo "Unknown")
        echo "  - WAL Symlink Target: $WAL_TARGET"
        
        if sudo test -d "$WAL_TARGET"; then
            WAL_SIZE=$(sudo du -sh "$WAL_TARGET" 2>/dev/null | cut -f1 || echo "Unknown")
            WAL_FILES=$(sudo find "$WAL_TARGET" -name "0*" -type f 2>/dev/null | wc -l || echo "Unknown")
            echo "  - WAL Size: $WAL_SIZE"
            echo "  - WAL Files: $WAL_FILES"
            print_status $GREEN "✓ WAL directory accessible (symlink)"
        else
            print_status $YELLOW "⚠ WAL symlink target not accessible"
        fi
    elif sudo test -d "$DATA_DIR/pg_wal"; then
        WAL_SIZE=$(sudo du -sh "$DATA_DIR/pg_wal" 2>/dev/null | cut -f1 || echo "Unknown")
        WAL_FILES=$(sudo find "$DATA_DIR/pg_wal" -name "0*" -type f 2>/dev/null | wc -l || echo "Unknown")
        echo "  - WAL Size: $WAL_SIZE"
        echo "  - WAL Files: $WAL_FILES"
        print_status $GREEN "✓ WAL directory accessible (regular directory)"
    else
        print_status $YELLOW "⚠ Cannot access WAL directory (permission or path issue)"
    fi
fi

# Get database table information
print_section "Database Table Information"
TABLE_INFO=$(sudo -u postgres psql -h "$XRAY_DB_SERVER" -d "$XRAY_PG_DB_NAME" -t -c "
SELECT 
    COUNT(*) as table_count,
    pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename))) as total_table_size,
    SUM(pg_total_relation_size(schemaname||'.'||tablename)) as total_size_bytes
FROM pg_tables 
WHERE schemaname NOT IN ('information_schema', 'pg_catalog');
" 2>/dev/null)

if [[ -n "$TABLE_INFO" ]]; then
    echo "$TABLE_INFO" | while IFS='|' read -r table_count total_size total_bytes; do
        echo "  - Total Tables: $(echo $table_count | xargs)"
        echo "  - Total Table Size: $(echo $total_size | xargs)"
        echo "  - Size in Bytes: $(echo $total_bytes | xargs)"
    done
    
    # Show top 5 largest tables
    echo
    print_status $BLUE "Top 5 Largest Tables:"
    sudo -u postgres psql -h "$XRAY_DB_SERVER" -d "$XRAY_PG_DB_NAME" -c "
    SELECT 
        schemaname as \"Schema\",
        tablename as \"Table Name\",
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as \"Size\",
        (SELECT reltuples::bigint FROM pg_class WHERE relname = tablename) as \"Est. Rows\"
    FROM pg_tables 
    WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
    LIMIT 5;
    " 2>/dev/null || print_status $YELLOW "⚠ Could not retrieve table information"
fi

# Get connection and activity information
print_section "Connection and Activity Information"
ACTIVITY_INFO=$(sudo -u postgres psql -h "$XRAY_DB_SERVER" -t -c "
SELECT 
    (SELECT setting FROM pg_settings WHERE name = 'max_connections') as max_connections,
    COUNT(*) as current_connections,
    COUNT(*) FILTER (WHERE state = 'active') as active_connections,
    COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
    COUNT(*) FILTER (WHERE datname = '$XRAY_PG_DB_NAME') as db_connections
FROM pg_stat_activity;
" 2>/dev/null)

if [[ -n "$ACTIVITY_INFO" ]]; then
    echo "$ACTIVITY_INFO" | while IFS='|' read -r max_conn curr_conn active_conn idle_conn db_conn; do
        echo "  - Max Connections: $(echo $max_conn | xargs)"
        echo "  - Current Connections: $(echo $curr_conn | xargs)"
        echo "  - Active Connections: $(echo $active_conn | xargs)"
        echo "  - Idle Connections: $(echo $idle_conn | xargs)"
        echo "  - Connections to $XRAY_PG_DB_NAME: $(echo $db_conn | xargs)"
    done
fi

# Performance and statistics
print_section "Database Statistics and Performance"
print_status $BLUE "Database Performance Metrics:"
sudo -u postgres psql -h "$XRAY_DB_SERVER" -d "$XRAY_PG_DB_NAME" -c "
SELECT 
    'Active Backends' as metric, numbackends::text as value
FROM pg_stat_database WHERE datname = '$XRAY_PG_DB_NAME'
UNION ALL
SELECT 
    'Transactions Committed', xact_commit::text
FROM pg_stat_database WHERE datname = '$XRAY_PG_DB_NAME'
UNION ALL
SELECT 
    'Transactions Rolled Back', xact_rollback::text
FROM pg_stat_database WHERE datname = '$XRAY_PG_DB_NAME'
UNION ALL
SELECT 
    'Cache Hit Ratio (%)', 
    ROUND((blks_hit::float / NULLIF(blks_hit + blks_read, 0)) * 100, 2)::text
FROM pg_stat_database WHERE datname = '$XRAY_PG_DB_NAME'
UNION ALL
SELECT 
    'Tuples Returned', tup_returned::text
FROM pg_stat_database WHERE datname = '$XRAY_PG_DB_NAME'
UNION ALL
SELECT 
    'Tuples Inserted', tup_inserted::text
FROM pg_stat_database WHERE datname = '$XRAY_PG_DB_NAME';
" 2>/dev/null || print_status $YELLOW "⚠ Could not retrieve database statistics"

# Final summary
print_section "Configuration Summary"
print_status $GREEN "✓ Database configuration analysis complete"
echo
print_status $BLUE "Quick Summary:"
echo "  - Database: $XRAY_PG_DB_NAME"
echo "  - Status: $(if sudo -u postgres psql -h "$XRAY_DB_SERVER" -lqt | cut -d \| -f 1 | grep -qw "$XRAY_PG_DB_NAME"; then echo "EXISTS"; else echo "MISSING"; fi)"
echo "  - Tablespace: $ACTUAL_TABLESPACE"
if [[ -n "$ACTUAL_TABLESPACE_LOCATION" && "$ACTUAL_TABLESPACE_LOCATION" != "default location" ]]; then
    echo "  - Tablespace Location: $ACTUAL_TABLESPACE_LOCATION"
fi
echo "  - Data Directory: $DATA_DIR"
echo "  - WAL Directory: $DATA_DIR/pg_wal"
if sudo test -L "$DATA_DIR/pg_wal"; then
    WAL_TARGET=$(sudo readlink -f "$DATA_DIR/pg_wal" 2>/dev/null || echo "Unknown")
    echo "  - WAL Directory Target: $WAL_TARGET"
fi
if [[ -n "$XRAY_PG_DB_TS" ]]; then
    echo "  - Expected Tablespace: $XRAY_PG_DB_TS"
    if [[ "$ACTUAL_TABLESPACE" == "$XRAY_PG_DB_TS" ]]; then
        echo "  - Tablespace Match: ✓ YES"
    else
        echo "  - Tablespace Match: ✗ NO"
    fi
fi
# Get shared_buffers setting for summary
SHARED_BUFFERS_SUMMARY=$(sudo -u postgres psql -h "$XRAY_DB_SERVER" -t -c "SHOW shared_buffers;" 2>/dev/null | xargs || echo "Unknown")
echo "  - Shared Buffers: $SHARED_BUFFERS_SUMMARY"

echo
print_status $CYAN "Configuration analysis completed successfully!"

# Exit successfully
exit 0