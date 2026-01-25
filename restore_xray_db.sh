#!/bin/bash

# Script to restore the XRAY PostgreSQL database from backup
# Author: Generated script
# Usage: ./restore_xray_db.sh [--env-file <path>] [backup_path] [--force] [--create-db]

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Default backup path
DEFAULT_BACKUP_PATH="/mnt/pgbackup/tpcc"

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

print_header() {
    echo "=================================================="
    print_status $BLUE "XRAY PostgreSQL Database RESTORE Script"
    echo "=================================================="
    echo
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [--env-file <path>] [backup_path] [--force] [--create-db]"
    echo
    echo "Options:"
    echo "  --env-file <path>  Path to XRAY environment file (default: /home/nutanix/bin/xray_set_env)"
    echo "  backup_path        Path to backup directory (default: $DEFAULT_BACKUP_PATH)"
    echo "  --force            Skip confirmations and force restore"
    echo "  --create-db        Create database if it doesn't exist"
    echo
    echo "Examples:"
    echo "  $0                                    # Restore from default backup"
    echo "  $0 /path/to/backup                   # Restore from custom backup"
    echo "  $0 --force                           # Force restore without prompts"
    echo "  $0 /path/to/backup --create-db       # Create DB and restore"
    echo
}

# Parse command line arguments
BACKUP_PATH="$DEFAULT_BACKUP_PATH"
FORCE_MODE=false
CREATE_DB=false
XRAY_ENV_FILE="/home/nutanix/bin/xray_set_env"

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_usage
            exit 0
            ;;
        --env-file)
            XRAY_ENV_FILE="$2"
            shift 2
            ;;
        --force)
            FORCE_MODE=true
            shift
            ;;
        --create-db)
            CREATE_DB=true
            shift
            ;;
        -*)
            print_status $RED "Unknown option: $1"
            show_usage
            exit 1
            ;;
        *)
            if [[ -z "$BACKUP_PATH" || "$BACKUP_PATH" == "$DEFAULT_BACKUP_PATH" ]]; then
                BACKUP_PATH="$1"
            else
                print_status $RED "Multiple backup paths specified"
                show_usage
                exit 1
            fi
            shift
            ;;
    esac
done

print_header

if [[ "$FORCE_MODE" == "true" ]]; then
    print_status $YELLOW "Running in FORCE mode - skipping confirmations"
    echo
fi

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
else
    print_status $BLUE "Target database: ${BOLD}$XRAY_PG_DB_NAME${NC}"
fi

# Check if XRAY_PG_DB_TS is set
if [[ -z "$XRAY_PG_DB_TS" ]]; then
    print_status $YELLOW "⚠ XRAY_PG_DB_TS not set - will use default tablespace"
    TABLESPACE_CLAUSE=""
else
    print_status $BLUE "Target tablespace: ${BOLD}$XRAY_PG_DB_TS${NC}"
    # Verify tablespace exists
    if sudo -u postgres psql -t -c "SELECT 1 FROM pg_tablespace WHERE spcname = '$XRAY_PG_DB_TS';" | grep -q 1; then
        print_status $GREEN "✓ Tablespace '$XRAY_PG_DB_TS' exists"
        TABLESPACE_CLAUSE="TABLESPACE $XRAY_PG_DB_TS"
    else
        print_status $RED "✗ Tablespace '$XRAY_PG_DB_TS' does not exist"
        print_status $YELLOW "Available tablespaces:"
        sudo -u postgres psql -c "\db+"
        exit 1
    fi
fi

echo

# Check if PostgreSQL is running
print_status $BLUE "Checking PostgreSQL service status..."
if ! pgrep -x "postgres" > /dev/null; then
    print_status $RED "✗ PostgreSQL is not running"
    exit 1
else
    print_status $GREEN "✓ PostgreSQL is running"
fi

echo

# Validate backup path
print_status $BLUE "Validating backup path: $BACKUP_PATH"
if [[ ! -d "$BACKUP_PATH" ]]; then
    print_status $RED "✗ Backup directory does not exist: $BACKUP_PATH"
    exit 1
fi

if ! sudo test -f "$BACKUP_PATH/toc.dat"; then
    print_status $RED "✗ Invalid backup format - missing toc.dat file"
    print_status $YELLOW "Expected PostgreSQL directory format backup"
    exit 1
fi

# Count backup files
BACKUP_FILES=$(sudo find "$BACKUP_PATH" -name "*.dat.gz" -o -name "*.dat" | wc -l)
BACKUP_SIZE=$(sudo du -sh "$BACKUP_PATH" 2>/dev/null | cut -f1 || echo "Unknown")

print_status $GREEN "✓ Valid backup found"
echo "  - Backup path: $BACKUP_PATH"
echo "  - Backup size: $BACKUP_SIZE"
echo "  - Data files: $BACKUP_FILES"
echo "  - Format: PostgreSQL directory format"

echo

# Check if target database exists
DB_EXISTS=false
if sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$XRAY_PG_DB_NAME"; then
    DB_EXISTS=true
    print_status $YELLOW "⚠ Target database '$XRAY_PG_DB_NAME' already exists"
    
    # Get current database info
    CURRENT_SIZE=$(sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -t -c "SELECT pg_size_pretty(pg_database_size('$XRAY_PG_DB_NAME'));" 2>/dev/null | xargs || echo "Unknown")
    CURRENT_TABLES=$(sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | xargs || echo "Unknown")
    
    echo "  - Current size: $CURRENT_SIZE"
    echo "  - Current tables: $CURRENT_TABLES"
    echo "  - ${BOLD}Restore will OVERWRITE existing data!${NC}"
else
    print_status $BLUE "Target database '$XRAY_PG_DB_NAME' does not exist"
    if [[ "$CREATE_DB" == "true" ]]; then
        print_status $GREEN "✓ Will create database during restore"
    else
        print_status $RED "✗ Database must exist or use --create-db option"
        exit 1
    fi
fi

echo

# Safety confirmation (unless force mode)
if [[ "$FORCE_MODE" != "true" ]]; then
    print_status $YELLOW "${BOLD}⚠️  RESTORE CONFIRMATION ⚠️${NC}"
    echo "You are about to restore database: $XRAY_PG_DB_NAME"
    echo "From backup: $BACKUP_PATH"
    echo "Backup size: $BACKUP_SIZE"
    
    if [[ "$DB_EXISTS" == "true" ]]; then
        print_status $RED "This will OVERWRITE the existing database!"
    fi
    
    echo
    read -p "Do you want to proceed with the restore? (type 'yes' to confirm): " confirm
    if [[ "$confirm" != "yes" ]]; then
        print_status $GREEN "Restore cancelled by user"
        exit 0
    fi
fi

echo

# Create database if needed
if [[ "$DB_EXISTS" == "false" && "$CREATE_DB" == "true" ]]; then
    print_status $BLUE "Creating database '$XRAY_PG_DB_NAME'..."
    if [[ -n "$TABLESPACE_CLAUSE" ]]; then
        print_status $BLUE "Using tablespace: $XRAY_PG_DB_TS"
        CREATE_CMD="CREATE DATABASE \"$XRAY_PG_DB_NAME\" $TABLESPACE_CLAUSE;"
    else
        CREATE_CMD="CREATE DATABASE \"$XRAY_PG_DB_NAME\";"
    fi
    
    if sudo -u postgres psql -c "$CREATE_CMD"; then
        print_status $GREEN "✓ Database '$XRAY_PG_DB_NAME' created"
        if [[ -n "$TABLESPACE_CLAUSE" ]]; then
            echo "  - Tablespace: $XRAY_PG_DB_TS"
        fi
    else
        print_status $RED "✗ Failed to create database '$XRAY_PG_DB_NAME'"
        exit 1
    fi
    echo
fi

# Terminate active connections to the database
if [[ "$DB_EXISTS" == "true" ]]; then
    print_status $BLUE "Terminating active connections to database '$XRAY_PG_DB_NAME'..."
    TERMINATED_CONNECTIONS=$(sudo -u postgres psql -t -c "
    SELECT pg_terminate_backend(pid) 
    FROM pg_stat_activity 
    WHERE datname = '$XRAY_PG_DB_NAME' 
      AND pid <> pg_backend_pid();" 2>/dev/null | grep -c "t" || echo "0")
    
    if [[ "$TERMINATED_CONNECTIONS" -gt 0 ]]; then
        print_status $YELLOW "⚠ Terminated $TERMINATED_CONNECTIONS active connections"
    else
        print_status $GREEN "✓ No active connections to terminate"
    fi
    
    # Wait for connections to close
    sleep 2
    echo
fi

# Perform the restore
print_status $BLUE "Starting database restore..."
print_status $YELLOW "This may take a while depending on backup size..."

START_TIME=$(date +%s)

# Use the same command structure as the original script but with better error handling
RESTORE_CMD="sudo -u postgres pg_restore -j 8 -F d \"$BACKUP_PATH\" -d \"$XRAY_PG_DB_NAME\""

print_status $BLUE "Executing: $RESTORE_CMD"
echo

if eval "$RESTORE_CMD"; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    MINUTES=$((DURATION / 60))
    SECONDS=$((DURATION % 60))
    
    print_status $GREEN "✓ Database restore completed successfully"
    echo "  - Duration: ${MINUTES}m ${SECONDS}s"
else
    print_status $RED "✗ Database restore failed"
    print_status $YELLOW "Common issues:"
    echo "  - Insufficient disk space"
    echo "  - Permission problems"
    echo "  - Corrupted backup files"
    echo "  - Database connection issues"
    exit 1
fi

echo

# Verify the restore
print_status $BLUE "Verifying restored database..."
RESTORED_TABLES=$(sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | xargs || echo "0")
RESTORED_SIZE=$(sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -t -c "SELECT pg_size_pretty(pg_database_size('$XRAY_PG_DB_NAME'));" 2>/dev/null | xargs || echo "Unknown")

if [[ "$RESTORED_TABLES" -gt 0 ]]; then
    print_status $GREEN "✓ Restore verification successful"
    echo "  - Tables restored: $RESTORED_TABLES"
    echo "  - Database size: $RESTORED_SIZE"
    
    # Show detailed table sizes and row counts
    echo
    print_status $BLUE "Table sizes and row counts:"
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
    " 2>/dev/null || print_status $YELLOW "⚠ Could not retrieve detailed table sizes"
else
    print_status $YELLOW "⚠ Warning: No tables found in restored database"
fi

echo

# Final status
print_status $GREEN "=================================================="
print_status $GREEN "✓ DATABASE RESTORE COMPLETED SUCCESSFULLY"
print_status $GREEN "=================================================="

echo
print_status $BLUE "Restore Summary:"
echo "  - Database: $XRAY_PG_DB_NAME"
echo "  - Backup source: $BACKUP_PATH"
echo "  - Backup size: $BACKUP_SIZE"
echo "  - Tables restored: $RESTORED_TABLES"
echo "  - Final database size: $RESTORED_SIZE"
echo "  - Restore duration: ${MINUTES}m ${SECONDS}s"
if [[ -n "$XRAY_PG_DB_TS" ]]; then
    echo "  - Tablespace: $XRAY_PG_DB_TS"
fi

# Exit successfully
exit 0