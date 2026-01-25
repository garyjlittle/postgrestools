#!/bin/bash

# Script to drop the XRAY PostgreSQL database
# Author: Generated script
# Usage: ./drop_xray_db.sh [--env-file <path>] [--force]

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

print_header() {
    echo "=================================================="
    print_status $RED "XRAY PostgreSQL Database DROP Script"
    print_status $YELLOW "⚠️  WARNING: This will PERMANENTLY DELETE the database!"
    echo "=================================================="
    echo
}

# Default values
FORCE_MODE=false
XRAY_ENV_FILE="/home/nutanix/bin/xray_set_env"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --env-file)
            XRAY_ENV_FILE="$2"
            shift 2
            ;;
        --force)
            FORCE_MODE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--env-file <path>] [--force]"
            echo "  --env-file <path>  Path to XRAY environment file (default: /home/nutanix/bin/xray_set_env)"
            echo "  --force           Skip confirmation prompts"
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

if [[ "$FORCE_MODE" == "true" ]]; then
    print_status $YELLOW "Running in FORCE mode - skipping confirmations"
    echo
fi

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
else
    print_status $BLUE "Database to drop: ${BOLD}$XRAY_PG_DB_NAME${NC}"
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

# Check if the database exists
print_status $BLUE "Checking if database '$XRAY_PG_DB_NAME' exists..."
if ! sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$XRAY_PG_DB_NAME"; then
    print_status $YELLOW "⚠ Database '$XRAY_PG_DB_NAME' does not exist - nothing to drop"
    exit 0
else
    print_status $GREEN "✓ Database '$XRAY_PG_DB_NAME' exists"
fi

# Get database info before dropping
print_status $BLUE "Getting database information..."
DB_SIZE=$(sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -t -c "SELECT pg_size_pretty(pg_database_size('$XRAY_PG_DB_NAME'));" 2>/dev/null | xargs || echo "Unknown")
TABLE_COUNT=$(sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | xargs || echo "Unknown")
CONNECTION_COUNT=$(sudo -u postgres psql -t -c "SELECT count(*) FROM pg_stat_activity WHERE datname = '$XRAY_PG_DB_NAME';" 2>/dev/null | xargs || echo "Unknown")

echo "  - Database size: $DB_SIZE"
echo "  - Number of tables: $TABLE_COUNT"
echo "  - Active connections: $CONNECTION_COUNT"

echo

# Safety confirmation (unless force mode)
if [[ "$FORCE_MODE" != "true" ]]; then
    print_status $RED "${BOLD}⚠️  DANGER ZONE ⚠️${NC}"
    print_status $RED "You are about to PERMANENTLY DELETE the database: $XRAY_PG_DB_NAME"
    print_status $RED "This action CANNOT be undone!"
    echo
    print_status $YELLOW "Database details:"
    echo "  - Name: $XRAY_PG_DB_NAME"
    echo "  - Size: $DB_SIZE"
    echo "  - Tables: $TABLE_COUNT"
    echo "  - Active connections: $CONNECTION_COUNT"
    echo
    
    # First confirmation
    read -p "Are you absolutely sure you want to drop this database? (type 'yes' to confirm): " confirm1
    if [[ "$confirm1" != "yes" ]]; then
        print_status $GREEN "Operation cancelled by user"
        exit 0
    fi
    
    # Second confirmation with database name
    read -p "Please type the database name '$XRAY_PG_DB_NAME' to confirm: " confirm2
    if [[ "$confirm2" != "$XRAY_PG_DB_NAME" ]]; then
        print_status $RED "Database name mismatch. Operation cancelled."
        exit 1
    fi
    
    # Final confirmation
    read -p "Last chance! Type 'DROP IT NOW' to proceed: " confirm3
    if [[ "$confirm3" != "DROP IT NOW" ]]; then
        print_status $GREEN "Operation cancelled by user"
        exit 0
    fi
fi

echo

# Terminate active connections to the database
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

# Wait a moment for connections to close
sleep 2

echo

# Drop the database
print_status $BLUE "Dropping database '$XRAY_PG_DB_NAME'..."
if sudo -u postgres psql -c "DROP DATABASE \"$XRAY_PG_DB_NAME\";" 2>/dev/null; then
    print_status $GREEN "✓ Successfully dropped database '$XRAY_PG_DB_NAME'"
else
    print_status $RED "✗ Failed to drop database '$XRAY_PG_DB_NAME'"
    print_status $YELLOW "Possible reasons:"
    echo "  - Database has active connections"
    echo "  - Insufficient permissions"
    echo "  - Database is being accessed by another process"
    exit 1
fi

echo

# Verify the database is gone
print_status $BLUE "Verifying database removal..."
if sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$XRAY_PG_DB_NAME"; then
    print_status $RED "✗ Database '$XRAY_PG_DB_NAME' still exists!"
    exit 1
else
    print_status $GREEN "✓ Confirmed: Database '$XRAY_PG_DB_NAME' has been removed"
fi

echo

# Final status
print_status $GREEN "=================================================="
print_status $GREEN "✓ DATABASE '$XRAY_PG_DB_NAME' SUCCESSFULLY DROPPED"
print_status $GREEN "=================================================="

echo
print_status $BLUE "Summary:"
echo "  - Database '$XRAY_PG_DB_NAME' has been permanently deleted"
echo "  - Size freed: $DB_SIZE"
echo "  - Tables removed: $TABLE_COUNT"
echo "  - Connections terminated: $TERMINATED_CONNECTIONS"

# Exit successfully
exit 0