#!/bin/bash

# Script to check if the XRAY PostgreSQL database is live
# Author: Generated script
# Usage: ./check_xray_db.sh [--env-file <path>]

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

print_header() {
    echo "=================================================="
    print_status $BLUE "XRAY PostgreSQL Database Status Check"
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
    print_status $GREEN "✓ XRAY_PG_DB_NAME is set to: $XRAY_PG_DB_NAME"
fi

echo

# Check if PostgreSQL is running
print_status $BLUE "Checking PostgreSQL service status..."
if pgrep -x "postgres" > /dev/null; then
    print_status $GREEN "✓ PostgreSQL processes are running"
    
    # Count PostgreSQL processes
    PG_PROCESS_COUNT=$(pgrep -x "postgres" | wc -l)
    echo "  - Found $PG_PROCESS_COUNT PostgreSQL processes"
else
    print_status $RED "✗ PostgreSQL is not running"
    exit 1
fi

echo

# Check if the database exists
print_status $BLUE "Checking if database '$XRAY_PG_DB_NAME' exists..."
if sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$XRAY_PG_DB_NAME"; then
    print_status $GREEN "✓ Database '$XRAY_PG_DB_NAME' exists"
else
    print_status $RED "✗ Database '$XRAY_PG_DB_NAME' does not exist"
    echo
    print_status $YELLOW "Available databases:"
    sudo -u postgres psql -l
    exit 1
fi

echo

# Test database connectivity
print_status $BLUE "Testing database connectivity..."
DB_VERSION=$(sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -t -c "SELECT version();" 2>/dev/null | xargs)
if [[ $? -eq 0 && -n "$DB_VERSION" ]]; then
    print_status $GREEN "✓ Successfully connected to database '$XRAY_PG_DB_NAME'"
    echo "  - PostgreSQL Version: $DB_VERSION"
else
    print_status $RED "✗ Failed to connect to database '$XRAY_PG_DB_NAME'"
    exit 1
fi

echo

# Check database contents
print_status $BLUE "Checking database contents..."
TABLE_COUNT=$(sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | xargs)
if [[ $? -eq 0 && -n "$TABLE_COUNT" ]]; then
    print_status $GREEN "✓ Database contains $TABLE_COUNT tables"
    
    if [[ $TABLE_COUNT -gt 0 ]]; then
        echo
        print_status $BLUE "Tables in database:"
        sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -c "\dt" 2>/dev/null | grep -E "^\s+(public|Schema)" || true
    fi
else
    print_status $YELLOW "⚠ Could not retrieve table count (database may be empty or inaccessible)"
fi

echo

# Check database size
print_status $BLUE "Checking database size..."
DB_SIZE=$(sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -t -c "SELECT pg_size_pretty(pg_database_size('$XRAY_PG_DB_NAME'));" 2>/dev/null | xargs)
if [[ $? -eq 0 && -n "$DB_SIZE" ]]; then
    print_status $GREEN "✓ Database size: $DB_SIZE"
else
    print_status $YELLOW "⚠ Could not retrieve database size"
fi

echo

# Final status
print_status $GREEN "=================================================="
print_status $GREEN "✓ XRAY DATABASE '$XRAY_PG_DB_NAME' IS LIVE!"
print_status $GREEN "=================================================="

# Optional: Show connection info
echo
print_status $BLUE "Connection Information:"
echo "  - Database Name: $XRAY_PG_DB_NAME"
echo "  - Host: localhost (default)"
echo "  - Port: 5432 (default)"
echo "  - User: postgres"

# Exit successfully
exit 0