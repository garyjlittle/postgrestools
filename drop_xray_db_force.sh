#!/bin/bash

# Simple script to forcefully drop XRAY PostgreSQL database
# Usage: ./drop_xray_db_force.sh [--env-file <path>]
# WARNING: This script drops the database WITHOUT confirmation!

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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
            echo "WARNING: This script drops the database WITHOUT confirmation!"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo -e "${RED}⚠️  FORCE DROP MODE - NO CONFIRMATIONS!${NC}"

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

echo -e "${YELLOW}Dropping database: $XRAY_PG_DB_NAME${NC}"

# Check if database exists
if ! sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$XRAY_PG_DB_NAME"; then
    echo -e "${YELLOW}Database '$XRAY_PG_DB_NAME' does not exist${NC}"
    exit 0
fi

# Terminate connections
echo "Terminating connections..."
sudo -u postgres psql -c "
SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE datname = '$XRAY_PG_DB_NAME' 
  AND pid <> pg_backend_pid();" >/dev/null 2>&1 || true

# Wait for connections to close
sleep 2

# Drop database
echo "Dropping database..."
if sudo -u postgres psql -c "DROP DATABASE \"$XRAY_PG_DB_NAME\";" >/dev/null 2>&1; then
    echo -e "${GREEN}SUCCESS: Database '$XRAY_PG_DB_NAME' dropped${NC}"
    exit 0
else
    echo -e "${RED}FAILED: Could not drop database '$XRAY_PG_DB_NAME'${NC}"
    exit 1
fi