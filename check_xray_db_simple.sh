#!/bin/bash

# Simple script to check if XRAY PostgreSQL database is live
# Usage: ./check_xray_db_simple.sh [--env-file <path>]

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

# Source environment
source "$XRAY_ENV_FILE" 2>/dev/null || {
    echo "ERROR: Cannot source XRAY environment from: $XRAY_ENV_FILE"
    exit 1
}

# Check if variable is set
if [[ -z "$XRAY_PG_DB_NAME" ]]; then
    echo "ERROR: XRAY_PG_DB_NAME not set"
    exit 1
fi

# Quick database connectivity test
if sudo -u postgres psql -d "$XRAY_PG_DB_NAME" -c "SELECT 1;" >/dev/null 2>&1; then
    echo "SUCCESS: Database '$XRAY_PG_DB_NAME' is LIVE"
    exit 0
else
    echo "FAILED: Database '$XRAY_PG_DB_NAME' is NOT accessible"
    exit 1
fi