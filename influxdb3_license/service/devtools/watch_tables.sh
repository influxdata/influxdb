#!/bin/bash
# This script creates a tmux session with 5 panes showing database tables
# Support for both local and remote database connections with password authentication

# Default configuration values
DB_HOST="localhost"
DB_USER="postgres"
DB_NAME="influxdb_pro_license"
REFRESH_INTERVAL=1
DB_PORT=5432

# Function to display usage instructions
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -h <host>     Database host (default: localhost)"
    echo "  -u <user>     Database user (default: postgres)"
    echo "  -d <dbname>   Database name (default: influxdb_pro_license)"
    echo "  -i <seconds>  Refresh interval in seconds (default: 1)"
    echo "  -p <port>     Database port (default: 5432)"
    echo "  -H            Show this help message"
    exit 1
}

# Parse command line arguments
while getopts "h:u:d:i:p:H" opt; do
    case $opt in
        h) DB_HOST="$OPTARG" ;;
        u) DB_USER="$OPTARG" ;;
        d) DB_NAME="$OPTARG" ;;
        i) REFRESH_INTERVAL="$OPTARG" ;;
        p) DB_PORT="$OPTARG" ;;
        H) usage ;;
        \?) usage ;;
    esac
done

# Validate refresh interval is a positive number
if ! [[ "$REFRESH_INTERVAL" =~ ^[0-9]+$ ]] || [ "$REFRESH_INTERVAL" -lt 1 ]; then
    echo "Error: Refresh interval must be a positive integer"
    exit 1
fi

# SQL queries - focusing on human-readable fields
LICENSE_QUERY="select id, user_id, email, node_id, state, created_at, valid_from, valid_until from licenses order by id desc limit 10;"
USER_QUERY="select id, email, emails_sent_cnt, created_at, verified_at, updated_at from users order by id desc limit 10;"
EMAIL_QUERY="select id, user_id, verification_url, verified_at, license_id, to_email, state, scheduled_at, sent_at, send_cnt, send_fail_cnt, last_err_msg from emails order by id desc limit 10;"
USER_IP_QUERY="select user_id, ipaddr, blocked from user_ips order by user_id desc limit 10;"

# Check if psql is available
if ! command -v psql >/dev/null 2>&1; then
    echo "Error: psql command not found. Please install PostgreSQL client tools."
    exit 1
fi

# Check if tmux is available
if ! command -v tmux >/dev/null 2>&1; then
    echo "Error: tmux command not found. Please install tmux."
    exit 1
fi

# Function to get database password
get_db_password() {
    # Try reading from PGPASSWORD environment variable first
    if [ -n "$PGPASSWORD" ]; then
        return 0
    fi
    
    # If no PGPASSWORD, prompt for it
    echo -n "Enter database password: "
    read -s DB_PASSWORD
    echo  # New line after password input
    export PGPASSWORD="$DB_PASSWORD"
}

# Test database connection and get password if needed
if ! PGPASSWORD=${PGPASSWORD} psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c '\q' >/dev/null 2>&1; then
    get_db_password
    if ! PGPASSWORD=${PGPASSWORD} psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c '\q' >/dev/null 2>&1; then
        echo "Error: Could not connect to database. Please check your credentials and connection settings."
        exit 1
    fi
fi

# Function to create a watch command with a title
create_watch_cmd() {
    local title=$1
    local query=$2
    echo "PGPASSWORD='${PGPASSWORD}' watch -t -n ${REFRESH_INTERVAL} 'echo \"=== ${title} ===\" && psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -d ${DB_NAME} -c \"${query}\"'"
}

# Check if we're already in a tmux session
if [ -z "$TMUX" ]; then
    # Start fresh
    tmux kill-session -t db_watch 2>/dev/null
    
    # Create new session with emails panel
    tmux new-session -s db_watch -d -x "$(tput cols)" -y "$(tput lines)" \; \
        send-keys "$(create_watch_cmd "Emails" "$EMAIL_QUERY")" C-m \; \
        split-window -v -p 65 \; \
        send-keys "$(create_watch_cmd "Licenses" "$LICENSE_QUERY")" C-m \; \
        split-window -v -p 50 \; \
        send-keys "$(create_watch_cmd "Users" "$USER_QUERY")" C-m \; \
        select-pane -t 1 \; \
        split-window -h -p 20 \; \
        send-keys "$(create_watch_cmd "User IPs" "$USER_IP_QUERY")" C-m \; \
        select-pane -t 3 \; \
        split-window -h -p 40 \; \
        select-pane -t 4
    
    # Print connection info
    echo "Starting tmux session with database watch..."
    echo "Connection details:"
    echo "  Host: $DB_HOST"
    echo "  User: $DB_USER"
    echo "  Database: $DB_NAME"
    echo "  Port: $DB_PORT"
    echo "  Refresh interval: ${REFRESH_INTERVAL}s"
    
    # Attach to session
    tmux attach-session -t db_watch
else
    echo "Error: Already in a tmux session. Please run this script from outside tmux."
    exit 1
fi