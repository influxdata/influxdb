#!/bin/bash

# This script creates a tmux session with 5 panes. Four of them are running
# `watch` commands that refresh on a regular interval, showing the latest rows
# from the `licenses`, `users`, `emails`, and `user_ips` tables. The fifth pane
# is for running ad-hoc commands. This makes it easy to run curl commands
# against the API and see the database updates in real-time.

# Configuration
DB_HOST="localhost"
DB_USER="postgres"
DB_NAME="influxdb_pro_license"
REFRESH_INTERVAL=1 # Refresh every 2 seconds

# SQL queries - focusing on human-readable fields
LICENSE_QUERY="select id, user_id, email, host_id, state, created_at, valid_from, valid_until from licenses order by id desc limit 10;"
USER_QUERY="select id, email, emails_sent_cnt, created_at, verified_at, updated_at from users order by id desc limit 10;"
EMAIL_QUERY="select id, user_id, verification_url, verified_at, license_id, to_email, state, scheduled_at, sent_at, send_cnt, send_fail_cnt, last_err_msg from emails order by id desc limit 10;"
USER_IP_QUERY="select user_id, ipaddr, blocked from user_ips order by user_id desc limit 10;"

# Function to create a watch command with a title
create_watch_cmd() {
    local title=$1
    local query=$2
    echo "watch -t -n ${REFRESH_INTERVAL} 'echo \"=== ${title} ===\" && psql -h ${DB_HOST} -U ${DB_USER} -d ${DB_NAME} -c \"${query}\"'"
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
        split-window -h -p 50 \; \
        select-pane -t 4

    # Attach to session
    tmux attach-session -t db_watch
else
    echo "Error: Already in a tmux session. Please run this script from outside tmux."
    exit 1
fi
