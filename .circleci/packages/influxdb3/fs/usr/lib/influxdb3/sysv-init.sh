#!/bin/bash
### BEGIN INIT INFO
# Provides:          influxdb3-###FLAVOR###
# Required-Start:    $network $remote_fs
# Required-Stop:     $network $remote_fs
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: InfluxDB 3
### END INIT INFO

# NOTE: THIS SCRIPT (/etc/init.d/influxdb3-$FLAVOR) IS OVERWRITTEN ON UPGRADE

# Configuration
FLAVOR="###FLAVOR###"
DATA_DIR="/var/lib/influxdb3"
CONF_FILE="/etc/influxdb3/influxdb3-$FLAVOR.conf"
DEFAULT_FILE="/etc/default/influxdb3-$FLAVOR"
LOG_FILE="/var/log/influxdb3/influxdb3-$FLAVOR.log"
PID_FILE="/run/influxdb3/influxdb3-$FLAVOR.pid"
USER="influxdb3"
GROUP="influxdb3"
OPEN_FILE_LIMIT=65536

# Check for root
if [ "$(id -u)" -ne 0 ]; then
    echo "Error: This script must be run as root"
    exit 4
fi

# Ensure the runtime directory exists
if ! mkdir -p "$(dirname "$PID_FILE")"; then
    echo "Error: Failed to create runtime directory $(dirname "$PID_FILE")"
    exit 1
fi
chmod 0775 "$(dirname "$PID_FILE")"
chown "root:$GROUP" "$(dirname "$PID_FILE")"

# Helper functions
is_running() {
    if [ ! -f "$PID_FILE" ]; then
        return 1
    fi

    # shellcheck disable=SC2002
    PID=$(cat "$PID_FILE" 2>/dev/null | tr -d '[:space:]')
    case "$PID" in
        ''|*[!0-9]*) return 1 ;;
    esac

    if ! kill -0 "$PID" 2>/dev/null; then
        return 1
    fi

    # If /proc is mounted, verify it's actually influxdb3
    if [ -r /proc/"$PID"/cmdline ]; then
        if ! grep -q influxdb3 /proc/"$PID"/cmdline 2>/dev/null; then
            return 1
        fi
    fi

    return 0
}

start() {
    if is_running; then
        echo "influxdb3-$FLAVOR is already running"
        return 0
    fi

    # Clean up stale PID file if present
    if [ -f "$PID_FILE" ]; then
        rm -f "$PID_FILE"
    fi

    # Check if service is enabled
    if [ ! -f "$DEFAULT_FILE" ]; then
        echo "Error: $DEFAULT_FILE not found"
        echo "To enable influxdb3-$FLAVOR, create $DEFAULT_FILE with ENABLED=yes"
        return 6
    fi

    # Source the defaults file
    # shellcheck disable=SC1090
    . "$DEFAULT_FILE"

    if [ "$ENABLED" != "yes" ]; then
        echo "influxdb3-$FLAVOR is disabled"
        echo "To enable, set ENABLED=yes in $DEFAULT_FILE"
        return 0
    fi

    if [ ! -f "$CONF_FILE" ]; then
        echo "Error: Config file $CONF_FILE not found"
        return 6
    fi

    echo "Starting influxdb3-$FLAVOR..."

    # Set file limit
    ulimit -n "$OPEN_FILE_LIMIT" 2>/dev/null || true

    # Ensure log file exists with correct ownership
    LOG_DIR=$(dirname "$LOG_FILE")
    if [ -d "$LOG_DIR" ]; then
        touch "$LOG_FILE"
        chown "$USER:$GROUP" "$LOG_FILE"
        chmod 640 "$LOG_FILE"
    fi

    # Start the process - launcher daemonizes and writes PID file
    su -s /bin/sh "$USER" -c "cd $DATA_DIR && \
        exec /usr/lib/influxdb3/python/bin/python3 /usr/lib/influxdb3/influxdb3-launcher \
            --exec=/usr/bin/influxdb3 \
            --flavor=$FLAVOR \
            --stamp-dir=$DATA_DIR \
            --config-toml=$CONF_FILE \
            --pidfile=$PID_FILE \
            --daemonize \
            --log-file=$LOG_FILE \
            -- serve"

    # Verify it started
    sleep 1
    if [ ! -f "$PID_FILE" ]; then
        echo "Error: influxdb3-$FLAVOR failed to start (PID file not created)"
        return 1
    fi
    if is_running; then
        echo "influxdb3-$FLAVOR started successfully"
        return 0
    else
        echo "Error: influxdb3-$FLAVOR failed to start"
        return 1
    fi
}

stop() {
    if ! is_running; then
        echo "influxdb3-$FLAVOR is not running"
        return 0
    fi

    echo "Stopping influxdb3-$FLAVOR..."
    # shellcheck disable=SC2002
    PID=$(cat "$PID_FILE" 2>/dev/null | tr -d '[:space:]')

    case "$PID" in
        ''|*[!0-9]*) return 1 ;;
    esac

    kill "$PID" 2>/dev/null

    # Wait up to 30 seconds for graceful shutdown
    for _ in $(seq 1 30); do
        if ! kill -0 "$PID" 2>/dev/null; then
            rm -f "$PID_FILE"
            echo "influxdb3-$FLAVOR stopped"
            return 0
        fi
        sleep 1
    done

    # Force kill if still running
    echo "Timeout - forcing shutdown..."
    kill -9 "$PID" 2>/dev/null
    rm -f "$PID_FILE"
    echo "influxdb3-$FLAVOR stopped (forced)"
    return 0
}

status() {
    if is_running; then
        # shellcheck disable=SC2002
        PID=$(cat "$PID_FILE" 2>/dev/null | tr -d '[:space:]')
        echo "influxdb3-$FLAVOR is running (pid $PID)"
        return 0
    else
        echo "influxdb3-$FLAVOR is not running"
        return 3
    fi
}

case "$1" in
    start)
        start
        exit $?
        ;;
    stop)
        stop
        exit $?
        ;;
    restart)
        stop
        start
        exit $?
        ;;
    status)
        status
        exit $?
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 2
        ;;
esac
