#!/bin/bash
### BEGIN INIT INFO
# Provides:          influxd
# Required-Start:    $all
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start the InfluxDB process
### END INIT INFO

# If you modify this, please make sure to also edit influxdb.service

# Command-line options that can be set in /etc/default/influxdb.  These will override
# any config file values.
DEFAULT=/etc/default/influxdb

# Daemon options
INFLUXD_OPTS=

# Process name ( For display )
NAME=influxdb

# User and group
USER=influxdb
GROUP=influxdb

# Check for sudo or root privileges before continuing
if [ "$UID" != "0" ]; then
    echo "You must be root to run this script"
    exit 1
fi

# Daemon name, where is the actual executable If the daemon is not
# there, then exit.
DAEMON=/usr/bin/influxd
if [ ! -x $DAEMON ]; then
    echo "Executable $DAEMON does not exist!"
    exit 5
fi

# Configuration file
CONFIG=/etc/influxdb/influxdb.conf

# PID file for the daemon
PIDFILE=/var/run/influxdb/influxd.pid
PIDDIR=`dirname $PIDFILE`
if [ ! -d "$PIDDIR" ]; then
    mkdir -p $PIDDIR
    chown $USER:$GROUP $PIDDIR
fi

# Max open files
OPEN_FILE_LIMIT=65536

if [ -r /lib/lsb/init-functions ]; then
    source /lib/lsb/init-functions
fi

# Logging
if [ -z "$STDOUT" ]; then
    STDOUT=/dev/null
fi

if [ ! -f "$STDOUT" ]; then
    mkdir -p $(dirname $STDOUT)
fi

if [ -z "$STDERR" ]; then
    STDERR=/var/log/influxdb/influxd.log
fi

if [ ! -f "$STDERR" ]; then
    mkdir -p $(dirname $STDERR)
fi

# Override init script variables with DEFAULT values
if [ -r $DEFAULT ]; then
    source $DEFAULT
fi

# Retrieve configuration value from influxd.
function influxd_config() {
    local header="${1}"
    local target="${2}"

    while IFS= read -r line
    do
        # The TOML specification allows for key-value pairs to be namespaced in
        # collections. Therefore, it is not enough to grep for the desired key.
        # This filters out all values that are outside the target section.
        if [[ "${line}" =~ ^\[${header}\]$ ]]
        then
            local section_found=1
        fi

        # blank line signifies that the section has ended
        if [[ ! ${line} ]]
        then
            local section_found=
        fi

        if [[ ${section_found:-} ]]
        then
                # Once within the target section, search the key-value pairs for the
                # desired key. Currently, this only supports string values. Since
                # this is used only for wal-dir and data-dir, this should be okay.
                if [[ "${line}" =~ ^[[:space:]]*${target}[[:space:]]*=[[:space:]]\"(.*)\" ]]
                then
                    echo "${BASH_REMATCH[1]}" ; return
                fi
        fi
    done <<< "$(influxd config -config "${CONFIG}" ${INFLUXD_OPTS} 2>/dev/null)"
}

function log_failure_msg() {
    echo "$@" "[ FAILED ]"
}

function log_success_msg() {
    echo "$@" "[ OK ]"
}

function start() {
    # Check if config file exist
    if [ ! -r $CONFIG ]; then
        log_failure_msg "config file $CONFIG doesn't exist (or you don't have permission to view)"
        exit 4
    fi

    # Check that the PID file exists, and check the actual status of process
    if [ -f $PIDFILE ]; then
        PID="$(cat $PIDFILE)"
        if kill -0 "$PID" &>/dev/null; then
            # Process is already up
            log_success_msg "$NAME process is already running"
            return 0
        fi
    else
        su -s /bin/sh -c "touch $PIDFILE" $USER &>/dev/null
        if [ $? -ne 0 ]; then
            log_failure_msg "$PIDFILE not writable, check permissions"
            exit 5
        fi
    fi

    # Bump the file limits, before launching the daemon. These will
    # carry over to launched processes.
    ulimit -n $OPEN_FILE_LIMIT
    if [ $? -ne 0 ]; then
        log_failure_msg "Unable to set ulimit to $OPEN_FILE_LIMIT"
        exit 1
    fi

    DATA_DIR="$( influxd_config data dir     )"
    WAL_DIR="$(  influxd_config data wal-dir )"
    if [[ ( -d "${DATA_DIR}" ) && ( -d "${WAL_DIR}" ) ]]
    then
        # buildtsi prompts with a warning it is is run as root as the files it makes will be owned by root.
        # In that case, it awaits an interactive Yes but that can't be supplied. All around, best to run it
        # as the influxdb user. sudo is also an option but not as available as su
        echo "Building tsi with influxd_inspect buildtsi."
        runuser -u influxdb -- /usr/bin/influx_inspect buildtsi -compact-series-file \
            -datadir "${DATA_DIR}"                                                   \
            -waldir  "${WAL_DIR}"
    fi

    # Launch process
    echo "Starting $NAME..."
    if command -v start-stop-daemon &>/dev/null; then
        start-stop-daemon \
            --chuid $USER:$GROUP \
            --start \
            --quiet \
            --pidfile $PIDFILE \
            --exec $DAEMON \
            -- \
            -pidfile $PIDFILE \
            -config $CONFIG \
            $INFLUXD_OPTS >>$STDOUT 2>>$STDERR &
    else
        local CMD="$DAEMON -pidfile $PIDFILE -config $CONFIG $INFLUXD_OPTS >>$STDOUT 2>>$STDERR &"
        su -s /bin/sh -c "$CMD" $USER
    fi

    # Sleep to verify process is still up
    sleep 1
    if [ -f $PIDFILE ]; then
        # PIDFILE exists
        if kill -0 $(cat $PIDFILE) &>/dev/null; then
            # PID up, service running
            log_success_msg "$NAME process was started"
            return 0
        fi
    fi
    log_failure_msg "$NAME process was unable to start"
    exit 1
}

function stop() {
    # Stop the daemon.
    if [ -f $PIDFILE ]; then
        local PID="$(cat $PIDFILE)"
        if kill -0 $PID &>/dev/null; then
            echo "Stopping $NAME..."
            # Process still up, send SIGTERM and remove PIDFILE
            kill -s TERM $PID &>/dev/null && rm -f "$PIDFILE" &>/dev/null
            n=0
            while true; do
                # Enter loop to ensure process is stopped
                kill -0 $PID &>/dev/null
                if [ "$?" != "0" ]; then
                    # Process stopped, break from loop
                    log_success_msg "$NAME process was stopped"
                    return 0
                fi

                # Process still up after signal, sleep and wait
                sleep 1
                n=$(expr $n + 1)
                if [ $n -eq 30 ]; then
                    # After 30 seconds, send SIGKILL
                    echo "Timeout exceeded, sending SIGKILL..."
                    kill -s KILL $PID &>/dev/null
                elif [ $? -eq 40 ]; then
                    # After 40 seconds, error out
                    log_failure_msg "could not stop $NAME process"
                    exit 1
                fi
            done
        fi
    fi
    log_success_msg "$NAME process already stopped"
}

function restart() {
    # Restart the daemon.
    stop
    start
}

function status() {
    # Check the status of the process.
    if [ -f $PIDFILE ]; then
        PID="$(cat $PIDFILE)"
        if kill -0 $PID &>/dev/null; then
            log_success_msg "$NAME process is running"
            exit 0
        fi
    fi
    log_failure_msg "$NAME process is not running"
    exit 1
}

case $1 in
    start)
        start
        ;;

    stop)
        stop
        ;;

    restart)
        restart
        ;;

    status)
        status
        ;;

    version)
        $DAEMON version
        ;;

    *)
        # For invalid arguments, print the usage message.
        echo "Usage: $0 {start|stop|restart|status|version}"
        exit 2
        ;;
esac
