#! /usr/bin/env bash

### BEGIN INIT INFO
# Provides:          influxdb
# Required-Start:    $all
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start influxdb at boot time
### END INIT INFO

# this init script supports three different variations:
#  1. New lsb that define start-stop-daemon
#  2. Old lsb that don't have start-stop-daemon but define, log, pidofproc and killproc
#  3. Centos installations without lsb-core installed
#
# In the third case we have to define our own functions which are very dumb
# and expect the args to be positioned correctly.

if [ -e /lib/lsb/init-functions ]; then
    source /lib/lsb/init-functions
fi

function pidofproc() {
    if [ $# -ne 3 ]; then
        echo "Expected three arguments, e.g. $0 -p pidfile daemon-name"
    fi

    pid=`pgrep -f $3`
    local pidfile=`cat $2`

    if [ "x$pidfile" == "x" ]; then
        return 1
    fi

    if [ "x$pid" != "x" -a "$pidfile" == "$pid" ]; then
        return 0
    fi

    return 1
}

function killproc() {
    if [ $# -ne 3 ]; then
        echo "Expected three arguments, e.g. $0 -p pidfile signal"
    fi

    pid=`cat $2`

    kill -s $3 $pid
}

function log_failure_msg() {
    echo "$@" "[ FAILED ]"
}

function log_success_msg() {
    echo "$@" "[ OK ]"
}

# Process name ( For display )
name=influxdb

# Daemon name, where is the actual executable
daemon=/usr/bin/$name

# pid file for the daemon
pidfile=/opt/influxdb/shared/influxdb.pid

# Configuration file
config=/opt/$name/shared/config.toml

# If the daemon is not there, then exit.
[ -x $daemon ] || exit 5

case $1 in
    start)
        # Checked the PID file exists and check the actual status of process
        if [ -e $pidfile ]; then
            pidofproc -p $pidfile $daemon > /dev/null 2>&1 && status="0" || status="$?"
            # If the status is SUCCESS then don't need to start again.
            if [ "x$status" = "x0" ]; then
                log_failure_msg "$name process is running"
                exit 1 # Exit
            fi
        fi
        # Start the daemon.
        log_success_msg "Starting the process" "$name"
        # Start the daemon with the help of start-stop-daemon
        # Log the message appropriately
        cd /
        if which start-stop-daemon > /dev/null 2>&1; then
            nohup start-stop-daemon --chuid influxdb:influxdb -d / --start --quiet --oknodo --pidfile $pidfile --exec $daemon -- -pidfile $pidfile -config $config > /dev/null 2>&1 &
        elif set | egrep '^start_daemon' > /dev/null 2>&1; then
            start_daemon -u influxdb ${daemon}-daemon -pidfile $pidfile -config $config
        else
            sudo -u influxdb -g influxdb ${daemon}-daemon -pidfile $pidfile -config $config
        fi
        log_success_msg "$name process was started"
        ;;
    stop)
        # Stop the daemon.
        if [ -e $pidfile ]; then
            pidofproc -p $pidfile $daemon > /dev/null 2>&1 && status="0" || status="$?"
            if [ "$status" = 0 ]; then
                if killproc -p $pidfile SIGTERM && /bin/rm -rf $pidfile; then
                    log_success_msg "$name process was stopped"
                else
                    log_failure_msg "$name failed to stop service"
                fi
            fi
        else
            log_failure_msg "$name process is not running"
        fi
        ;;
    restart)
        # Restart the daemon.
        $0 stop && sleep 2 && $0 start
        ;;
    status)
        # Check the status of the process.
        if [ -e $pidfile ]; then
            if pidofproc -p $pidfile $daemon > /dev/null; then
                log_success_msg "$name Process is running"
                exit 0
            else
                log_failure_msg "$name Process is not running"
                exit 1
            fi
        else
            log_failure_msg "$name Process is not running"
            exit 1
        fi
        ;;
    # reload)
    #     # Reload the process. Basically sending some signal to a daemon to reload its configurations.
    #     if [ -e $pidfile ]; then
    #         start-stop-daemon --stop --signal SIGHUSR2 --quiet --pidfile $pidfile --name $name
    #         log_success_msg "$name process reloaded successfully"
    #     else
    #         log_failure_msg "$pidfile does not exists"
    #     fi
    #     ;;
    *)
        # For invalid arguments, print the usage message.
        echo "Usage: $0 {start|stop|restart|reload|status}"
        exit 2
        ;;
esac
