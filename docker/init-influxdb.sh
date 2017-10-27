#!/bin/bash
set -e

AUTH_ENABLED="$INFLUXDB_HTTP_AUTH_ENABLED"

if [ -z "$AUTH_ENABLED" ]; then
	AUTH_ENABLED="$(grep -iE '^\s*auth-enabled\s*=\s*true' /etc/influxdb/influxdb.conf | grep -io 'true' | cat)"
else
	AUTH_ENABLED="$(echo ""$INFLUXDB_HTTP_AUTH_ENABLED"" | grep -io 'true' | cat)"
fi

INIT_USERS=$([ ! -z "$AUTH_ENABLED" ] && [ ! -z "$INFLUXDB_ADMIN_USER" ] && echo 1 || echo)

if ( [ ! -z "$INIT_USERS" ] || [ ! -z "$INFLUXDB_DB" ] || [ "$(ls -A /docker-entrypoint-initdb.d 2> /dev/null)" ] ) && [ ! "$(ls -A /var/lib/influxdb)" ]; then

	INIT_QUERY=""
	CREATE_DB_QUERY="CREATE DATABASE $INFLUXDB_DB"

	if [ ! -z "$INIT_USERS" ]; then

		if [ -z "$INFLUXDB_ADMIN_PASSWORD" ]; then
			INFLUXDB_ADMIN_PASSWORD="$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32;echo;)"
			echo "INFLUXDB_ADMIN_PASSWORD:$INFLUXDB_ADMIN_PASSWORD"
		fi

		INIT_QUERY="CREATE USER $INFLUXDB_ADMIN_USER WITH PASSWORD '$INFLUXDB_ADMIN_PASSWORD' WITH ALL PRIVILEGES"
	elif [ ! -z "$INFLUXDB_DB" ]; then
		INIT_QUERY="$CREATE_DB_QUERY"
	else
		INIT_QUERY="SHOW DATABASES"
	fi

	INFLUXDB_INIT_PORT="8086"

	INFLUXDB_HTTP_BIND_ADDRESS=127.0.0.1:$INFLUXDB_INIT_PORT INFLUXDB_HTTP_HTTPS_ENABLED=false influxd "$@" &
	pid="$!"

	INFLUX_CMD="influx -host 127.0.0.1 -port $INFLUXDB_INIT_PORT -execute "

	for i in {30..0}; do
		if $INFLUX_CMD "$INIT_QUERY" &> /dev/null; then
			break
		fi
		echo 'influxdb init process in progress...'
		sleep 1
	done

	if [ "$i" = 0 ]; then
		echo >&2 'influxdb init process failed.'
		exit 1
	fi

	if [ ! -z "$INIT_USERS" ]; then

		INFLUX_CMD="influx -host 127.0.0.1 -port $INFLUXDB_INIT_PORT -username ${INFLUXDB_ADMIN_USER} -password ${INFLUXDB_ADMIN_PASSWORD} -execute "

		if [ ! -z "$INFLUXDB_DB" ]; then
			$INFLUX_CMD "$CREATE_DB_QUERY"
		fi

		if [ ! -z "$INFLUXDB_USER" ] && [ -z "$INFLUXDB_USER_PASSWORD" ]; then
			INFLUXDB_USER_PASSWORD="$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32;echo;)"
			echo "INFLUXDB_USER_PASSWORD:$INFLUXDB_USER_PASSWORD"
		fi

		if [ ! -z "$INFLUXDB_USER" ]; then
			$INFLUX_CMD "CREATE USER $INFLUXDB_USER WITH PASSWORD '$INFLUXDB_USER_PASSWORD'"

			$INFLUX_CMD "REVOKE ALL PRIVILEGES FROM ""$INFLUXDB_USER"""

			if [ ! -z "$INFLUXDB_DB" ]; then
				$INFLUX_CMD "GRANT ALL ON ""$INFLUXDB_DB"" TO ""$INFLUXDB_USER"""
			fi
		fi

		if [ ! -z "$INFLUXDB_WRITE_USER" ] && [ -z "$INFLUXDB_WRITE_USER_PASSWORD" ]; then
			INFLUXDB_WRITE_USER_PASSWORD="$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32;echo;)"
			echo "INFLUXDB_WRITE_USER_PASSWORD:$INFLUXDB_WRITE_USER_PASSWORD"
		fi

		if [ ! -z "$INFLUXDB_WRITE_USER" ]; then
			$INFLUX_CMD "CREATE USER $INFLUXDB_WRITE_USER WITH PASSWORD '$INFLUXDB_WRITE_USER_PASSWORD'"
			$INFLUX_CMD "REVOKE ALL PRIVILEGES FROM ""$INFLUXDB_WRITE_USER"""

			if [ ! -z "$INFLUXDB_DB" ]; then
				$INFLUX_CMD "GRANT WRITE ON ""$INFLUXDB_DB"" TO ""$INFLUXDB_WRITE_USER"""
			fi
		fi

		if [ ! -z "$INFLUXDB_READ_USER" ] && [ -z "$INFLUXDB_READ_USER_PASSWORD" ]; then
			INFLUXDB_READ_USER_PASSWORD="$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32;echo;)"
			echo "INFLUXDB_READ_USER_PASSWORD:$INFLUXDB_READ_USER_PASSWORD"
		fi

		if [ ! -z "$INFLUXDB_READ_USER" ]; then
			$INFLUX_CMD "CREATE USER $INFLUXDB_READ_USER WITH PASSWORD '$INFLUXDB_READ_USER_PASSWORD'"
			$INFLUX_CMD "REVOKE ALL PRIVILEGES FROM ""$INFLUXDB_READ_USER"""

			if [ ! -z "$INFLUXDB_DB" ]; then
				$INFLUX_CMD "GRANT READ ON ""$INFLUXDB_DB"" TO ""$INFLUXDB_READ_USER"""
			fi
		fi

	fi

	for f in /docker-entrypoint-initdb.d/*; do
		case "$f" in
			*.sh)     echo "$0: running $f"; . "$f" ;;
			*.iql)    echo "$0: running $f"; $INFLUX_CMD "$(cat ""$f"")"; echo ;;
			*)        echo "$0: ignoring $f" ;;
		esac
		echo
	done

	if ! kill -s TERM "$pid" || ! wait "$pid"; then
		echo >&2 'influxdb init process failed. (Could not stop influxdb)'
		exit 1
	fi

fi
