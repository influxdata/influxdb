#!/bin/bash
#
#   Copyright 2018  SenX S.A.S.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

### BEGIN INIT INFO
# Provides:          warp10
# Required-Start:
# Required-Stop:
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Warp data platform
# Description:       Warp stores sensor data
### END INIT INFO

# Source function library.
if [[ -e /lib/lsb/init-functions ]]; then
  . /lib/lsb/init-functions
fi

set -euo pipefail

#JAVA_HOME=/opt/java8
#WARP10_HOME=/opt/warp10-@VERSION@
JMX_PORT=1098

# Strongly inspired by gradlew
# Determine the Java command to use to start the JVM.
if [ -n "${JAVA_HOME:-}" ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
        # IBM's JDK on AIX uses strange locations for the executables
        JAVACMD="$JAVA_HOME/jre/sh/java"
    elif [ -x "$JAVA_HOME/bin/java" ] ; then
        JAVACMD="$JAVA_HOME/bin/java"
    else
        JAVACMD="$JAVA_HOME/jre/bin/java"
    fi
    if [ ! -x "$JAVACMD" ] ; then
        echo "ERROR: JAVA_HOME is set to an invalid directory: $JAVA_HOME
Please set the JAVA_HOME variable in your environment or in $0 to match the location of your Java installation."
        exit 1
    fi
else
    JAVACMD="java"
    which java >/dev/null 2>&1 || (echo "ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
Please set the JAVA_HOME variable in your environment or in $0 to match the location of your Java installation."; exit 1)
fi

#
# Check java version
#
JAVA_VERSION="`${JAVACMD} -version 2>&1 | head -n 1`"
if [ "`echo ${JAVA_VERSION} | egrep '.*\"1\.(7|8).*'`" == "" ]; then
  echo "You are using a non compatible java version: ${JAVA_VERSION}"
  echo "We recommend the latest update of OpenJDK 1.8"
  exit 1
fi

# If WARP10_HOME is not defined, set it to the parent directory
if [[ -z "${WARP10_HOME:-}" ]]; then
  WARP10_HOME=`cd $(dirname $0); cd $(pwd -P)/..; pwd -P`
fi

export WARP10_HOME

#
# Data directory that contains logs, leveldb, config defined ?
#
#WARP10_DATA_DIR=/data

if [[ -z "${WARP10_DATA_DIR:-}" ]]; then
  WARP10_DATA_DIR=${WARP10_HOME}
fi

#
# PID File
#
PID_FILE=${WARP10_HOME}/logs/warp10.pid

#
# File to indicate this is the first init of Warp 10 (bootstrap)
#
FIRSTINIT_FILE=${WARP10_HOME}/logs/.firstinit


#
# Classpath
#
WARP10_REVISION=@VERSION@
export WARP10_USER=${WARP10_USER:=warp10}
WARP10_GROUP=${WARP10_GROUP:=warp10}
WARP10_CONFIG_DIR=${WARP10_HOME}/etc/conf.d
WARP10_SECRETS=${WARP10_CONFIG_DIR}/00-secrets.conf
WARP10_JAR=${WARP10_HOME}/bin/warp10-${WARP10_REVISION}.jar
WARP10_CLASS=io.warp10.standalone.Warp
WARP10_INIT=io.warp10.standalone.WarpInit
#
# The lib directory is dedicated to user libraries except of UDF(jars directory): extensions;..
#
WARP10_CP=${WARP10_HOME}/etc:${WARP10_JAR}:${WARP10_HOME}/lib/*
WARP10_HEAP=${WARP10_HEAP:-1g}
WARP10_HEAP_MAX=${WARP10_HEAP_MAX:-1g}

LEVELDB_HOME=${WARP10_DATA_DIR}/leveldb

SENSISION_EVENTS_DIR=/var/run/sensision/metrics

#
# Set SENSISIONID as environment variable to identify this instance by Sensision
#
export SENSISIONID=warp10

LOG4J_CONF=${WARP10_HOME}/etc/log4j.properties
JAVA_HEAP_DUMP=${WARP10_HOME}/logs/java.heapdump
# you can specialize your metrics for this instance of Warp10
#SENSISION_DEFAULT_LABELS=-Dsensision.default.labels=instance=warp10-test,env=dev
JAVA_OPTS="-Djava.awt.headless=true -Dlog4j.configuration=file:${LOG4J_CONF} -Dsensision.server.port=0 ${SENSISION_DEFAULT_LABELS:-} -Dsensision.events.dir=${SENSISION_EVENTS_DIR} -Xms${WARP10_HEAP} -Xmx${WARP10_HEAP_MAX} -XX:+UseG1GC ${JAVA_OPTS}"
export MALLOC_ARENA_MAX=1

# Sed suffix allows compatibility between Linux and MacOS
SED_SUFFIX=".bak"

moveDir() {
  dir=$1
  if [[ -e ${WARP10_DATA_DIR}/${dir} ]]; then
      echo "Error: ${WARP10_DATA_DIR}/${dir} already exists"
      exit 1
  fi
  su ${WARP10_USER} -c "mv ${WARP10_HOME}/${dir} ${WARP10_DATA_DIR}/ 2>&1"
  if [[ $? != 0 ]]; then
    echo "ERROR: move ${WARP10_HOME}/${dir} to ${WARP10_DATA_DIR}"
    exit 1
  fi
  ln -s ${WARP10_DATA_DIR}/${dir} ${WARP10_HOME}/${dir}
  chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}/${dir}
  chmod 755 ${WARP10_DATA_DIR}/${dir}
}

#
# Exit this script if user doesn't match
#
isUser() {
  if [ "`whoami`" != "${1}" ]; then
    echo "You must be '${1}' to run this script."
    exit 1
  fi
}

#
# Return 0 if a Warp 10 instance is started
#
isStarted() {
  # Don't use 'ps -p' for docker compatibility
  if [[ -e ${PID_FILE} ]] && ps -Ao pid | grep "^\s*$(cat ${PID_FILE})$" > /dev/null; then
    true
  else
    false
  fi
}

CONFIG_FILES=
getConfigFiles() {
  # Get standard configuration directory
  if [[ -d "${WARP10_CONFIG_DIR}" ]]; then
    CONFIG_FILES=`find ${WARP10_CONFIG_DIR} -not -path "*/\.*" -name "*.conf" | sort | tr '\n' ' ' 2> /dev/null`
  fi

  # Get additional configuration directory
  if [[  -d "${WARP10_EXT_CONFIG_DIR:-}" ]]; then
    CONFIG_FILES="${CONFIG_FILES} `find ${WARP10_EXT_CONFIG_DIR} -not -path "*/\.*" -name "*.conf" | sort | tr '\n' ' ' 2> /dev/null`"
  fi
}

bootstrap() {
  echo "Bootstrap.."

  #
  # Make sure the caller is root
  #
  isUser root

  # warp10 user ?
  if ! id -u "${WARP10_USER}" >/dev/null 2>&1; then
    echo "User '${WARP10_USER}' does not exist - Creating it.."
    # Create user warp10
    if [[ $(command -v useradd || true) ]]; then
      groupadd -f ${WARP10_GROUP}
      useradd --system --home-dir ${WARP10_HOME} --no-create-home --shell /bin/bash --gid ${WARP10_GROUP} ${WARP10_USER}
    elif [[ $(command -v adduser || true) ]]; then
      [[ $(getent group ${WARP10_GROUP}) ]] || addgroup ${WARP10_GROUP}
      adduser --system --home ${WARP10_HOME} --no-create-home --shell /bin/bash --ingroup ${WARP10_GROUP} ${WARP10_USER}
    elif [[ $(command -v dscl || true) ]]; then
      if [[ ! $(grep -q "^${WARP10_GROUP}:" /etc/group || true) ]]; then
        dscl . -create /Groups/${WARP10_GROUP}
        dscl . -create /Groups/${WARP10_GROUP} gid 10042
      fi
      gid=$(dscl . -read /Groups/${WARP10_GROUP} | awk '($1 == "PrimaryGroupID:") { print $2 }')
      dscl . -create /Users/${WARP10_USER}
      dscl . -create /Users/${WARP10_USER} UniqueID 10042
      dscl . -create /Users/${WARP10_USER} PrimaryGroupID ${gid}
      dscl . -create /Users/${WARP10_USER} UserShell /bin/bash
    else
      echo "Cannot create the ${WARP10_USER} user with home directory ${WARP10_HOME}. Create it manually then run the script again."
      exit 1
    fi
  fi

  #
  # If config file already exists then.. exit
  #
  getConfigFiles
  if [[ ! -z "${CONFIG_FILES}" ]]; then
    echo "Config file already exists - Abort bootstrap..."
    exit 2
  fi

  # Fix ownership
  echo "Fix ownership.."
  echo "WARP10_HOME: ${WARP10_HOME}"

  # Trailing slash is needed when ${WARP10_HOME} is a symlink
  chown -R ${WARP10_USER}:${WARP10_GROUP} ${WARP10_HOME}/

  # Fix permissions
  echo "Fix permissions.."
  chmod 750 ${WARP10_HOME}
  chmod 755 ${WARP10_HOME}/bin
  chmod 755 ${WARP10_HOME}/etc
  chmod 755 ${WARP10_HOME}/logs
  chmod 755 ${WARP10_HOME}/macros
  chmod 755 ${WARP10_HOME}/jars
  chmod 755 ${WARP10_HOME}/lib
  chmod 755 ${WARP10_HOME}/templates
  chmod 755 ${WARP10_HOME}/warpscripts
  chmod 755 ${WARP10_HOME}/etc/throttle
  chmod 755 ${WARP10_HOME}/etc/trl
  chmod 755 ${WARP10_HOME}/etc/bootstrap
  chmod 644 ${WARP10_HOME}/etc/bootstrap/*.mc2
  chmod 755 ${WARP10_HOME}/bin/*.sh
  chmod 755 ${WARP10_HOME}/bin/*.init
  chmod 644 ${WARP10_HOME}/bin/*.service
  chmod 644 ${WARP10_HOME}/bin/warp10-@VERSION@.jar
  chmod -R 755 ${WARP10_HOME}/datalog
  chmod -R 755 ${WARP10_HOME}/datalog_done
  chmod -R 755 ${WARP10_HOME}/leveldb

  #
  # Test access to WARP10_HOME for WARP10_USER
  #
  su ${WARP10_USER} -c "ls ${WARP10_HOME} >/dev/null 2>&1"
  if [ $? != 0 ]; then
    echo "ERROR: ${WARP10_USER} user cannot access to ${WARP10_HOME}"
    exit 1
  fi

  #
  # ${WARP10_HOME} != ${WARP10_DATA_DIR}
  # A dedicated data directory has been provided
  # Move data to ${WARP10_DATA_DIR}/etc, ${WARP10_DATA_DIR}/logs, ${WARP10_DATA_DIR}/leveldb..
  #
  if [ "${WARP10_DATA_DIR}" != "${WARP10_HOME}" ]; then
    echo "WARP10_DATA_DIR: ${WARP10_DATA_DIR}"

    #
    # ${WARP10_DATA_DIR} exists ?
    #
    if [ ! -e ${WARP10_DATA_DIR} ]; then
      echo "${WARP10_DATA_DIR} does not exist - Creating it..."
      mkdir -p ${WARP10_DATA_DIR}
      if [ $? != 0 ]; then
        echo "${WARP10_DATA_DIR} creation failed"
        exit 1
      fi
    fi

    # force ownerships / permissions
    chown ${WARP10_USER}:${WARP10_GROUP} ${WARP10_DATA_DIR}
    chmod 750 ${WARP10_DATA_DIR}

    #
    # Test access to WARP10_DATA_DIR and its parent directories
    #
    su ${WARP10_USER} -c "ls ${WARP10_DATA_DIR} >/dev/null 2>&1"
    if [ $? != 0 ]; then
      echo "ERROR: Cannot access to ${WARP10_DATA_DIR}"
      exit 1
    fi

    # Move directories to ${WARP10_DATA_DIR}
    moveDir logs
    moveDir etc
    moveDir leveldb
    moveDir datalog
    moveDir datalog_done
    moveDir macros
    moveDir jars
    moveDir lib
    moveDir warpscripts

  fi

  WARP10_HOME_ESCAPED=$(echo ${WARP10_HOME} | sed 's/\\/\\\\/g' )           # Escape \
  WARP10_HOME_ESCAPED=$(echo ${WARP10_HOME_ESCAPED} | sed 's/\&/\\&/g' )    # Escape &
  WARP10_HOME_ESCAPED=$(echo ${WARP10_HOME_ESCAPED} | sed 's/|/\\|/g' )     # Escape | (separator for sed)

  LEVELDB_HOME_ESCAPED=$(echo ${LEVELDB_HOME} | sed 's/\\/\\\\/g' )           # Escape \
  LEVELDB_HOME_ESCAPED=$(echo ${LEVELDB_HOME_ESCAPED} | sed 's/\&/\\&/g' )    # Escape &
  LEVELDB_HOME_ESCAPED=$(echo ${LEVELDB_HOME_ESCAPED} | sed 's/|/\\|/g' )     # Escape | (separator for sed)

  # Copy the template configuration file
  for file in ${WARP10_HOME}/conf.templates/standalone/*.template
  do
    filename=`basename $file`
    cp "${file}" ${WARP10_CONFIG_DIR}/${filename%.template}
  done

  sed -i${SED_SUFFIX} -e 's|^standalone\.home.*|standalone.home = '${WARP10_HOME_ESCAPED}'|' ${WARP10_CONFIG_DIR}/*
  rm ${WARP10_CONFIG_DIR}/*${SED_SUFFIX}

  sed -i${SED_SUFFIX} -e 's|^\(\s\{0,100\}\)WARP10_HOME=/opt/warp10-.*|\1WARP10_HOME='${WARP10_HOME_ESCAPED}'|' ${WARP10_HOME}/bin/snapshot.sh
  sed -i${SED_SUFFIX} -e 's|^\(\s\{0,100\}\)LEVELDB_HOME=${WARP10_HOME}/leveldb|\1LEVELDB_HOME='${LEVELDB_HOME_ESCAPED}'|' ${WARP10_HOME}/bin/snapshot.sh
  rm ${WARP10_HOME}/bin/snapshot.sh${SED_SUFFIX}

  sed -i${SED_SUFFIX} -e 's|warpLog\.File=.*|warpLog.File='${WARP10_HOME_ESCAPED}'/logs/warp10.log|' ${WARP10_HOME}/etc/log4j.properties
  sed -i${SED_SUFFIX} -e 's|warpscriptLog\.File=.*|warpscriptLog.File='${WARP10_HOME_ESCAPED}'/logs/warpscript.out|' ${WARP10_HOME}/etc/log4j.properties
  rm ${WARP10_HOME}/etc/log4j.properties${SED_SUFFIX}

  # Generate secrets
  ${WARP10_HOME}/etc/generate_crypto_key.py ${WARP10_SECRETS}
  chown -R ${WARP10_USER}:${WARP10_GROUP} ${WARP10_CONFIG_DIR}


  getConfigFiles

  # Edit the warp10-tokengen.mc2 to use or not the secret
  secret=`su ${WARP10_USER} -c "${JAVACMD} -cp ${WARP10_CP} io.warp10.WarpConfig ${CONFIG_FILES} 'token.secret' | grep 'token.secret' | sed -e 's/^.*=//'"`
  if [[ "${secret}"  != "null" ]]; then
    sed -i${SED_SUFFIX} -e "s|^{{secret}}|'"${secret}"'|" ${WARP10_HOME}/templates/warp10-tokengen.mc2
  else
    sed -i${SED_SUFFIX} -e "s|^{{secret}}||" ${WARP10_HOME}/templates/warp10-tokengen.mc2
  fi
  rm ${WARP10_HOME}/templates/warp10-tokengen.mc2${SED_SUFFIX}

  # Generate read/write tokens valid for a period of 100 years. We use 'io.warp10.bootstrap' as application name.
  su ${WARP10_USER} -c "${JAVACMD} -cp ${WARP10_JAR} io.warp10.worf.TokenGen ${CONFIG_FILES} ${WARP10_HOME}/templates/warp10-tokengen.mc2 ${WARP10_HOME}/etc/initial.tokens"
  sed -i${SED_SUFFIX} 's/^.\{1\}//;$ s/.$//' ${WARP10_HOME}/etc/initial.tokens # Remove first and last character
  rm "${WARP10_HOME}/etc/initial.tokens${SED_SUFFIX}"

  echo "Warp 10 config has been generated here: ${WARP10_CONFIG_DIR}"

  touch ${FIRSTINIT_FILE}

}

start() {

  #
  # Make sure the caller is WARP10_USER
  #
  isUser ${WARP10_USER}

  if [[ -f ${JAVA_HEAP_DUMP} ]]; then
    mv "${JAVA_HEAP_DUMP}" "${JAVA_HEAP_DUMP}-`date +%s`"
  fi

  if isStarted; then
    echo "Start failed! - A Warp 10 instance is currently running"
    exit 1
  fi

  #
  # Get all configurations files
  #
  getConfigFiles

  #
  # Config file exists ?
  #
  if [[ -z "${CONFIG_FILES}" ]]; then
    echo "Config file does not exist - Use 'bootstrap' command before the very first launch (it must be run as root)"
    echo "WARNING: Since version 2.1.0, Warp 10 can use multiple configuration files. The files have to be present in ${WARP10_CONFIG_DIR}"
    exit 1
  fi

  LEVELDB_HOME="`${JAVACMD} -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${CONFIG_FILES} 'leveldb.home' | grep 'leveldb.home' | sed -e 's/^.*=//'`"

  #
  # Leveldb exists ?
  #
  if [ ! -e ${LEVELDB_HOME} ]; then
    echo "${LEVELDB_HOME} does not exist - Creating it..."
    mkdir -p ${LEVELDB_HOME} 2>&1
    if [ $? != 0 ]; then
      echo "${LEVELDB_HOME} creation failed"
      exit 1
    fi
  fi

  if [ "$(find -L ${LEVELDB_HOME} -maxdepth 1 -type f | wc -l)" -eq 0 ]; then
    echo "Init leveldb"
    # Create leveldb database
    echo \"Init leveldb database...\" >> ${WARP10_HOME}/logs/warp10.log
    ${JAVACMD} ${JAVA_OPTS} -cp ${WARP10_CP} ${WARP10_INIT} ${LEVELDB_HOME} >> ${WARP10_HOME}/logs/warp10.log 2>&1
  fi

  WARP10_LISTENSTO_HOST="`${JAVACMD} -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${CONFIG_FILES} 'standalone.host' | grep 'standalone.host' | sed -e 's/^.*=//'`"
  WARP10_LISTENSTO_PORT="`${JAVACMD} -Xms64m -Xmx64m -XX:+UseG1GC -cp ${WARP10_CP} io.warp10.WarpConfig ${CONFIG_FILES} 'standalone.port' | grep 'standalone.port' | sed -e 's/^.*=//'`"
  WARP10_LISTENSTO="${WARP10_LISTENSTO_HOST}:${WARP10_LISTENSTO_PORT}"

  #
  # Start Warp10 instance..
  # By default, standard and error output is redirected to warp10.log file, and error output is duplicated to standard output
  # As a consequence, if Warp 10 is launched by systemd, error messages will be in systemd journal too.
  #
  ${JAVACMD} ${JAVA_OPTS} -cp ${WARP10_CP} ${WARP10_CLASS} ${CONFIG_FILES} >> ${WARP10_HOME}/logs/warp10.log 2> >(tee >(cat 1>&2)) &

  echo $! > ${PID_FILE}

  if ! isStarted; then
    echo "Start failed! - See ${WARP10_HOME}/logs/warp10.log for more details"
    exit 1
  fi

  echo '  ___       __                           ____________   '
  echo '  __ |     / /_____ _______________      __<  /_  __ \  '
  echo '  __ | /| / /_  __ `/_  ___/__  __ \     __  /_  / / /  '
  echo '  __ |/ |/ / / /_/ /_  /   __  /_/ /     _  / / /_/ /   '
  echo '  ____/|__/  \__,_/ /_/    _  .___/      /_/  \____/    '
  echo '                           /_/                          '

  echo "##"
  echo "## Warp 10 listens on ${WARP10_LISTENSTO}"
  echo "##"

  if [ -e ${FIRSTINIT_FILE} ]; then

    #
    # Output the generated tokens
    #

    READ_TOKEN=`tail -n 1 ${WARP10_HOME}/etc/initial.tokens | sed -e 's/{"read":{"token":"//' -e 's/".*//'`
    WRITE_TOKEN=`tail -n 1 ${WARP10_HOME}/etc/initial.tokens | sed -e 's/.*,"write":{"token":"//' -e 's/".*//'`

    echo "##"
    echo "## An initial set of tokens was generated for you so you can immediately use Warp 10:"
    echo "##"
    echo "## Write Token: ${WRITE_TOKEN}"
    echo "## Read Token: ${READ_TOKEN}"
    echo "##"
    echo "## Push some test data using:"
    echo "##"
    echo "##   curl -H 'X-Warp10-Token: ${WRITE_TOKEN}' http://${WARP10_LISTENSTO}/api/v0/update --data-binary '// test{} 42'"
    echo "##"
    echo "## And read it back using:"
    echo "##"
    echo "##   curl 'http://${WARP10_LISTENSTO}/api/v0/fetch?token=${READ_TOKEN}&selector=~.*\{\}&now=now&timespan=-1'"
    echo "##"
    echo "## You can submit WarpScript for execution via:"
    echo "##"
    echo "##   curl http://${WARP10_LISTENSTO}/api/v0/exec --data-binary @path/to/WarpScriptFile"
    echo "##"
    rm -f ${FIRSTINIT_FILE}

  fi

  # Check again 5s later (time for plugin load errors)
  sleep 5
  if ! isStarted; then
    echo "Start failed! - See ${WARP10_HOME}/logs/warp10.log for more details"
    exit 1
  fi

}

stop() {

  #
  # Make sure the caller is WARP10_USER
  #
  isUser ${WARP10_USER}

  if isStarted; then
    echo "Stop Warp 10..."
    kill $(cat ${PID_FILE})
    echo "Wait for Warp 10 to stop..."
    while $(kill -0 $(cat ${PID_FILE}) 2>/dev/null); do
      sleep 2
    done
    echo "Warp 10 stopped..."
    rm -f ${PID_FILE}
  else
    echo "No instance of Warp 10 is currently running"
  fi
}

status() {

  #
  # Make sure the caller is WARP10_USER
  #
  isUser ${WARP10_USER}

  if isStarted; then
    ps -Ao pid,etime,args | grep "^\s*$(cat ${PID_FILE})\s"
  else
    echo "No instance of Warp 10 is currently running"
  fi
}

snapshot() {
  if [ $# -ne 2 -a $# -ne 3 ]; then
    echo $"Usage: $0 {snapshot 'snapshot_name' ['base_snapshot_name']}"
    exit 2
  fi
  # Name of snapshot
  SNAPSHOT=$2
  if [ $# -eq 2 ]; then
    ${WARP10_HOME}/bin/snapshot.sh ${SNAPSHOT} "${WARP10_HOME}" "${LEVELDB_HOME}" "${PID_FILE}"
  else
    BASE_SNAPSHOT=$3
    ${WARP10_HOME}/bin/snapshot.sh ${SNAPSHOT} ${BASE_SNAPSHOT} "${WARP10_HOME}" "${LEVELDB_HOME}" "${PID_FILE}"
  fi
}

worf() {

  #
  # Make sure the caller is WARP10_USER
  #
  isUser ${WARP10_USER}

  if [ "$#" -ne 3 ]; then
    echo "Usage: $0 $1 appName ttl(ms)"
    exit 1
  fi
  ${JAVACMD} -cp ${WARP10_JAR} io.warp10.worf.Worf ${WARP10_SECRETS} -puidg -t -a $2 -ttl $3
}

repair() {

  #
  # Make sure the caller is WARP10_USER
  #
  isUser ${WARP10_USER}

  if isStarted; then
    echo "Repair has been cancelled! - Warp 10 instance must be stopped for repair"
    exit 1
  else
    echo "Repair Leveldb..."
    ${JAVACMD} -cp ${WARP10_JAR} io.warp10.standalone.WarpRepair ${LEVELDB_HOME}
  fi
}

# See how we were called.
case "$1" in
  bootstrap)
  bootstrap
  ;;
  start)
  start
  ;;
  jmxstart)
  JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=${JMX_PORT}"
  echo "## WARNING: JMX is enabled on port ${JMX_PORT}"
  start
  ;;
  stop)
  stop
  ;;
  status)
  status
  ;;
  restart)
  stop
  sleep 2
  start
  ;;
  jmxrestart)
  stop
  sleep 2
  JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=${JMX_PORT}"
  echo "## WARNING: JMX is enabled on port ${JMX_PORT}"
  start
  ;;
  worfcli)
  worfcli
  ;;
  worf)
  worf "$@"
  ;;
  snapshot)
  snapshot "$@"
  ;;
  repair)
  repair
  ;;
  *)
  echo $"Usage: $0 {bootstrap|start|jmxstart|stop|status|worf appName ttl(ms)|snapshot 'snapshot_name'|repair|restart|jmxrestart}"
  exit 2
esac

exit $?
