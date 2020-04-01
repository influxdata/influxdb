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

#
# Script to create a snapshot of the leveldb (standalone) version of Warp.
#

WARP10_USER=${WARP10_USER:=warp10}

#
# Make sure the caller is warp10
#
if [ "`whoami`" != "${WARP10_USER}" ]
then
  echo "You must be ${WARP10_USER} to run this script."
  exit 1
fi

if [ "$#" -eq 1 ]; then
  # Name of snapshot
  SNAPSHOT=$1
  # default
  WARP10_HOME=/opt/warp10-@VERSION@
  LEVELDB_HOME=${WARP10_HOME}/leveldb
  PID_FILE=${WARP10_HOME}/logs/warp10.pid
elif [ "$#" -eq 2 ]; then
  SNAPSHOT=$1
  BASE_SNAPSHOT=$2
  WARP10_HOME=/opt/warp10-@VERSION@
  LEVELDB_HOME=${WARP10_HOME}/leveldb
  PID_FILE=${WARP10_HOME}/logs/warp10.pid
elif [ "$#" -eq 4 ]; then
  # Name of snapshot
  SNAPSHOT=$1
  # default
  WARP10_HOME=$2
  LEVELDB_HOME=$3
  PID_FILE=$4
elif [ "$#" -eq 5 ]; then
  SNAPSHOT=$1
  BASE_SNAPSHOT=$2
  WARP10_HOME=$3
  LEVELDB_HOME=$4
  PID_FILE=$5
else
  echo "Usage: $0 'snapshot-name' [ 'base-snapshot-name' ] [ '{WARP10_HOME}' '{LEVELDB_HOME}' '{PID_FILE}' ]"
  exit 1
fi

WARP10_CONFIG_DIR=${WARP10_HOME}/etc/conf.d
# Snapshot directory, MUST be on the same device as LEVELDB_HOME so we can create hard links
SNAPSHOT_DIR=${LEVELDB_HOME}/snapshots

# Path to the 'trigger' file
TRIGGER_PATH=${LEVELDB_HOME}/snapshot.trigger
# Path to the 'signal' file
SIGNAL_PATH=${LEVELDB_HOME}/snapshot.signal

if [ "" = "${SNAPSHOT}" ]
then
  echo "Snapshot name is empty."
  exit 1
fi

# Check that the base snapshot exists
if [ "" != "${BASE_SNAPSHOT}" ]
then
  if [ ! -d "${SNAPSHOT_DIR}/${BASE_SNAPSHOT}" ]
  then
    echo "Base snapshot ${BASE_SNAPSHOT} does not exist."
    exit 1
  fi

  # List the '.sst' files of the base snapshot
  find -L "${SNAPSHOT_DIR}/${BASE_SNAPSHOT}" -maxdepth 1 -name '*.sst' | sed -e 's,.*/,,' | sort -u > ${SNAPSHOT_DIR}/${BASE_SNAPSHOT}/sst.files
fi

#
# Check if Warp 10 instance is currently running
#
# Don't use 'ps -p' for docker compatibility
if [ ! -e ${PID_FILE} ] || ! ps -Ao pid | grep "^\s*$(cat ${PID_FILE})$" > /dev/null; then
  echo "No Warp 10 instance is currently running !"
  exit 1
fi

#
# Check if snapshot already exists
#

if [ -e "${SNAPSHOT_DIR}/${SNAPSHOT}" ]
then
  echo "Snapshot '${SNAPSHOT_DIR}/${SNAPSHOT}' already exists"
  exit 1
fi

#
# Check snapshots and leveldb data dir are on the same mount point
#

if [ "`df -P ${LEVELDB_HOME}|sed '1d'|awk '{ print $1 }'`" != "`df -P ${SNAPSHOT_DIR}|sed '1d'|awk '{ print $1 }'`" ]
then
  echo "'${SNAPSHOT_DIR}' and '${LEVELDB_HOME}' must be mounted onto the same mount point."
  exit 1
fi

#
# Bail out if 'signal' path exists
#

if [ -e "${SIGNAL_PATH}" ]
then
  echo "Signal file '${SIGNAL_PATH}' already exists, aborting."
  exit 1
fi

#
# Check if 'trigger' path exists, create it if not
#

if [ -e "${TRIGGER_PATH}" ]
then
  echo "Trigger file '${TRIGGER_PATH}' already exists, aborting"
  exit 1
else
  touch  ${TRIGGER_PATH}
fi

#
# Wait for the 'signal' file to appear
#

while [ ! -e "${SIGNAL_PATH}" ]
do
  sleep 1
done

#
# Create snapshot directory
#

mkdir ${SNAPSHOT_DIR}/${SNAPSHOT}
cd ${SNAPSHOT_DIR}/${SNAPSHOT}

# List sst files from leveldb directory
find -L ${LEVELDB_HOME} -maxdepth 1 -name '*sst'|sed -e 's,.*/,,'|sort -u > ${SNAPSHOT_DIR}/${SNAPSHOT}/sst.leveldb

#
# Create hard links of '.sst' files
#

STATUS=1

if [ "" == "${BASE_SNAPSHOT}" ]
then
  find -L ${LEVELDB_HOME} -maxdepth 1 -name '*sst'|xargs echo|while read FILES; do if [ -n "${FILES}" ]; then ln ${FILES} ${SNAPSHOT_DIR}/${SNAPSHOT}; fi; done
  STATUS=$?
else
  pushd ${LEVELDB_HOME} > /dev/null 2>&1
  comm -23 ${SNAPSHOT_DIR}/${SNAPSHOT}/sst.leveldb ${SNAPSHOT_DIR}/${BASE_SNAPSHOT}/sst.files|xargs echo|while read FILES; do if [ -n "${FILES}" ]; then ln ${FILES} ${SNAPSHOT_DIR}/${SNAPSHOT}; fi; done
  STATUS=$?
  popd > /dev/null 2>&1
fi

if [ ${STATUS} != 0 ]
then
  echo "Hard link creation failed - Cancel Snapshot !"
  rm -rf ${SNAPSHOT_DIR:?}/${SNAPSHOT}
  exit 1
fi


#
# Copy CURRENT and MANIFEST
#

cp ${LEVELDB_HOME}/CURRENT ${SNAPSHOT_DIR}/${SNAPSHOT}
cp ${LEVELDB_HOME}/MANIFEST-* ${SNAPSHOT_DIR}/${SNAPSHOT}
cp ${LEVELDB_HOME}/LOG* ${SNAPSHOT_DIR}/${SNAPSHOT}
cp ${LEVELDB_HOME}/*.log ${SNAPSHOT_DIR}/${SNAPSHOT}

#
# Remove 'trigger' file
#

rm -f ${TRIGGER_PATH}

#
# If using a base snapshot, create links for the files already referenced by the base snapshot
# Do so with an adjusted niceness
#

if [ "" != "${BASE_SNAPSHOT}" ]
then
  pushd ${SNAPSHOT_DIR}/${BASE_SNAPSHOT} > /dev/null 2>&1
  comm -12 ${SNAPSHOT_DIR}/${SNAPSHOT}/sst.leveldb ${SNAPSHOT_DIR}/${BASE_SNAPSHOT}/sst.files|xargs echo|while read FILES; do if [ -n "${FILES}" ]; then nice -n 39 ln ${FILES} ${SNAPSHOT_DIR}/${SNAPSHOT}; fi; done
  STATUS=$?
  popd > /dev/null 2>&1
  if [ ${STATUS} != 0 ]
  then
    echo "Hard link creation failed - Snapshot aborted."
    rm -rf ${SNAPSHOT_DIR:?}/${SNAPSHOT}
    exit 1
  fi
fi

#
# Snapshot configuration (contains hash/aes keys)
#

mkdir ${SNAPSHOT_DIR}/${SNAPSHOT}/warp10-config
# only warp10 user can have access to this config
chmod 700 ${SNAPSHOT_DIR}/${SNAPSHOT}/warp10-config
cp -a ${WARP10_CONFIG_DIR} ${SNAPSHOT_DIR}/${SNAPSHOT}/warp10-config/


if [[ -n "${WARP10_EXT_CONFIG_DIR}" && -d "${WARP10_EXT_CONFIG_DIR}" ]]; then
  cp -r ${WARP10_EXT_CONFIG_DIR} ${SNAPSHOT_DIR}/${SNAPSHOT}/warp10-config/
fi
