#!/bin/sh
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
# Build the distribution .tgz for Warp 10
#

VERSION=$1
# Warp 10 root project path (../warp10)
WARP_ROOT_PATH=$2

WARP10_HOME=warp10-${VERSION}

ARCHIVE=${WARP_ROOT_PATH}/archive

# Remove existing archive dir
rm -rf ${ARCHIVE}

# Create the directory hierarchy
mkdir ${ARCHIVE}
cd ${ARCHIVE}
mkdir -p ${WARP10_HOME}/bin
mkdir -p ${WARP10_HOME}/calls
mkdir -p ${WARP10_HOME}/datalog
mkdir -p ${WARP10_HOME}/datalog_done
mkdir -p ${WARP10_HOME}/etc/bootstrap
mkdir -p ${WARP10_HOME}/etc/conf.d
mkdir -p ${WARP10_HOME}/etc/throttle
mkdir -p ${WARP10_HOME}/etc/trl
mkdir -p ${WARP10_HOME}/jars
mkdir -p ${WARP10_HOME}/leveldb/snapshots
mkdir -p ${WARP10_HOME}/lib
mkdir -p ${WARP10_HOME}/logs
mkdir -p ${WARP10_HOME}/macros
mkdir -p ${WARP10_HOME}/templates
mkdir -p ${WARP10_HOME}/warpscripts/test/60000


cd ${ARCHIVE}
# Copy startup scripts
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/sh/warp10.service >> ${WARP10_HOME}/bin/warp10.service
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/sh/warp10-standalone.init >> ${WARP10_HOME}/bin/warp10-standalone.init
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/sh/snapshot.sh >> ${WARP10_HOME}/bin/snapshot.sh
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/sh/warp10-standalone.sh >> ${WARP10_HOME}/bin/warp10-standalone.sh

# Copy log4j README, config, runner, bootstrap...
cp ../../etc/bootstrap/*.mc2 ${WARP10_HOME}/etc/bootstrap
cp ../../etc/install/README.md ${WARP10_HOME}
cp ${WARP_ROOT_PATH}/build/changelog.* ${WARP10_HOME}
cp ../../etc/warpscripts/*.mc2* ${WARP10_HOME}/warpscripts/test/60000
cp ../../etc/calls/*.sh ${WARP10_HOME}/calls
cp ../../etc/calls/*.py ${WARP10_HOME}/calls
cp ../../etc/macros/* ${WARP10_HOME}/macros
cp ../../etc/generate_crypto_key.py ${WARP10_HOME}/etc
sed -e "s/@VERSION@/${VERSION}/g" ../../etc/log4j.properties >> ${WARP10_HOME}/etc/log4j.properties

# Copy template configuration
cp -r ../../etc/conf.templates ${WARP10_HOME}/
sed -i -e "s/@VERSION@/${VERSION}/g" ${WARP10_HOME}/conf.templates/*/*
cp  ../../etc/warp10-tokengen.mc2 ${WARP10_HOME}/templates/warp10-tokengen.mc2

# Copy jars
cp ../build/libs/warp10-${VERSION}.jar ${WARP10_HOME}/bin/warp10-${VERSION}.jar

find ${WARP10_HOME} -type d -exec chmod 755 {} \;
find ${WARP10_HOME} -type f -exec chmod 644 {} \;
find ${WARP10_HOME} -type f \( -name "*.sh" -o -name "*.py" -o -name "*.init" \) -exec chmod 755 {} \;

# Build tar
tar czpf ../build/libs/warp10-${VERSION}.tar.gz ${WARP10_HOME}
