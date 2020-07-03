#!/usr/bin/env bash

APP_ROOT="$(dirname "$(dirname "$(readlink -fm "$0")")")"
TEST_CONTAINER=experim
INFLUX2_CONTAINER=influx2_solo
E2E_MAP_DIR=/tmp/e2e
INFLUX2_HOST=$(sudo docker inspect -f "{{ .NetworkSettings.IPAddress }}" ${INFLUX2_CONTAINER})
TAGS="@influx-influx"

echo "------ Targeting influx container ${INFLUX2_CONTAINER} at ${INFLUX2_HOST} ------"

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
   -t| --tags)
   TAGS="$2"
   shift; # past argument
   shift; # past val
esac
done

echo "TAGS $TAGS"

# Tear down running test container
echo "----- Tearing down test container ${TEST_CONTAINER} ------"

echo ${APP_ROOT}

if docker container inspect ${TEST_CONTAINER} > /dev/null 2>&1
then

  if [ "$( docker container inspect -f '{{.State.Running}}' ${TEST_CONTAINER} )" == "true" ]; then

   echo stopping ${TEST_CONTAINER}
   sudo docker stop ${TEST_CONTAINER}

  fi

  echo removing ${TEST_CONTAINER}
  sudo docker rm ${TEST_CONTAINER}

fi

# Ensure mapped dirs are current
echo "----- Ensuring linked dir for volumes is current ------"
if [ -L ${E2E_MAP_DIR}/etc ]; then
     echo ${E2E_MAP_DIR}/etc is linked
     echo removing ${E2E_MAP_DIR}
     sudo rm -r ${E2E_MAP_DIR}
fi
sudo mkdir -p ${E2E_MAP_DIR}
echo linking ${APP_ROOT}/etc
sudo ln -s  ${APP_ROOT}/etc ${E2E_MAP_DIR}/etc

echo "------ (Re)start Selenoid ------"
source ${APP_ROOT}/scripts/selenoid.sh
echo SELENOID_HOST is ${SELENOID_HOST}

# Rebuild and start test container
echo "----- Rebuilding test container ${TEST_CONTAINER} ------"
if [[ -d "$APP_ROOT/report" ]]; then
   echo cleaning "$APP_ROOT/report"
   sudo npm run clean
   rm -rdf report
fi

sudo docker build -t e2e-${TEST_CONTAINER} -f scripts/Dockerfile.experim .
sudo docker run -it -v `pwd`/report:/home/e2e/report -v `pwd`/screenshots:/home/e2e/screenshots \
     -v /tmp/e2e/etc:/home/e2e/etc -v /tmp/e2e/downloads:/home/e2e/downloads \
     -e SELENIUM_REMOTE_URL="http://${SELENOID_HOST}:4444/wd/hub" \
     -e E2E_DEVELOPMENT_INFLUX_URL="http://${INFLUX2_HOST}:9999" --detach \
     --name ${TEST_CONTAINER} e2e-${TEST_CONTAINER}:latest


sudo docker exec ${TEST_CONTAINER} npm test -- --tags "$TAGS"

sudo docker exec ${TEST_CONTAINER} npm run report:html
sudo docker exec ${TEST_CONTAINER} npm run report:junit

sudo docker stop ${TEST_CONTAINER}
sudo docker stop selenoid

