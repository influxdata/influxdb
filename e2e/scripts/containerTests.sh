#!/usr/bin/env bash

APP_ROOT="$(dirname "$(dirname "$(readlink -fm "$0")")")"
TEST_CONTAINER=bonitoo_e2e
INFLUX2_CONTAINER=influx2_solo
E2E_MAP_DIR=/tmp/e2e
INFLUX2_HOST=$(sudo docker inspect -f "{{ .NetworkSettings.IPAddress }}" ${INFLUX2_CONTAINER})
INFLUX2_URL="http://${INFLUX2_HOST}:9999"
#TAGS="@influx-influx"
ACTIVE_CONF=development

echo ------ CHECK ENV IN containerTests.sh --------
env
echo ------- END ENV CHECK ------------------------

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
   -t| --tags)
   TAGS="$2"
   shift; # past argument
   shift; # past val
   ;;
   -c| --config)
   ACTIVE_CONF="$2"
   shift;
   shift;
   ;;
   -b| --base)
   BASE_DIR="$2"
   shift;
   shift;
esac
done

echo E2E_CLOUD_DEFAULT_USER_USERNAME = ${E2E_CLOUD_DEFAULT_USER_USERNAME}

echo Working from ${APP_ROOT}

DOCKER_TEST_CMD="npm test -- activeConf=${ACTIVE_CONF}"

if [[ -n "${TAGS}" ]]; then
    DOCKER_TEST_CMD="${DOCKER_TEST_CMD} --tags ${TAGS}"
fi

if [[ -n "${BASE_DIR+x}" ]]; then
    DOCKER_TEST_CMD="${DOCKER_TEST_CMD} ${BASE_DIR}"
fi

echo DOCKER_TEST_CMD = ${DOCKER_TEST_CMD}

if [ ${ACTIVE_CONF} = 'cloud' ]; then
    echo configuring for cloud
    if [ -z ${E2E_CLOUD_INFLUX_URL+x} ]; then
      echo
      echo E2E_CLOUD_INFLUX_URL is unset
      echo But cloud configuration chosen
      echo
      echo Please set E2E_CLOUD_INFLUX_URL to use cloud configuration
      exit 1
    else
      echo E2E_CLOUD_INFLUX_URL ${E2E_CLOUD_INFLUX_URL}
      INFLUX2_URL=${E2E_CLOUD_INFLUX_URL}
    fi

    if [ -z ${E2E_CLOUD_DEFAULT_USER_PASSWORD} ]; then
      echo
      echo E2E_CLOUD_DEFAULT_USER_PASSWORD is unset
      echo But cloud configuration chosen
      echo
      echo Please set E2E_CLOUD_DEFAULT_USER_PASSWORD to use cloud configuration
      exit 1
    fi
fi

echo "------ Targeting influx at ${INFLUX2_URL} ------"


# Tear down running test container
echo "----- Tearing down test container ${TEST_CONTAINER} ------"

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

DOCKER_ENVARS="-e SELENIUM_REMOTE_URL=http://${SELENOID_HOST}:4444/wd/hub -e E2E_${ACTIVE_CONF^^}_INFLUX_URL=${INFLUX2_URL}"

if [ -n ${E2E_CLOUD_DEFAULT_USER_PASSWORD} ]; then
   DOCKER_ENVARS="${DOCKER_ENVARS} -e E2E_CLOUD_DEFAULT_USER_PASSWORD=${E2E_CLOUD_DEFAULT_USER_PASSWORD}"
fi

sudo docker build -t e2e-${TEST_CONTAINER} -f scripts/Dockerfile.tests .
sudo docker run -it -v `pwd`/report:/home/e2e/report -v `pwd`/screenshots:/home/e2e/screenshots \
     -v /tmp/e2e/etc:/home/e2e/etc -v /tmp/e2e/downloads:/home/e2e/downloads \
     ${DOCKER_ENVARS} --detach \
     --name ${TEST_CONTAINER} e2e-${TEST_CONTAINER}:latest

echo ACTIVE_CONF ${ACTIVE_CONF} BASE_DIR ${BASE_DIR} TAGS ${TAGS}

sudo docker exec ${TEST_CONTAINER} ${DOCKER_TEST_CMD}

sudo docker exec ${TEST_CONTAINER} npm run report:html
sudo docker exec ${TEST_CONTAINER} npm run report:junit

sudo docker stop ${TEST_CONTAINER}
sudo docker stop selenoid

