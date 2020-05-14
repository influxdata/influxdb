#!/bin/sh

USE_DOCKER=1 # Docker or tarball
USE_ALPHA=0 # alpha or nightly
DEBUG=0
ACTION="setup"
TAR_DOWNLOAD="https://dl.influxdata.com/influxdb/releases/influxdb_2.0.0-alpha.1_linux_amd64.tar.gz"
DOCKER_IMAGE="quay.io/influxdb/influx:nightly"
INSTANCE_NAME="influx2_solo"
INFLUX2_HOME="${HOME}/.influxdbv2"
LOG_DIR=${PWD}/log
LOG_FILE=${LOG_DIR}/docker.log
TELEGRAF_DOWNLOAD="https://dl.influxdata.com/telegraf/releases/telegraf_1.8.2-1_amd64.deb"


download_telegraf(){
   echo "["$(date +"%d.%m.%Y %T")"] downloading telegraf.deb - if changed"
#curl -s -o telegraf.deb -z telegraf.deb https://dl.influxdata.com/telegraf/releases/telegraf_1.7.2-1_amd64.deb
   curl -s -o telegraf.deb -z telegraf.deb https://dl.influxdata.com/telegraf/releases/telegraf_1.8.2-1_amd64.deb
}

install_telegraf(){
  echo "["$(date +"%d.%m.%Y %T")"] installing telegraf.deb - if changed"
  sudo dpkg -i telegraf.deb
  echo "["$(date +"%d.%m.%Y %T")"] installed telegraf.deb - if changed"
}


stop_telegraf(){

   echo "["$(date +"%d.%m.%Y %T")"] stoping telegraf.service - if changed"
   sudo systemctl stop telegraf.service
   echo "["$(date +"%d.%m.%Y %T")"] stoped telegraf.service - if changed"

}

pull_docker(){
   echo "["$(date +"%d.%m.%Y %T")"] pulling ${DOCKER_IMAGE}"
   docker pull ${DOCKER_IMAGE}
}

run_docker_influx(){
   mkdir -p ${LOG_DIR}
   echo "["$(date +"%d.%m.%Y %T")"] starting docker instance ${INSTANCE_NAME}"
   sudo docker run --name ${INSTANCE_NAME} --publish 9999:9999 ${DOCKER_IMAGE} > ${LOG_FILE} 2>&1 &
   echo "["$(date +"%d.%m.%Y %T")"] started instance $INSTANCE_NAME listening at port 9999."
   echo "logfile at $LOG_FILE"
   sleep 3
   echo "\n$(tail -n32 $LOG_FILE)\n"
}

run_docker_influx_test_env(){

   mkdir -p ${LOG_DIR}
   sudo docker stop $INSTANCE_NAME
   sudo docker rm $INSTANCE_NAME
   sudo docker pull quay.io/influxdb/influx:nightly
   sudo docker build -t influxdb_test_image .
   sudo docker run --name $INSTANCE_NAME --publish 9999:9999 influxdb_test_image > ${LOG_FILE} 2>&1 &

}

stop_docker_influx(){
  if [ "$(sudo docker ps -q -f name=$INSTANCE_NAME)" ] ; then
      echo "["$(date +"%d.%m.%Y %T")"] stopping docker instance ${INSTANCE_NAME}"
      sudo docker stop ${INSTANCE_NAME}
      echo "["$(date +"%d.%m.%Y %T")"] stopped $INSTANCE_NAME"
   fi
}

remove_docker_influx(){
  if [ "$(sudo docker ps -a -q -f name=$INSTANCE_NAME)" ] ; then
      echo "["$(date +"%d.%m.%Y %T")"] removing docker instance ${INSTANCE_NAME}"
      sudo docker rm ${INSTANCE_NAME}
      echo "["$(date +"%d.%m.%Y %T")"] removed $INSTANCE_NAME"
  fi
}

clean_influx_home(){
  echo "["$(date +"%d.%m.%Y %T")"] cleaning  ${INFLUX2_HOME}"
  sudo rm -rf ${INFLUX2_HOME}
  echo "["$(date +"%d.%m.%Y %T")"] cleaned  ${INFLUX2_HOME}"
}

usage(){
  echo "usage $0"
  echo "   -a|--alpha     use alpha release - otherwise nightly build is used"
  echo "   -t|--tarball   use tarball build - otherwise docker is used"
  echo "   -n|--name      set name of docker container - default $INSTANCE_NAME"
  echo "   setup|start    start the service daemon - default action"
  echo "   shutdown|stop  shutdown the service daemon"
  echo "   clean          clean(remove) local directory ${INFLUX_HOME}"
  echo "   -h|--help      print this message"
}

# Will need to get the telegraf config on first creating org
# then restart telegraf...  Will be part of test cases

while [ "$1" != "" ]; do
    case $1 in
        -a | --alpha )          USE_ALPHA=1
                                DOCKER_IMAGE="quay.io/influxdb/influxdb:2.0.0-alpha"
                                ;;
        -t | --tarball )        USE_DOCKER=0
                                ;;
        -n | --name )           shift
                                INSTANCE_NAME=$1
                                ;;
        setup | start )         ACTION="setup"
                                ;;
        setup-qa | start-qa )   ACTION="setup-qa"
                                ;;
        stop | shutdown )       ACTION="stop"
                                ;;
        clean )                 ACTION="clean"
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        -d | --debug )          DEBUG=1
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done

if [ $DEBUG -gt 0 ] ; then
  echo "USE_DOCKER          $USE_DOCKER"
  echo "USE_ALPHA           $USE_ALPHA"
  echo "ACTION              $ACTION"
  echo "TAR_DOWNLOAD        $TAR_DOWNLOAD"
  echo "DOCKER_IMAGE        $DOCKER_IMAGE"
  echo "INSTANCE_NAME       $INSTANCE_NAME"
  echo "INFLUX2_HOME        $INFLUX2_HOME"
  echo "LOG_FILE            $LOG_FILE"
  echo "TELEGRAF_DOWNLOAD   $TELEGRAF_DOWNLOAD"
fi

if [ $USE_ALPHA -ne 0 ] ; then
  echo "USING ALPHA"
fi

if [ $USE_ALPHA -eq 0 ] && [ $USE_DOCKER -eq 0 ] ; then
  echo "Nightly builds with tar balls not supported at this time"
  exit 1
fi

if [ $USE_DOCKER -eq 0 ] ; then
  echo "tarball install - not yet supported"
  exit
fi

#if [ $USE_ALPHA -eq 0 ] && [ $USE_DOCKER -gt 0 ] ; then
case $ACTION in
  setup )     stop_docker_influx
              remove_docker_influx
              download_telegraf
              pull_docker
              run_docker_influx
              ;;
  setup-qa)   run_docker_influx_test_env
              ;;
  stop )      stop_docker_influx
              ;;
  clean )     clean_influx_home
              ;;
   * )        echo "Unhandled ACTION $ACTION"
              exit 1
esac

#if [  "$ACTION" = "setup" ] ; then
#   stop_docker_influx
#   remove_docker_influx
#   download_telegraf
   #  install_telegraf
   #  stop_telegraf
#   pull_docker_nightly
#   run_docker_influx
#elif [ "$ACTION" = "stop" ] ; then  # stop docker nightly
#   stop_docker_influx
# else
#   echo "Unhandled ACTION $ACTION"
#   exit 1
#fi
#fi

#if [ -z $1 ] ; then  # check if first cmdline param is not set
#  echo "["$(date +"%d.%m.%Y %T")"] USAGE: $0 [setup|start|stop]"
#elif [ "$1" = "setup" ] || [ "$1" = "start" ] ; then
#  stop_docker_influx
#  remove_docker_influx
#  download_telegraf
##  install_telegraf
##  stop_telegraf
#  pull_docker_nightly
#  run_docker_influx
#elif [ "$1" = "stop" ] ; then
#  stop_docker_influx
#fi
