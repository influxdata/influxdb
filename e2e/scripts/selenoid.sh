#!/bin/bash

APP_ROOT="$(dirname "$(dirname "$(readlink -fm "$0")")")"

sudo docker stop selenoid
sudo docker rm selenoid

sudo docker run -d --name selenoid -p 4444:4444 -v /var/run/docker.sock:/var/run/docker.sock \
     -v ${APP_ROOT}/etc/selenoid/config/:/etc/selenoid/:ro aerokube/selenoid:latest-release \
     -enable-file-upload -video-output-dir ${APP_ROOT}/etc/selenoid/video -timeout 5m0s

sleep 10

export SELENOID_HOST=$(sudo docker inspect -f "{{ .NetworkSettings.IPAddress }}" selenoid)
echo SELENOID_HOST ${SELENOID_HOST}

sudo docker pull selenoid/vnc:chrome_83.0
#sudo docker pull selenoid/video-recorder:latest-release

if [ "$1" = "-debug"  ]; then 
	echo debugging
	sudo docker stop selenoid-ui
	sudo docker rm selenoid-ui
	docker run -d --name selenoid-ui -p 8080:8080 aerokube/selenoid-ui --selenoid-uri http://${SELENOID_HOST}:4444
fi
