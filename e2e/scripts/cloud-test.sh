#!/usr/bin/env bash

npm test -- activeConf=cloud cloud
sleep 15
npm run report:html
npm run report:junit
