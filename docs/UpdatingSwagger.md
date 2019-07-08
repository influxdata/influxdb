## Updating the Swagger.yml

* get latest from master
* create a separate branch
* update swagger.yml with your change
* once the change is merged in master, cd into influxdb2-js dir
* get latest from master and create a separate branch
* run ‘yarn generate’ command in influxdb2-js dir
* yarn buid && yarn lint
* git status
* git add .
* git commit -m “message”
* git push
* create pr and merge it in
* get latest from master
* create a separate branch
* update influxdb2-js  in package.json with correct version
