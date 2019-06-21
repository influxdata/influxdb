## Updating the API Client


* Clone influxdata/influxdb2-js repo, if you haven’t
    * cd influxdata/influxdb2-js
    * git pull on master
    * git checkout -b “branch name”
    * open the repo in vs code
    * update the wrapper class with correct api call and return value
    * run ‘yarn build && yarn lint’ in the influxdb2-js dir
    * git add the changed files
    * send a pr for the update to brandon?
    * git pull master on influxdb repo
    * update the package.json with correct version number
    * restart npm