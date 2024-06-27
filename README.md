# InfluxDB Pro
This is a fork of OSS with all the Pro functionality. To bring in the latest changes from OSS, clone this repo then do:

```
# add the OSS repo as a remote
git remote add OSS git@github.com:influxdata/influxdb.git
git fetch OSS
git checkout -b pd/oss-sync
git merge OSS/main
# fix any conflicts and commit
git push --set-upstream origin pd/oss-sync
```

Then create a PR to merge `pd/oss-sync` into `main`. When you do the merge, be sure to the rebase version so all 
the commits from the OSS repo are brought into main.