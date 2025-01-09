# InfluxDB Enterprise
This is a fork of Core with all the Enterprise functionality. To bring in the latest changes from Core, clone this repo then do:

```
# add the Core repo as a remote
git remote add core git@github.com:influxdata/influxdb.git
git fetch core
git checkout -b pd/oss-sync
git merge core/main
# fix any conflicts and commit
git push --set-upstream origin pd/oss-sync
```

Then create a PR to merge `pd/oss-sync` into `main`. Use a merge PR that pulls over the commits.

## influxdb3_core_internal dependency

Enterprise depends on both `influxdb3_core` and `influxdb3_core_internal` (closed source crates). And also, `influxdb3_core_internal`
itself depends on `influxdb3_core` for common utils (like `iox_time`). When amending `influxdb3_core` version/revision
`influxdb3_core_internal` needs to be on the same version/revision as well.

### CircleCI

- To be able to pull `influxdb3_core_internal` crates, SSH key has been added to CircleCI. Public key is added to
  `influxdb3_core_internal` repo. The private key is added to the [engineering vault](https://team-influxdata.1password.com/app#/gy77wv6esuba5hoknu3apl4mlu/Tag?itemListId=yruguqzv7uyenepybxltzh23ei)
  in 1password.

- In order to revoke or rotate the keys, it should be as simple as deleting the keys in CircleCI and in Github and
  reinstating new keys following the [docs](https://github.com/influxdata/docs.influxdata.io/blob/main/content/development/security/kb.md#circleci-and-ssh-keys)
