# Release process for Enterprise

## Manual

- Create a branch and update `Cargo.toml`s version field (examples: `3.0.0-beta.1`, `3.0.0-beta.2`, `3.0.0-rc.1`, `3.0.0`, `3.0.1`, `3.1.0`, etc)
- Push the branch (this should include change to `Cargo.toml` and `Cargo.lock`)
- Create a PR and merge this release PR
- Checkout main locally and pull the changes
- Create annotated tag, eg
    - `git tag -a 'v3.0.0-0.beta.1' -m '3.0.0-beta.1 release'`
    - `git tag -a 'v3.0.0-0.beta.2' -m '3.0.0-beta.2 release'`
    - `git tag -a 'v3.0.0-0.rc.1' -m '3.0.0-rc.1 release'`
    - `git tag -a 'v3.0.0' -m '3.0.0 release'`
    - `git tag -a 'v3.0.1' -m '3.0.1 release'`
    - `git tag -a 'v3.1.0' -m '3.1.0 release'`

  There is a full explanation on what each portion of the tag means in `.circleci/packages/config.yaml`. This tag should match the regex as configured in `release-filter` in `.circleci/config.yml`
- Once tagged, push it. Eg, `git push origin v3.0.0-0.beta.1`
- This should run the full build and publish the packages, a quick test will be to run a `curl` (replace `3.0.0-0.beta.1` with your tag name without the leading `v`)

  e.g. `curl -LO https://dl.influxdata.com/influxdb/releases/influxdb3-enterprise-3.0.0-0.beta.1_linux_amd64.tar.gz`

  See `.circleci/packages/config.yaml` for more info on how git tags map to filenames.


_At some point this should be scripted so that versions etc are in sync between the steps_
