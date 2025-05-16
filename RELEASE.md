# Release process for Core

## Point Releases

- Checkout the base branch for the point release. So, if releasing `3.0.2`, checkout `3.0` and list
  out commits to see which was the latest tagged release commit:
  ```
  git checkout 3.0
  git log --oneline --graph
  ```
  There should be a tagged commit from the previous release, in this case `(tag: v3.0.1, ...)`:
  ```
  * d7c071e0c4 (tag: v3.0.1, origin/3.0) chore: version bump for 3.0.1 (#26282)
  * e4cfbf71f7 (origin/pd/three-dot-oh-one) chore: back-port catalog limit refactor from enterprise (#26278)
  ```
  Take note of the commit immediately before it (`e4cfbf71f7` here), as the tagged commit (`d7c071e0c4` here)
  will not appear on the `main` branch.

- Open a branch off of the `3.0` branch before proceeding, we will cherry pick the commits for this
  release onto the new branch:
  ```
  git checkout -b 3.0/my-branch-name
  ```

- You need to cherry pick commits from the `main` branch that will go into the `3.0.2` point release,
  so you can list out the most recent commits on the `main` branch to determine those:
  ```
  git log --oneline --graph main
  ```
  Will output:
  ```
  * d30f26618c fix: distinct cache counting bug (#26332)
  * 0f52ebb90d fix: group by tag columns with escape quotes (#26326)
  * b41a2d9bc3 feat: expose `--format json` for token creation cmd (#26286)
  * 2ceed952b8 docs: clarify default maxes for distinct cache (#26281)
  * e4cfbf71f7 (origin/pd/three-dot-oh-one) chore: back-port catalog limit refactor from enterprise (#26278)
  ```
  For any commits newer than the one previously noted (`e4cfbf71f7`), you will want to cherry pick
  them onto the `3.0/my-branch-name` branch in the order they were originally applied:
  ```
  git cherry-pick 2ceed952b8
  git cherry-pick b41a2d9bc3
  git cherry-pick 0f52ebb90d
  git cherry-pick d30f26618c
  ```

- Once all necessary commits have been `git cherry-pick`'d, push them up to the remote:
  ```
  git push
  ```
  Open a PR from `3.0/my-branch-name` into `3.0` (not `main`) so the changes can be reviewed by
  another developer.

- Once that PR is merged, the version needs to be bumped on the `3.0` branch for the new release, so
  start a new branch:
  ```
  git checkout -b hiltontj/three-zero-two
  ```
  On this branch, update the `version` in the `Cargo.toml` file to use the desired release version
  in the `[workspace.package]` section.

  Commit and open a PR with the version change _into the `3.0` branch_ (not `main`).

- Once the PR to update the version is merged into the `3.0` branch on the remote, jump back to the
  `3.0` branch locally to `git pull` and tag/push the release commit (see
  [Commit Tagging](#commit-tagging)):
  ```
  git tag -a 'v3.0.2' -m '3.0.2 release'
  git push origin v3.0.2
  ```

- This should run the full build and publish the packages, a quick test will be to run a `curl`
  (replace `3.0.2` with your tag name without the leading `v`)

  ```
  curl -LO https://dl.influxdata.com/influxdb/releases/influxdb3-core-3.0.2_linux_amd64.tar.gz
  ```

- When satisfied, update `install_influxdb.sh` to use the new version for `INFLUXDB_VERSION`

- Once the above is complete, the official Docker image repository needs to be updated. See
  [Official Docker Image Repository](#official-docker-image-repository) for the steps required to
  do so.

- Lastly, `apt` and `yum` repositories need to be updated. This can be done with a change to the
  respective files for `core`/`enterprise` in the private `repos` repository (see [example][repos-commit]).

_At some point this should be scripted so that versions etc are in sync between the steps_

[repos-commit]: https://github.com/influxdata/repos/pull/179/commits/fa0f8374e52ee86359efd00ce7dcb011d5ebb37a

## Commit Tagging

Annotated tags can be created with `git tag -a <tag_name> -m <description>`, e.g.,

- `git tag -a 'v3.0.0-0.beta.1' -m '3.0.0-beta.1 release'`
- `git tag -a 'v3.0.0-0.beta.2' -m '3.0.0-beta.2 release'`
- `git tag -a 'v3.0.0-0.rc.1' -m '3.0.0-rc.1 release'`
- `git tag -a 'v3.0.0' -m '3.0.0 release'`
- `git tag -a 'v3.0.1' -m '3.0.1 release'`
- `git tag -a 'v3.1.0' -m '3.1.0 release'`

There is a full explanation on what each portion of the tag means, as well as how git tags map to
filenames in `.circleci/packages/config.yaml`. This tag should match the regex as configured in
`release-filter` in `.circleci/config.yml`.

## Official Docker Image Repository

Influx maintains a [repository][influx-docker] of `Dockerfile`s for various projects. The official
Docker image [repository][docker-official] refers to the former.

When a release is complete, the above repositories need to be updated by:

- Updating the `INFLUXDB_VERSION` in the following files in `influxdata-docker` (see [example][ex1]):
  - `influxdb/3.0-core/Dockerfile`
  - `influxdb/3.0-enterprise/Dockerfile`

- Once merged, take the commit SHA from the merge commit, and use it to update the official docker
  image manifest for Influx. In addition to updating the SHA, the image tag with the full version
  numbers need to be updated for `core` and `enterprise` images (see [example][ex2]).

[influx-docker]: https://github.com/influxdata/influxdata-docker
[docker-official]: https://github.com/docker-library/official-images
[ex1]: https://github.com/influxdata/influxdata-docker/commit/a956adaa6d24473d3ea6b9c638d9b4658dfecc44
[ex2]: https://github.com/docker-library/official-images/pull/18972/commits/e543ab7774878d9246ee95c50e8719844b6c6788
