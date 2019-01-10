properties([disableConcurrentBuilds()])

// TODO: change "next" to "master" as part of https://github.com/influxdata/influxdb/issues/10618.

if (env.CHANGE_ID) {
  // We're in a pull request.
  // Bail out if the target branch isn't 2.0 development.
  // Ref: https://github.com/jenkinsci/pipeline-github-plugin.
  if (pullRequest.base != "next") {
    println "Skipping build because it's a non-2.0 PR. Got pullRequest.base = ${pullRequest.Base}"
    return
  }
} else if (BRANCH_NAME != "next") {
  // Bail out if we aren't a PR and we aren't building the 2.0 branch.
  println "Skipping build because it's not the 2.0 branch. Got BRANCH_NAME = ${BRANCH_NAME}"
  return
}

node("dind") {
    container('dind') {
        // This method is provided by the private api-compatibility library.
        compat.test_build()
    }
}
