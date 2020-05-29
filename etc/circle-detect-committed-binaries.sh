#!/bin/bash

# This script wraps detect-committed-binaries.sh in a way that is usable from the Makefile.

# Why not put these changes into detect-committed-binaries.sh?
# Because we aren't using CircleCI for the enterprise code base,
# and I don't want the OSS copy to diverge from the enterprise copy.

# Why not just do the conditional logic in the Makefile?
# Because I don't know Makefile syntax and practices well enough to do it in a reasonable amount of time.
# If you know how to do it, please refactor the logic into the Makefile.

if [ -n "$CIRCLE_PR_NUMBER" ] || [ -n "$CIRCLE_PULL_REQUEST" ]; then
	# We want the PR number, but sometimes it isn't set (bug on CircleCI's side).
	# https://discuss.circleci.com/t/circle-pr-number-missing-from-environment-variables/3745
	CIRCLE_PR_NUMBER="${CIRCLE_PR_NUMBER:-${CIRCLE_PULL_REQUEST##*/}}"
	# Looks like we're running on CircleCI.
	# You might think we could use CIRCLE_COMPARE_URL, but that compares commits with the previous push,
	# not with the base branch.
	# This is roughly how you're supposed to determine the base branch/sha according to Circle:
	# https://circleci.com/blog/enforce-build-standards/
	PR_URL="https://api.github.com/repos/$CIRCLE_PROJECT_USERNAME/$CIRCLE_PROJECT_REPONAME/pulls/$CIRCLE_PR_NUMBER?access_token=$GITHUB_READONLY_TOKEN"
	COMMIT_RANGE="$(curl -s "$PR_URL" | jq -r '.head.sha + "..." + .base.sha')"
	echo "Calculated commit range: $COMMIT_RANGE"
else
	# We're not running on circle.
	# There's no reliable way to figure out the appropriate base commit,
	# so just take a reasonable guess that we're comparing to master.
	COMMIT_RANGE="HEAD...master"
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
"$DIR/detect-committed-binaries.sh" "$COMMIT_RANGE"
