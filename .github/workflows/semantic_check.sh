#!/usr/bin/env bash
#
# Script that checks for conventional commit[1] messages
#
# We tried Semantic Pull Requests github app for a while, but it
# caused too many interittent failures / outages.
#
# To test:
# ./semantic_check.sh test
#
# [1] https://www.conventionalcommits.org/en/v1.0.0/
# [2] https://probot.github.io/apps/semantic-pull-requests/
#


set -e -o pipefail

shopt -s nocasematch
semantic_pattern='^(Merge .*branch '\''.+'\'' into|Revert ".+"|(feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)(\([^)]+\))?: +[^ ])'

if [[ $1 == "test" ]]; then
  exit_code=0

  echo checking strings that should be OK
  expect_ok="chore: foo
chore(hello): foo
CHORE: foo
Revert \"fix: certain this fix is correct!\"
Merge remote-tracking branch 'origin/main' into alamb/update_df_101
Merge branch 'main' into foo"
  while read -r s; do
    if [[ ! $s =~ $semantic_pattern ]]; then
      echo got FAIL, expected OK: "$s"
      exit_code=1
    fi
  done <<< "$expect_ok"

  echo checking strings that should FAIL
  expect_fail="more: foo
chore(: foo
chore : foo
chore:
chore:
chore:foo
Revert my thing
Merge this is not a legit merge
"
  while read -r s; do
    if [[ $s =~ $semantic_pattern ]]; then
      echo got OK, expected FAIL: "$s"
      exit_code=1
    fi
  done <<< "$expect_fail"

  exit $exit_code
fi

# ensure expected env vars are set
set -u
exit_code=0

if [[ ! $PR_TITLE =~ $semantic_pattern ]]; then
  echo ::error::PR title not semantic: "$PR_TITLE"
  exit_code=1
else
  echo PR title OK: "$PR_TITLE"
fi

json=$(curl --fail --retry 3 --silent "$COMMITS_URL")
commits=$(echo "$json" | jq --raw-output '.[] | [.sha, (.commit.message | split("\n") | first)] | join(" ")')

while read -r commit; do
  commit_title=$(echo "$commit" | cut -c 42-999)

  if [[ ! $commit_title =~ $semantic_pattern ]]; then
    echo ::error::Commit title not semantic: "$commit"
    exit_code=1
  else
    echo Commit title OK: "$commit"
  fi
done <<< "$commits"

exit $exit_code
